from concurrent.futures import thread
from flask import Flask
import flask
import json
import os
import sys
import uuid
import time
import traceback
from threading import Thread
from flask_cors import CORS
from pyparsing import dbl_slash_comment
import sqlalchemy
from datetime import datetime, timedelta
from dateutil.relativedelta import *
from google.cloud import bigquery

import requests
from bs4 import BeautifulSoup
import pandas as pd
from clickhouse_driver import Client

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from luapy.utils.pg_db_utils import (
    insertJob,
    updateJob,
    getJobSummary,
    getPendingJobs,
    getDoneMaxJob,
    getMaxJob,
)

import luapy.utils.pg_db_utils as pgu

from luapy.el.tweets import extract_tweets_load
from luapy.el.twitterUserLookup import extract_users_load

from luapy.logger import logger
from luapy.db import create_db
from luapy.job import Job
import luapy.utils.lua_utils as lu


test_from_cloud_run = lu.get_secret('test_from_cloud_run')
print('test_from_cloud_run: ', test_from_cloud_run)

RUNNING_LOCAL = str(os.getenv("RUNNING_LOCAL", "0")) == "1"
print("RUNNING_LOCAL: ", RUNNING_LOCAL)



CH_ADMIN_PASSWORD = lu.get_secret("CH_ADMIN_PASSWORD")

SQLALCHEMY_DATABASE_URI = lu.get_secret("SUPABASE_SQLALCHEMY_DATABASE_URI")
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQL_POOL_PRE_PING = True


def create_app(config=__name__, db_options={}, **kwargs):

    if not RUNNING_LOCAL and not kwargs.get('is_test', False):
        sentry_sdk.init(
            dsn="https://5fce4fd9b9404cbe978b509a2465f027@o1176187.ingest.sentry.io/6325459",
            integrations=[FlaskIntegration()],
            # Set traces_sample_rate to 1.0 to capture 100%
            # of transactions for performance monitoring.
            # We recommend adjusting this value in production.
            traces_sample_rate=0.1,
        )


    app = Flask(__name__)
    app.config.from_object(config)
    db = create_db(**db_options)
    db.init_app(app)
    CORS(app)

    @app.route("/")
    def hello_world():
        name = os.environ.get("NAME", "World")
        return "Hello {}!".format(name)


    @app.route("/test_threads", methods=["GET", "POST"])
    def test_threads():
        data = flask.request.get_json()

        def threaded_task(data):
            for i in range(data["duration"]):
                # print("Working... {}/{}".format(i + 1, data['duration']))
                data["i"] = i
                logger.info(f"test_threads run {i}", extra={"json_fields": data})
                time.sleep(60)

        d = {"duration": 100, "type": "test_threads"}
        thread = Thread(target=threaded_task, args=(d,))
        thread.daemon = True
        thread.start()
        logger.info(f"test_threads...", extra={"json_fields": d})
        return json.dumps(d), 200, {"ContentType":"application/json"}


    @app.route("/ping", methods=["GET", "POST"])
    def ping():
        name = os.environ.get("NAME", "World")
        j = {"ok": True, "name": name}
        logger.info(f"ping...", extra={"json_fields": j.copy()})
        return json.dumps(j), 200, {"ContentType":"application/json"}


    @app.route("/test_secret", methods=["GET", "POST"])
    def test_secret():
        test_from_cloud_run = lu.get_secret("ANOTHER_TEST")
        j = {"ok": True, "test_from_cloud_run": test_from_cloud_run}
        return json.dumps(j), 200, {"ContentType":"application/json"}


    @app.route("/ping_sql", methods=["GET", "POST"])
    def pingsql():
        logger.info(f"ping_sql...")
        with db.engine.connect() as con:
            sql = f"""
            select
            max(id) as max_id,
            min(id) as min_id,
            count(id) as count,
            sum(case when j.status = 'running' then 1 else 0 end) as running
            from "public".jobs as j
            """
            statement = sqlalchemy.sql.text(sql)
            j = con.execute(statement).fetchone()
            return json.dumps(dict(j)), 200, {"ContentType": "application/json"}
        # j = {'ok': False}
        # return json.dumps(j), 200, {'ContentType':'application/json'}


    @app.route("/get_jobs", methods=["GET", "POST"])
    def get_jobs():
        data = flask.request.get_json()
        logger.info(f"get_jobs: {data}")
        jobs = getPendingJobs(db.engine)
        for job in jobs:
            # update job status to running
            updateJobRow = {"id": job["id"], "status": "running"}
            pgu.updateJobStatus(db.engine, updateJobRow)
            # send job to cloud run with post request
            # url = "https://luabase-mjr-py.ngrok.io/run_job"
            if RUNNING_LOCAL:
                url = "http://localhost:5000/run_job"
            else:
                url = "https://luabase-py-msgn5tdnsa-uc.a.run.app/run_job"
            payload = job["details"]
            payload["id"] = job["id"]
            headers = {"content-type": "application/json"}
            # try:
            try:
                requests.request("POST", url, json=payload, headers=headers, timeout=2)
            except requests.exceptions.ReadTimeout:
                pass
            logger.info(f"get_jobs to run_job: {payload}")
            # except requests.exceptions.ReadTimeout:
            #     pass

        j = {"ok": True, "data": jobs}
        return json.dumps(j), 200, {"ContentType": "application/json"}

    @app.route("/run_job", methods=["GET", "POST"])
    def run_job():
        data = flask.request.get_json()
        logger.info(f"run_job...", extra={"json_fields": data})
        try:
            response = _run_job(data, db)
            logger.info(f"run_job done...", extra={"json_fields": data})
            return response
        except Exception as e:
            data["error"] = str(e)
            updateJobRow = {
                "id": data["id"],
                "status": "failed",
                "details": json.dumps(data),
            }
            pgu.updateJob(db.engine, updateJobRow)
            data["traceback"] = ''.join(traceback.format_tb(e.__traceback__))
            logger.error(f"run_job error", extra={"json_fields": data})
            return json.dumps(data), 500, {"ContentType": "application/json"}

    return app, db


def _run_job(data, db):

    if data.get("type") == "getTweets":
        logger.info(f"run_job getTweets new...", extra={"json_fields": data})
        tweetLimit = data.get("tweetLimit", None)
        since_id = data.get("since_id", None)
        max_running=data.get("max_running", 10)
        job_type=data.get("type")

        j = extract_tweets_load(
            db.engine, tweetLimit, since_id, max_running, job_type
        )
        return json.dumps(j), 200, {"ContentType": "application/json"}

    if data.get("type") == "getUsers":
        logger.info(f"run_job getUsers...", extra={"json_fields": data})

        j = extract_users_load(
            db.engine
        )
        return json.dumps(j), 200, {"ContentType": "application/json"}


    if data.get("type") == "testJob":
        logger.info(f"run_job testJob...", extra={"json_fields": data})
        updateJobRow = {"id": data["id"], "status": "success"}
        pgu.updateJobStatus(db.engine, updateJobRow)
        j = {"ok": True, "data": updateJobRow}
        return json.dumps(j), 200, {"ContentType":"application/json"}

    if data.get("type") == "PYTEST_ONLY":
        if data.get("error"):
            raise Exception("HandledError")
        else:
            j = {"ok": True, "data": "Hello pytest!"}
            return json.dumps(j), 200, {"ContentType": "application/json"}

    j = {"ok": True, "data": "running"}
    logger.info(f"run_job end without return...", extra={"json_fields": data})
    return json.dumps(j), 200, {"ContentType": "application/json"}


def send_request(url):
    response = requests.get(
        url="https://app.scrapingbee.com/api/v1/",
        verify=False,
        timeout=60,
        params={"api_key": SCRAPING_BEE_API_KEY, "url": url,},
    )
    print("Response HTTP Status Code: ", response.status_code)
    return response


def getChClient(use_numpy=True):
    client = Client(
        "lua-2.luabase.altinity.cloud",
        user="admin",
        password=CH_ADMIN_PASSWORD,
        port=9440,
        secure=True,
        verify=False,
        database="default",
        compression=True,
        settings={"use_numpy": use_numpy},
    )
    return client


def getLuaAddresses(newStart, newEnd):
    chClient = getChClient()
    sql = f"""
    SELECT id, address
    FROM default.name_tags2_local as nt
    WHERE
    nt.id >= {newStart}
    AND nt.id <= {newEnd}
    """
    print("getLuaAddresses sql: ", sql)
    df = chClient.query_dataframe(sql)
    return df


def deleteOldAddresses(newStart, newEnd):
    logger.info(f"deleteOldAddressData...")
    chClient = getChClient()
    sql = f"""
    ALTER TABLE default.name_tags2_local
    DELETE WHERE id >= {newStart} AND id <= {newEnd}
    """
    print("deleteOldAddressData sql: ", sql)
    df = chClient.execute(sql)
    return df


def createTagTables():
    sql = """
    DROP TABLE default.name_tags2_local
    """
    try:
        client.execute(sql)
    except Exception as e:
        print("createTagTables error: ", e)
        pass
    sql = """
    CREATE TABLE default.name_tags2_local
    (
        `address` String,
        `name_tag` String,
        INDEX name_tags_address_idx address TYPE bloom_filter GRANULARITY 1,
        INDEX name_tags_name_tag_idx name_tag TYPE bloom_filter GRANULARITY 1
    )
    ENGINE = MergeTree
    PRIMARY KEY (address, name_tag)
    ORDER BY (address, name_tag)
    """
    client.execute(sql)
    return True




def getTag(content):
    try:
        soup = BeautifulSoup(content, "html.parser")
        items = soup.select(
            'span[data-original-title="Public Name Tag (viewable by anyone)"]'
        )
        tag = "none"
        for item in items:
            tag = item.text
            return tag
        items = soup.select('span[title="Public Name Tag (viewable by anyone)"]')
        for item in items:
            tag = item.text
            return tag
        return tag
    except Exception as e:
        print("getTag error: ", e)
        return False


def getManyTags(addresses, newStart, newEnd):
    logger.info(f"getManyTags...")
    tags = 0
    for row in addresses:
        content = getSoup(row["address"])
        if content:
            row["name_tag"] = "none"
            tag = getTag(content)
            if tag:
                row["name_tag"] = tag
    deleteOldAddresses(newStart, newEnd)
    toChDf = pd.DataFrame(addresses)
    chClient = getChClient()
    logger.info(f"inserting new tags...")
    chClient.insert_dataframe(
        "INSERT INTO default.name_tags2_local (id, address, name_tag) VALUES", toChDf
    )


# testd = {
#     "type": "getEthNameTag",
#     "start": -1,
#     "end": -1,
#     "step": 2,
#     "maxRunning": 200
# }
# getEthNameTags(testd)

app, db = create_app()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
