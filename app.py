from flask import Flask
import flask
import json
import os
import sys
import uuid
from flask_cors import CORS 
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy


from logger import logger

import requests
from bs4 import BeautifulSoup
import pandas as pd
from clickhouse_driver import Client

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from pg_db_utils import insertJob, updateJob, getJobSummary, getDoneMaxJob, getMaxJob
from btc_etl import extract_transform_load_btc

sentry_sdk.init(
    dsn="https://5fce4fd9b9404cbe978b509a2465f027@o1176187.ingest.sentry.io/6325459",
    integrations=[FlaskIntegration()],

    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=0.1
)

SCRAPING_BEE_API_KEY = os.environ.get('SCRAPING_BEE_API_KEY')
CH_ADMIN_PASSWORD = os.environ.get('CH_ADMIN_PASSWORD')

SQLALCHEMY_DATABASE_URI = os.environ.get('SUPABASE_SQLALCHEMY_DATABASE_URI')
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQL_POOL_PRE_PING = True
QUICKNODE_BTC = os.environ.get('BTC_QUICKNODE')

app = Flask(__name__)
app.config.from_object(__name__)

CORS(app)

SQLALCHEMY_SESSION_OPTIONS = {
    'autocommit': True,
    'pool_size': 10,
    'pool_recycle': 60,
    "max_overflow": 2,
    'pool_pre_ping': True,
}

SQLALCHEMY_ENGINE_OPTIONS = {
    'pool_size': 10,
    'pool_recycle': 60,
    'pool_pre_ping': True
}

db = SQLAlchemy(app, session_options=SQLALCHEMY_SESSION_OPTIONS, engine_options=SQLALCHEMY_ENGINE_OPTIONS)



def send_request(url):
    response = requests.get(
        url='https://app.scrapingbee.com/api/v1/',
        verify=False, 
        timeout=60,
        params={
            'api_key': SCRAPING_BEE_API_KEY,
            'url': url,  
        },
        
    )
    print('Response HTTP Status Code: ', response.status_code)
    return response

def getChClient():
    client = Client('lua-2.luabase.altinity.cloud',
        user='admin',
        password=CH_ADMIN_PASSWORD,
        port=9440,
        secure=True,
        verify=False,
        database='default',
        compression=True,
        settings = {'use_numpy': True}
    )
    return client

def getLuaAddresses(newStart, newEnd):
    chClient = getChClient()
    sql = f'''
    SELECT id, address
    FROM default.name_tags2_local as nt
    WHERE
    nt.id >= {newStart}
    AND nt.id <= {newEnd}
    '''
    print('getLuaAddresses sql: ', sql)
    df = chClient.query_dataframe(sql)
    return df

def deleteOldAddresses(newStart, newEnd):
    logger.info(f'deleteOldAddressData...')
    chClient = getChClient()
    sql = f'''
    ALTER TABLE default.name_tags2_local 
    DELETE WHERE id >= {newStart} AND id <= {newEnd}
    '''
    print('deleteOldAddressData sql: ', sql)
    df = chClient.execute(sql)
    return df

def createTagTables():
    sql = '''
    DROP TABLE default.name_tags2_local
    '''
    try:
        client.execute(sql)
    except Exception as e:
        print('createTagTables error: ', e)
        pass
    sql = '''
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
    '''
    client.execute(sql)
    return True

def getSoup(address):
    url = f"https://etherscan.io/address/{address}"
    try:
        res = send_request(url)
        if res.ok:
            return res.content
        else:
            print('getSoup error: ', res)
            return False
    except Exception as e:
        print('getSoup error: ', e)
        return False

def getTag(content):
    try:
        soup = BeautifulSoup(content,'html.parser')
        items = soup.select('span[data-original-title="Public Name Tag (viewable by anyone)"]')
        tag = 'none'
        for item in items:
            tag = item.text
            return tag
        items = soup.select('span[title="Public Name Tag (viewable by anyone)"]')
        for item in items:
            tag = item.text
            return tag
        return tag
    except Exception as e:
        print('getTag error: ', e)
        return False

def getManyTags(addresses, newStart, newEnd):
    logger.info(f'getManyTags...')
    tags = 0
    for row in addresses:
        content = getSoup(row['address'])
        if content:
            row['name_tag'] = 'none'
            tag = getTag(content)
            if tag:
                row['name_tag'] = tag
    deleteOldAddresses(newStart, newEnd)
    toChDf = pd.DataFrame(addresses)
    chClient = getChClient()
    logger.info(f'inserting new tags...')
    chClient.insert_dataframe('INSERT INTO default.name_tags2_local (id, address, name_tag) VALUES', toChDf)

def getEthNameTags(data):
    logger.info(f'getEthNameTags... {data}')
    maxRunning = data.get('maxRunning', 1000)
    jobSummary = getJobSummary(db.engine, data.get('type'))
    if jobSummary['running'] >= maxRunning:
        logger.info(f'already max running!')
        return {'ok': True, 'status': f"max of {maxRunning} already running"}
    maxJob = getMaxJob(db.engine, jobSummary['max_id'])
    newStart = maxJob['details'].get('end', -1) + 1
    newEnd = newStart + data.get('step', 100)
    # insert new jobs that is running
    jobDetails = {
        "type": "getEthNameTag",
        "start": newStart,
        "end": newEnd
    }
    jobRow = {
        'type': jobDetails['type'],
        'status': 'running',
        'details': json.dumps(jobDetails)
    }
    jobRow = insertJob(db.engine, jobRow)
    print('jobRow: ', jobRow)
    logger.info(f'getting addresses... ${jobRow}')
    addresses = getLuaAddresses(newStart, newEnd)
    if len(addresses) == 0:
        logger.info(f'no addresses!')
        jobDetails['reason'] = 'no addresses'
        updateJobRow = {
            'id': jobRow['row']['id'],
            'status': 'error',
            'details': json.dumps(jobDetails)
        }
        updateJob(db.engine, updateJobRow)
        return {'ok': True, 'status': f"no addresses for {newStart} to {newEnd}"}
    logger.info(f'got {len(addresses)} address...')
    getManyTags(addresses.to_dict(orient='records'), newStart, newEnd)
    # mark job complete, successs
    # jobRow['id']
    updateJobRow = {
        'id': jobRow['row']['id'],
        'status': 'success',
        'details': json.dumps(jobDetails)
    }
    updateJob(db.engine, updateJobRow)
    logger.info(f"job done. {jobRow}")
    return {'ok': True}

@app.route('/')
def hello_world():
    name = os.environ.get("NAME", "World")
    return "Hello {}!".format(name)

@app.route('/ping', methods=["GET", "POST"])
def ping():
    name = os.environ.get('NAME', 'World')
    j = {'ok': True, 'name': name}
    return json.dumps(j), 200, {'ContentType':'application/json'}

@app.route('/ping_sql', methods=["GET", "POST"])
def pingsql():
    logger.info(f'ping_sql...')
    with db.engine.connect() as con:
        sql = f'''
        select 
        max(id) as max_id, 
        min(id) as min_id, 
        count(id) as count,
        sum(case when j.status = 'running' then 1 else 0 end) as running
        from "public".jobs as j
        '''
        statement = sqlalchemy.sql.text(sql)
        j = con.execute(statement).fetchone()
        return json.dumps(dict(j)), 200, {'ContentType':'application/json'}
    # j = {'ok': False}
    # return json.dumps(j), 200, {'ContentType':'application/json'}

@app.route('/run_job', methods=["GET", "POST"])
def run_job():
    data = flask.request.get_json()
    logger.info(f'run_job: {data}')
    if data.get('type') == 'getEthNameTag':
        j = getEthNameTags(data)
        return json.dumps(j), 200, {'ContentType':'application/json'}
    if data.get('type') == 'getBtcEtl':
        target = data.get('target', 'both')
        lag = data.get('lag', 6)
        start_block = data.get('startBlock', None)
        end_block = data.get('endBlock', None)

        j = extract_transform_load_btc(
            getChClient(), 
            QUICKNODE_BTC, 
            db.engine, 
            target, 
            lag, 
            start_block, 
            end_block
            )
        return json.dumps(j), 200, {'ContentType':'application/json'}
    if data.get('type') == 'backlogBtc':
        # get min block from postgres
        # insertJob(min=minFromPg-20, max=minFromPg-1)
        # load_btc(min=minFromPg-20, max=minFromPg-1)
        # updateJob(asdfl jsdkf)
        # if minFromPg-20 > 0:
        #     url = "https://localhost:500/run_job"
        #     payload = {"job": "missingLogs"}
        #     headers = {"content-type": "application/json"}
        #     try:
        #         response = requests.request("POST", url, json=payload, headers=headers, timeout=1)
        #     except requests.exceptions.ReadTimeout: 
        #         pass
        return json.dumps(j), 200, {'ContentType':'application/json'}
    j = {'ok': True, 'data': 'running'}
    return json.dumps(j), 200, {'ContentType':'application/json'}


# testd = {
#     "type": "getEthNameTag",
#     "start": -1,
#     "end": -1,
#     "step": 2,
#     "maxRunning": 200
# }
# getEthNameTags(testd)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))