import requests
import os
import json
import pandas as pd
import numpy as np
import requests
import os
import json
import clickhouse_connect
import time

from luapy.logger import logger
from luapy.utils.pg_db_utils import (
    insertJob,
    getJobSummary,
    getMaxJob,
    updateJob
)
user = os.getenv('user')
pw = os.environ.get('pw')


originaldf = pd.DataFrame(columns= ['created_at', 'text', 'entities', 'source', 'lang', 'id', 'referenced_tweets', 'author_id', 'public_metrics', 'possibly_sensitive', 'in_reply_to_user_id', 'attachments', 'geo', 'withheld'])
originaldf['isnew'] = False
bearer_token = "AAAAAAAAAAAAAAAAAAAAANbKcAEAAAAAFKZ7zJwUbQbnk16szYN3by%2F2ogc%3Dr4yUwb04hiMnVG2IEDDUcieM2puKvBTVJ3Eyjs2Dsjnp1UxZNd"
search_url = "https://api.twitter.com/2/tweets/search/recent"

def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    if response.status_code == 429:
        logger.info(f"waiting 15 minutes")
        time.sleep(900)
        return connect_to_endpoint(url, params)
    elif response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def createcolumns(df):
    df.isnew = True
    if 'geo' not in df.columns:
        df['geo'] = np.NaN
    if 'withheld' not in df.columns:
        df['withheld'] = np.NaN

def fillNaN(df):
    df.loc[df['referenced_tweets'].isnull(),['referenced_tweets']] = df.loc[df['referenced_tweets'].isnull(),'referenced_tweets'].apply(lambda x: [])
    columns = ['attachments', 'withheld', 'geo']
    for col in columns:
        df.loc[df[col].isnull(),[col]] = df.loc[df[col].isnull(),col].apply(lambda x: {})

def objecttostring(df):
    for array in df['referenced_tweets']:
        for dict in array:
            array[array.index(dict)] = json.dumps(dict)

def editType(df):
    strings = ['entities', 'public_metrics', 'attachments', 'geo', 'withheld']
    for col in strings:
        df[col] = df[col].astype('str') 
    df['possibly_sensitive'] = df['possibly_sensitive'].astype('bool') 
    df['in_reply_to_user_id'] = df['in_reply_to_user_id'].astype('float') 
    df.created_at = pd.to_datetime(df.created_at)

def removeNonASCIIchar(df):
    ascii = ['text', 'entities', 'source']
    for col in ascii: 
        df[col] = df[col].apply(lambda x: x.encode('ascii', 'ignore').decode('ascii'))

def fetchalltweets(since_id = None, next_token = None):
    query_params = {'query': '(0x OR \.eth OR ethereum OR bitcoin OR btc OR etherscan OR web3 OR solana OR luna OR terra OR polygon OR nft -is:retweet)', 'max_results': "100", 'tweet.fields':"id,text,attachments,author_id,created_at,entities,geo,in_reply_to_user_id,lang,possibly_sensitive,referenced_tweets,source,public_metrics,withheld"}
    if since_id is not None: 
        query_params['since_id'] = since_id
    if next_token is not None:
        query_params['next_token'] = next_token
    json_response = connect_to_endpoint(search_url,query_params)
    df = pd.DataFrame(json_response['data'])
    createcolumns(df)
    if 'next_token' in json_response['meta'].keys():
        next_token = json_response['meta']['next_token']
        newdf, newest_id = fetchalltweets(since_id, next_token)
        df = pd.concat([df, newdf])
    return df, json_response['meta']['newest_id']

def addToDB(df):
    chClient = clickhouse_connect.get_client(host='twitter-test.luabase.altinity.cloud', port=8443, username="admin", password="tvM4udMRZ37sf8Kh")
    chClient.insert_df('twitter.tweets', df)

       
def extract_tweets_load(
    pg_db,
    tweetLimit,
    since_id,
    max_running=10,
    job_type="getTweets"
    ):

    ######check if another job is currently running######
    job_summary = getJobSummary(pg_db, job_type)
    if job_summary["running"] >= max_running:
        logger.info(f"already another {max_running} jobs running!")
        #return {"ok": True, "status": f"max of {max_running} jobs already running"}

    #####set start and end blocks######
    # get max block+1 in latest job if start block is null
    if since_id == None:
        max_job = getMaxJob(pg_db.engine, job_summary["max_id"])
        since_id = max_job["details"].get("newest_id")
    logger.info(f"newest_id: " + str(since_id))
    job_details = {"type": job_type, "newest_id": since_id, "tweetLimit": tweetLimit}
    job_row = {
        "type": job_details["type"],
        "status": "running",
        "details": json.dumps(job_details),
    }
    job_row = insertJob(pg_db.engine, job_row)
    log_details = {
        "type": job_type,
        "id": job_row["row"]["id"],
        "newest_id": since_id,
        "tweetLimit": tweetLimit
    }
    logger.info(
        f"fetching tweets... ${job_row}", extra={"json_fields": log_details}
    )

    try:
        df, since_id = fetchalltweets(since_id)
    except Exception as e:
        # mark job as failed if failed
        log_details["error"] = e
        logger.info(
            f"failed fetching tweets... ${job_row}:",
            extra={"json_fields": log_details},
        )
        logger.info(e)
        updateJobRow = {
            "id": job_row["row"]["id"],
            "status": "failed",
            "details": json.dumps(job_details),
        }
        updateJob(pg_db.engine, updateJobRow)
        return {"ok": False}
    logger.info(
        f"completed fetching tweets... ${job_row}",
        extra={"json_fields": log_details},
    )

    
    
    ######fill data with NaN############

    logger.info(
        f"fill data with NaN... ${job_row}", extra={"json_fields": log_details}
    )
    try:
        fillNaN(df)
    except Exception as e:
        # mark job as failed if failed
        log_details["error"] = e
        logger.info(
            f"failed filling data with NaN... ${job_row}:",
            extra={"json_fields": log_details},
        )
        updateJobRow = {
            "id": job_row["row"]["id"],
            "status": "failed",
            "details": json.dumps(job_details),
        }
        updateJob(pg_db.engine, updateJobRow)
        return {"ok": False}
    

     ######convert objects to strings############

    logger.info(
        f"convert objects to strings... ${job_row}", extra={"json_fields": log_details}
    )
    try:
        objecttostring(df)
    except Exception as e:
        # mark job as failed if failed
        log_details["error"] = e
        logger.info(
            f"failed converting objects to strings... ${job_row}:",
            extra={"json_fields": log_details},
        )
        updateJobRow = {
            "id": job_row["row"]["id"],
            "status": "failed",
            "details": json.dumps(job_details),
        }
        updateJob(pg_db.engine, updateJobRow)
        return {"ok": False}

    ######edit the types of columns############

    logger.info(
        f"edit the types of columns... ${job_row}", extra={"json_fields": log_details}
    )
    try:
        editType(df)
    except Exception as e:
        # mark job as failed if failed
        log_details["error"] = e
        logger.info(
            f"failed editing the types of columns... ${job_row}:",
            extra={"json_fields": log_details},
        )
        updateJobRow = {
            "id": job_row["row"]["id"],
            "status": "failed",
            "details": json.dumps(job_details),
        }
        updateJob(pg_db.engine, updateJobRow)
        return {"ok": False}
    
    ######remove non-ascii characters############

    logger.info(
        f"remove non-ascii characters... ${job_row}", extra={"json_fields": log_details}
    )
    try:
        removeNonASCIIchar(df)
    except Exception as e:
        # mark job as failed if failed
        log_details["error"] = e
        logger.info(
            f"failed removing non-ascii characters... ${job_row}:",
            extra={"json_fields": log_details},
        )
        updateJobRow = {
            "id": job_row["row"]["id"],
            "status": "failed",
            "details": json.dumps(job_details),
        }
        updateJob(pg_db.engine, updateJobRow)
        return {"ok": False}
    
    ######add to db############

    logger.info(
        f"add to db... ${job_row}", extra={"json_fields": log_details}
    )
    try:
        addToDB(df)
    except Exception as e:
        # mark job as failed if failed
        log_details["error"] = e
        logger.info(
            f"failed adding to db... ${job_row}:",
            extra={"json_fields": log_details},
        )
        updateJobRow = {
            "id": job_row["row"]["id"],
            "status": "failed",
            "details": json.dumps(job_details),
        }
        updateJob(pg_db.engine, updateJobRow)
        return {"ok": False}
    
    job_details = {"type": job_type, "newest_id": since_id, "tweetLimit": tweetLimit}
    # if made this far mark job complete, successs
    updateJobRow = {
        "id": job_row["row"]["id"],
        "status": "success",
        "details": json.dumps(job_details),
    }
    updateJob(pg_db.engine, updateJobRow)
    logger.info(f"job done. {job_row}", extra={"json_fields": log_details})
    return {"ok": True}
    

    

    


    

    
    