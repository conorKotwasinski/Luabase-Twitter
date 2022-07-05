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
    insertJob
)
pw = 'tvM4udMRZ37sf8Kh'
oldtime = time.time()
originaldf = pd.DataFrame(columns= ['created_at', 'text', 'entities', 'source', 'lang', 'id', 'referenced_tweets', 'author_id', 'public_metrics', 'possibly_sensitive', 'in_reply_to_user_id', 'attachments', 'geo', 'withheld'])
originaldf['isnew'] = False
bearer_token = "AAAAAAAAAAAAAAAAAAAAANbKcAEAAAAAFKZ7zJwUbQbnk16szYN3by%2F2ogc%3Dr4yUwb04hiMnVG2IEDDUcieM2puKvBTVJ3Eyjs2Dsjnp1UxZNd"
query_params = {'query': '(0x OR \.eth OR ethereum OR bitcoin OR btc OR etherscan OR web3 OR solana OR luna OR terra OR polygon OR nft -is:retweet)', 'max_results': "100", 'tweet.fields':"id,text,attachments,author_id,created_at,entities,geo,in_reply_to_user_id,lang,possibly_sensitive,referenced_tweets,source,public_metrics,withheld"}
search_url = "https://api.twitter.com/2/tweets/search/recent"

def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    if response.status_code == 429:
        time.sleep(2)
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


def waitAndFetch(tweetRate):
    now = time.time()
    global oldtime
    if now < oldtime + tweetRate:
        time.sleep(oldtime + tweetRate - now)
        oldtime = now
        return connect_to_endpoint(search_url,query_params)['data']
    else:    
        oldtime = now
        return connect_to_endpoint(search_url,query_params)['data']

def filterAndAddToDB(df):
    df['isnew'] = True
    global originaldf
    newdf = pd.concat([df,originaldf], axis=0)
    newdf = newdf.drop_duplicates(subset=[col for col in newdf.columns if col != 'isnew' and col != 'referenced_tweets'], keep=False)
    newdf = newdf[newdf.isnew == True]
    newdf.pop('isnew')
    chClient = clickhouse_connect.get_client(host='twitter-test.luabase.altinity.cloud', port=8443, username='admin', password='tvM4udMRZ37sf8Kh')
    chClient.insert_df('twitter.tweets', newdf)
    originaldf = df
    originaldf['isnew'] = False
       
def extract_tweets_load(
    pg_db,
    tweetLimit,
    tweetRate,
    tweetDuration,
    tweetsPerRequest,
    ):

    logger.info(f"started getting tweets")
    ######check if another job is currently running######
    #job_summary = getJobSummary(pg_db, "getTweets")
    #if job_summary["running"] >= 1:
    #    logger.info(f"already another job running!")
    #    return {"ok": True, "status": f"max of 1 job already running"}

    if tweetsPerRequest > 100:
        logger.info(f"too many tweets per request")
        return {"ok": True, "status": f"cannot have more than 100 tweets per request"}

    job_details = {
    "type": "getTweets",
    "tweetLimit": tweetLimit,
    "tweetRate": tweetRate,
    "tweetDuration": tweetDuration,
    "tweetsPerRequest": tweetsPerRequest
    }
    job_row = {
        "type": job_details["type"],
        "status": "running",
        "details": json.dumps(job_details),
    }
    job_row = insertJob(pg_db.engine, job_row)
    log_details = {"type": "getTweets", "id": job_row["row"]["id"]}
    logger.info(f"getting twitter data... ${job_row}", extra={"json_fields": log_details})
    
    num_tweets = 0
    duration = 0
    starttime = time.time()

    while((tweetLimit == None or num_tweets <= tweetLimit + tweetsPerRequest) and (tweetDuration == None or duration <= tweetDuration)):
        logger.info(f"in the while loop")
        df = pd.DataFrame(waitAndFetch(tweetRate))
        createcolumns(df)
        fillNaN(df)
        objecttostring(df)
        editType(df)
        removeNonASCIIchar(df)
        filterAndAddToDB(df)
        num_tweets
        duration = time.time() - starttime
        num_tweets += tweetsPerRequest
        
        
    log_details = {"type": "getTweets"}
    logger.info(f"job done. {job_row}", extra={"json_fields": log_details})
    return {"ok": True}

    

    

    


    

    
    