import requests
import pandas as pd
import numpy as np
import re
import json
import clickhouse_connect
import os
from luapy.logger import logger
from luapy.utils.pg_db_utils import (
    insertJob
)

url = "https://api.luabase.com/run"
payload = {
  "uuid": "ba0f3e2bbb1443888e1d4b1505225cfe",
  "parameters": {}
}
headers = {"content-type": "application/json"}
bearer_token = "AAAAAAAAAAAAAAAAAAAAAGhIeQEAAAAAiDsOi9PtEdJrSKzgMa486EUB5ZQ%3Dc4IBhrYP1vcRj6IQBhyXyL7A0opvAlLvReQS56IETQ5SCypO6g"
search_url = "https://api.twitter.com/2/users/by?usernames=twitterdev,twitterapi,adsapi&user.fields=created_at&expansions=pinned_tweet_id&tweet.fields=author_id,created_at"
user = os.getenv('user')
pw = os.environ.get('pw')



def getAddressesAndHandles():
    return requests.request("POST", url, json=payload, headers=headers).json()







def filterHandles(handles, removedhandles, updatedhandles):
    for handle in list(handles):
        editedhandle = handle
        if handle == "":
            handles.remove(handle)
            removedhandles.append(handle)
            continue
        
        if handle[0:4] == 'www.':
            handles[handles.index(handle)] = handle[4:]
            handle = handle[4:]
            updatedhandles[handle] = editedhandle
        if handle[0:12] == 'Twitter.com/':
            handles[handles.index(handle)] = handle[12:]
            handle = handle[12:]
            updatedhandles[handle] = editedhandle
        if handle[0:12] == 'https://www.':
            handles[handles.index(handle)] = handle[12:]
            handle = handle[12:]
        if handle[0:8] == 'https://':
            handles[handles.index(handle)] = handle[8:]
            handle = handle[8:]
        if handle[0:7] == 'http://':
            handles[handles.index(handle)] = handle[7:]
            handle = handle[7:]
            updatedhandles[handle] = editedhandle
        if handle[0] == '@':
            handles[handles.index(handle)] = handle[1:]
            handle = handle[1:]
            updatedhandles[handle] = editedhandle
        if handle[0] == '/':
            handles[handles.index(handle)] = handle[1:]
            handle = handle[1:]
            updatedhandles[handle] = editedhandle
        if handle[0] == '.':
            handles[handles.index(handle)] = handle[1:]
            handle = handle[1:]
            updatedhandles[handle] = editedhandle
        if handle[0] == ':':
            handles[handles.index(handle)] = handle[1:]
            handle = handle[1:]
            updatedhandles[handle] = editedhandle


        if handle == "":
            handles.remove(handle)
            removedhandles.append(handle)
        elif "Twitter" in handle or "Admin" in handle:
            handles.remove(handle)
            removedhandles.append(editedhandle)
        elif not re.match(r'^[A-Za-z0-9_]+$', handle):
            handles.remove(handle)
            removedhandles.append(editedhandle)
        elif len(handle)>15 or len(handle) < 4:
            handles.remove(handle)
            removedhandles.append(editedhandle)
    return handles, removedhandles, updatedhandles

def findIndexes(originalhandles, removedhandles, indexes):
    for i in range(len(originalhandles)):
        if originalhandles[i] in removedhandles:
            indexes.append(i)
    return indexes


def create_url(usernames):
    user_fields = "user.fields=description,created_at,entities,location,pinned_tweet_id,profile_image_url,protected,public_metrics,url,verified,withheld"
    url = "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)
    return url

def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def connect_to_endpoint(url,counter):
    response = requests.request("GET", url, auth=bearer_oauth,)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()



def getUsers(handles, errorusername, users, errors):
    counter = 0
    for counter in range(int(len(handles)/100)):
        usernames = "usernames="
        for handle in range(100):
            usernames += handles[handle +counter*100]+','
        usernames = usernames[:-1]
        url = create_url(usernames)
        json_response = connect_to_endpoint(url,counter)
        users = users + json_response['data']
        errors = errors + json_response['errors']
        counter += 1

    for username in errors:
        errorusername.append(username['value'])
    
    return users, errorusername, errors

def updateIndexes(indexes, errorusername, originalhandles, updatedhandles, df):
    keys = list(updatedhandles.keys())
    for username in errorusername:
        if username in keys:
            indexes.append(originalhandles.index(updatedhandles[username]))
        elif username in originalhandles:
            indexes.append(originalhandles.index(username))
    tobereviewed = df.iloc[indexes, :]
    tobereviewed.to_csv("tobereviewed.csv") 
    return indexes


def sql(df):
    chClient = clickhouse_connect.get_client(host='twitter-test.luabase.altinity.cloud', port=8443, username=user, password='tvM4udMRZ37sf8Kh')
    sql = '''
    DROP TABLE twitter.users
    '''
    chClient.command(sql)
    sql = '''
    CREATE TABLE twitter.users
    (
        `id` Int64,
        `description` String,
        `location` String,
        `entities` String,
        `name` String,
        `pinned_tweet_id` String,
        `profile_image_url` String,
        `protected` UInt8,
        `created_at` DateTime('UTC'),
        `public_metrics` String,
        `url` String,
        `username` String,
        `verified` UInt8,
        `protect` UInt8

    )
    ENGINE = MergeTree
    PRIMARY KEY id
    ORDER BY id
    '''
    chClient.command(sql)
    chClient.insert_df('twitter.users', df)

def editType(df):
    df.created_at = pd.to_datetime(df.created_at)
    columns = ['entities', 'public_metrics', 'pinned_tweet_id']
    for col in columns:
        df[col] = df[col].astype('str') 
    return df


def fillBlank(df):
    df.loc[df['location'].isnull(),['location']] = df.loc[df['location'].isnull(),'location'].apply(lambda x: '')
    df.loc[df['pinned_tweet_id'].isnull(),['pinned_tweet_id']] = df.loc[df['pinned_tweet_id'].isnull(),'pinned_tweet_id'].apply(lambda x: '')
    return df

def ascii(df):
    columns = ['name', 'entities', 'location', 'description']
    for col in columns:
        df[col] = df[col].apply(lambda x: x.encode('ascii', 'ignore').decode('ascii'))
    return df


def extract_users_load(pg_db):
    job_details = {
    "type": "getTwitterUsers"

    }
    job_row = {
        "type": job_details["type"],
        "status": "running",
        "details": json.dumps(job_details),
    }
    job_row = insertJob(pg_db.engine, job_row)
    logger.info(f"started getting from database")
    df = pd.DataFrame(getAddressesAndHandles()['data'])
    handles = df['handle'].to_numpy()
    originalhandles = handles.tolist()
    handles = np.unique(handles)
    handles = handles.tolist()
    removedhandles = []
    updatedhandles = {}
    users = []
    errors = []
    errorusername = []
    indexes = []
    logger.info(f"started filtering")
    handles, removedhandles, updatedhandles = filterHandles(handles, removedhandles, updatedhandles)
    indexes = findIndexes(originalhandles, removedhandles, indexes)
    users, errorusername, errors =  getUsers(handles, errorusername, users, errors)
    indexes = updateIndexes(indexes, errorusername, originalhandles, updatedhandles, df)
    logger.info(f"created user df")
    usersdf = pd.DataFrame(users)
    usersdf.rename(columns={"protected": "protect"})
    logger.info(f"editing user df")
    usersdf = editType(usersdf)
    usersdf = fillBlank(usersdf)
    usersdf = ascii(usersdf)
    logger.info(f"started sql")
    sql(usersdf)
    logger.info(f"finished sql")
    

