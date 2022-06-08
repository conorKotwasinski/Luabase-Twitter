import requests
import datetime
import json
# import solana
import pandas as pd
from clickhouse_driver import Client as ClickHouseClient

# client = Client(

def getChClient():
    client = ClickHouseClient('lua-2.luabase.altinity.cloud',
        user='admin',
        password='V4ri6NNWdV9SVFf',
        port=9440,
        secure=True,
        verify=False,
        database='default',
        compression=True,
        settings = {
            'use_numpy': False,
            'allow_experimental_object_types': 1,
            }
    )
    return client

from solana.rpc.api import Client
import os, sys
p = os.path.abspath('.')
sys.path.insert(1, p)
from luapy.logger import logger

def getSigner(transaction):
    try:
        ak = transaction['transaction']['message']['accountKeys']
        signer = [a['pubkey'] for a in ak if a.get('signer', False)][0]
        return signer
    except Exception as e:
        logger.error('getSigner error: {str(e)}', '', exc_info=True)
        return None

def getBlocks():
    SOL_URL = 'https://dark-still-night.solana-mainnet.quiknode.pro/6dee75570a1a4dda3c8230fb9d3eedede85c083b/'
    solana_client = Client(SOL_URL)
    # opts = {'encoding': 'jsonParsed'}
    # 94,101,948
    slot = 130138255
    block = solana_client.get_confirmed_block(slot, encoding='jsonParsed')
    transactions = block['result']['transactions']

    rows = []
    t = datetime.datetime.utcfromtimestamp(block['result'].get('blockTime', 0))
    for index, transaction  in enumerate(transactions):

        row = {
            'block_slot': slot,
            'block_timestamp': t,
            'block_hash': block['result']['blockhash'],
            'index': index,
            'id': transaction['transaction']['signatures'][0],
            'signer': getSigner(transaction),
            # 'details': transaction,
            'details': json.dumps(transaction['transaction']),
            # 'details': json.dumps({'this': 'that'}),
            # 'details': '',
        }
        rows.append(row)

    print('rowsrowsrowsrows:', json.dumps(rows[:2], indent=4, sort_keys=True, default=str))

    chClient = getChClient()
    # `block_slot` Int64,
    # `block_timestamp` DateTime,
    # `block_hash` LowCardinality(String),
    # `index` UInt16,
    # `id` String CODEC(ZSTD(1)),
    # `signer` LowCardinality(String),
    # `details` JSON,
    sql = '''
    INSERT INTO solana.transactions_raw
        (
            `block_slot`,
            `block_timestamp`,
            `block_hash`,
            `index`,
            `id`,
            `signer`,
            `details`
        ) VALUES
    '''
    logger.info(f'inserting {len(rows)} transactions...')
    logger.info(f'first row is {rows[0]}')
    # df = pd.DataFrame(rows)
    # chClient.insert_dataframe(sql, df)
    # chClient.execute(sql, rows)
