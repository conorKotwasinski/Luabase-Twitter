from bitcoinetl.enumeration.chain import Chain
from bitcoinetl.jobs.enrich_transactions import EnrichTransactionsJob
from bitcoinetl.jobs.export_blocks_job import ExportBlocksJob
from bitcoinetl.service.btc_service import BtcService
from bitcoinetl.streaming.btc_item_id_calculator import BtcItemIdCalculator
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from bitcoinetl.service.btc_block_range_service import BtcBlockRangeService
from bitcoinetl.jobs.enrich_transactions import EnrichTransactionsJob
from clickhouse_driver import Client
import pandas as pd
from datetime import datetime, timedelta
from logger import logger
import sqlalchemy
from flask_cors import CORS 
from flask_sqlalchemy import SQLAlchemy
from flask import Flask
import flask
import json
from utils.pg_db_utils import insertJob, updateJob, getJobSummary, getDoneMaxJob

# function for getting max block in clickhouse
def get_max_btc_db_block(clickhouse_client, target = 'both'):
    if target == 'both':
        blocks_max_block = clickhouse_client.execute('select max(number) from bitcoin.blocks_raw')[0][0]
        transactions_max_block = clickhouse_client.execute('select max(block_number) from bitcoin.transactions_raw')[0][0]
        assert blocks_max_block == transactions_max_block, 'max block for blocks and transactions do not match'
        max_block = blocks_max_block
    elif target == 'block':
        sql = 'select max(number) from bitcoin.blocks_raw;'
        max_block = clickhouse_client.execute('select max(number) from bitcoin.blocks_raw')[0][0]
    elif target == 'transaction':
        sql = 'select max(block_number) from bitcoin.transactions_raw;'
        max_block = clickhouse_client.execute('select max(block_number) from bitcoin.transactions_raw')[0][0]
    return max_block

#function to grab raw block data from quicknode
def btc_block_exporter(uri, start_block, end_block):
    blocks_item_exporter = InMemoryItemExporter(item_types=['block'])
    blocks_job = ExportBlocksJob(
        start_block=start_block,
        end_block=end_block,
        batch_size=1,
        bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(uri)),
        max_workers=5,
        item_exporter=blocks_item_exporter,
        chain=Chain.BITCOIN,
        export_blocks=True,
        export_transactions=False
    )
    blocks_job.run()
    blocks = blocks_item_exporter.get_items('block')
    return blocks

#function to grab raw transaction data from quicknode
def btc_transaction_exporter(uri, start_block, end_block):
    blocks_item_exporter = InMemoryItemExporter(item_types=['transaction'])
    blocks_job = ExportBlocksJob(
        start_block=start_block,
        end_block=end_block,
        batch_size=1,
        bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(uri)),
        max_workers=5,
        item_exporter=blocks_item_exporter,
        chain=Chain.BITCOIN,
        export_blocks=False,
        export_transactions=True
    )
    blocks_job.run()
    transactions = blocks_item_exporter.get_items('transaction')
    return transactions


# function to grab raw block and transaction data from quicknode
def btc_block_transaction_exporter(uri, start_block, end_block):
    blocks_item_exporter = InMemoryItemExporter(item_types=['block', 'transaction'])
    blocks_job = ExportBlocksJob(
        start_block=start_block,
        end_block=end_block,
        batch_size=1,
        bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(uri)),
        max_workers=5,
        item_exporter=blocks_item_exporter,
        chain=Chain.BITCOIN,
        export_blocks=True,
        export_transactions=True
    )
    blocks_job.run()
    blocks = blocks_item_exporter.get_items('block')
    transactions = blocks_item_exporter.get_items('transaction')
    return blocks, transactions

# function to grab raw enriched transaction data from quicknode
def btc_enriched_transaction_exporter(uri, transactions):
    transactions_item_exporter = InMemoryItemExporter(item_types=['transaction'])
    job = EnrichTransactionsJob(
        transactions_iterable = (transactions),
        batch_size = 1,
        bitcoin_rpc = ThreadLocalProxy(lambda: BitcoinRpc(uri)),
        max_workers = 5,
        item_exporter = transactions_item_exporter,
        chain = Chain.BITCOIN
    )
    job.run()
    enriched_transactions = transactions_item_exporter.get_items('transaction')
    return enriched_transactions


#function that gets new raw data within start and end blocks
def get_new_btc_data(clickhouse_client, uri, start_block, end_block, target = 'both'):
    #get new blocks 
    if target == 'block':
        try_counter = 1
        data_pull_succeeded = False
        while try_counter <= 10 and data_pull_succeeded == False:
            try:
                blocks = btc_block_exporter(uri, start_block, end_block)
                data_pull_succeeded = True
            except Exception as e:
                try_counter += 1
                print(e)
                print('failed getting block data from quicknode on try {try_counter}'.format(try_counter = try_counter))
        if try_counter > 10:
            raise AttributeError('failed to get new data from quicknode after 10 tries')
        else:
            return {'blocks':blocks}
    
    #get new transactions
    elif target == 'transaction':
        try_counter = 1
        data_pull_succeeded = False
        while try_counter <= 10 and data_pull_succeeded == False:
            try:
                transactions = btc_transaction_exporter(uri, start_block, end_block)
                data_pull_succeeded = True
            except Exception as e:
                try_counter += 1
                print(e)
                print('failed getting transaction data from quicknode on try {try_counter}'.format(try_counter = try_counter))
        if try_counter > 10:
            raise AttributeError('failed to get new data from quicknode after 10 tries')

        #retrieve enriched transaction data 
        data_pull_succeeded = False
        try_counter = 1
        while try_counter <= 10 and data_pull_succeeded == False:
            try:
                transactions= btc_enriched_transaction_exporter(uri, transactions)
                data_pull_succeeded = True
            except Exception as e:
                try_counter += 1
                print(e)
                print('failed getting enriched transaction data from quicknode on try {try_counter}'.format(try_counter = try_counter))
        
        if try_counter > 10:
            raise AttributeError('failed to get new data from quicknode after 10 tries')
        else:
            return {'transactions':transactions}

    #get new blocks and transactions
    elif target == 'both':
        try_counter = 1
        data_pull_succeeded = False
        while try_counter <= 10 and data_pull_succeeded == False:
            try:
                blocks, transactions = btc_block_transaction_exporter(uri, start_block, end_block)
                data_pull_succeeded = True
            except Exception as e:
                try_counter += 1
                print(e)
                print('failed getting block & transaction data from quicknode on try {try_counter}'.format(try_counter = try_counter))
        if try_counter > 10:
            raise AttributeError('failed to get new data from quicknode after 10 tries')

        #retrieve enriched transaction data 
        try_counter = 1
        data_pull_succeeded = False
        while try_counter <= 10 and data_pull_succeeded == False:
            try:
                transactions = btc_enriched_transaction_exporter(uri, transactions)
                data_pull_succeeded = True
            except Exception as e:
                try_counter += 1
                print(e)
                print('failed getting enriched transaction data from quicknode on try {try_counter}'.format(try_counter = try_counter))
        if try_counter > 10:
            raise AttributeError('failed to get new data from quicknode after 10 tries')
        else:
            return {'blocks':blocks, 'transactions':transactions}


#function to convert raw blocks json to pandas df   
def blocks_json_to_df(blocks_json):
    blocks_df = pd.DataFrame(blocks_json)
    blocks_df['timestamp'] = pd.to_datetime(blocks_df.timestamp, unit = 's', origin = 'unix')
    blocks_df['timestamp'] = pd.DatetimeIndex(blocks_df.timestamp)
    del blocks_df['type']
    return blocks_df


#function to extract transaction inputs and outputs
def txn_input_output(transactions_json):
    transaction_inputs = []
    transaction_outputs = []
    for txn in transactions_json:    
        inputs = txn['inputs']
        outputs = txn['outputs']

        if len(inputs) == 0:
            pass 
        else:
            for input in inputs:
                input['transaction_hash'] = txn['hash']
                input['block_number'] = txn['block_number']
                input['block_hash'] = txn['block_hash']
                input['block_timestamp'] = txn['block_timestamp']
                transaction_inputs.append(input)

        if len(outputs) == 0:
            pass 
        else:
            for output in outputs:
                output['transaction_hash'] = txn['hash']
                output['block_number'] = txn['block_number']
                output['block_hash'] = txn['block_hash']
                output['block_timestamp'] = txn['block_timestamp']
                transaction_outputs.append(output)
    return transaction_inputs, transaction_outputs

#function to convert transaction jsons to pandas df
def txn_json_to_df(transactions_json, transaction_inputs, transaction_outputs):
    
    transactions_df = pd.DataFrame(transactions_json)
    transactions_df['block_timestamp'] = pd.to_datetime(transactions_df.block_timestamp, unit = 's', origin = 'unix')
    transactions_df['block_timestamp'] = pd.DatetimeIndex(transactions_df.block_timestamp)
    transactions_df['is_coinbase'] = transactions_df.is_coinbase.map({True:1, False:0})
    transactions_df = transactions_df.rename(columns = {'index':'transaction_index'})
    del transactions_df['inputs']
    del transactions_df['outputs']

    transaction_inputs_df = pd.DataFrame(transaction_inputs)
    transaction_inputs_df = transaction_inputs_df.rename(columns = {'index':'input_index'})
    transaction_inputs_df['block_timestamp'] = pd.to_datetime(transaction_inputs_df.block_timestamp, unit = 's', origin = 'unix')

    transaction_outputs_df = pd.DataFrame(transaction_outputs)
    transaction_outputs_df = transaction_outputs_df.rename(columns = {'index':'output_index'})
    transaction_outputs_df['block_timestamp'] = pd.to_datetime(transaction_outputs_df.block_timestamp, unit = 's', origin = 'unix')

    del transactions_df['type']
    return transactions_df, transaction_inputs_df, transaction_outputs_df

#functions for loading into clickhouse
def load_blocks(blocks_df, clickhouse_client):
    blocks_load_query = '''
    INSERT INTO bitcoin.blocks_raw (
        hash, 
        size, 
        stripped_size, 
        weight, 
        number, 
        version, 
        merkle_root, 
        timestamp, 
        nonce, 
        bits, 
        coinbase_param, 
        transaction_count
        ) VALUES
        '''
    clickhouse_client.insert_dataframe(blocks_load_query, blocks_df)
    print('loaded new blocks into db')
    
def load_transactions(transactions_df, transaction_inputs_df, transaction_outputs_df, clickhouse_client):
    txns_load_sql = '''
    INSERT INTO bitcoin.transactions_raw
    (
        hash,
        size,
        virtual_size,
        version,
        lock_time,
        block_number,
        block_hash,
        block_timestamp,
        is_coinbase,
        transaction_index,
        input_count,
        output_count,
        input_value,
        output_value,
        fee
    ) VALUES
    '''
    clickhouse_client.insert_dataframe(txns_load_sql, transactions_df)
    print('loaded new transactions into db')

    #load inputs df into clickhouse 
    inputs_load_sql = '''
    INSERT INTO bitcoin.transaction_inputs_raw
    (
        input_index,
        spent_transaction_hash,
        spent_output_index,
        script_asm,
        script_hex,
        sequence,
        required_signatures,
        type,
        addresses,
        value,
        transaction_hash,
        block_number,
        block_hash,
        block_timestamp
    ) VALUES
    '''
    clickhouse_client.insert_dataframe(inputs_load_sql, transaction_inputs_df)
    print('loaded new transaction inputs into db')

    #load outputs df into clickhouse
    outputs_load_sql = '''
    INSERT INTO bitcoin.transaction_outputs_raw
    (
        output_index,
        script_asm,
        script_hex,
        required_signatures,
        type,
        addresses,
        value,
        transaction_hash,
        block_number,
        block_hash,
        block_timestamp
    ) VALUES
    '''
    clickhouse_client.insert_dataframe(outputs_load_sql, transaction_outputs_df)
    print('loaded new transaction outputs into db')

#takes extracted data and loads into clickhouse
def transform_load_btc_data(new_data, clickhouse_client):
    #load blocks and transactions
    if 'blocks' in new_data.keys() and 'transactions' in new_data.keys():
        print('transforming new blocks and transactions')
        blocks = new_data['blocks']
        transactions = new_data['transactions']
        #extract transaction input/outputs
        transaction_inputs, transaction_outputs = txn_input_output(transactions)

        #convert to pandas df 
        blocks_df = blocks_json_to_df(blocks)
        transactions_df, transaction_inputs_df, transaction_outputs_df = txn_json_to_df(transactions, transaction_inputs, transaction_outputs)

        #load into clickhouse 
        print('loading new blocks and transactions')
        load_blocks(blocks_df, clickhouse_client)
        load_transactions(transactions_df, transaction_inputs_df, transaction_outputs_df, clickhouse_client)

    #load just blocks
    elif 'blocks' in new_data.keys() and 'transactions' not in new_data.keys():
        print('transforming new blocks')
        blocks = new_data['blocks']

        #convert to pandas df 
        blocks_df = blocks_json_to_df(blocks)

        #load into clickhouse 
        print('loading new blocks')
        load_blocks(blocks_df, clickhouse_client)

    #load just transactions 
    elif 'blocks' not in new_data.keys() and 'transactions' in new_data.keys():
        print('transforming new transactions')
        transactions = new_data['transactions']
        #extract transaction input/outputs
        transaction_inputs, transaction_outputs = txn_input_output(transactions)

        #convert to pandas df 
        transactions_df, transaction_inputs_df, transaction_outputs_df = txn_json_to_df(transactions, transaction_inputs, transaction_outputs)

        #load into clickhouse 
        print('loading new transactions')
        load_transactions(transactions_df, transaction_inputs_df, transaction_outputs_df, clickhouse_client)

        
#function puts it all together, gets raw data, transforms, and loads data
def extract_transform_load_btc(clickhouse_client, node_uri, pg_db, target = 'both', lag = 6, start_block = None, end_block = None):
    
    ######check if another job is currently running######
    job_summary = getJobSummary(pg_db, 'getBtcEtl')
    if job_summary['running'] >= 1:
        logger.info(f'already another job running!')
        return {'ok': True, 'status': f"max of 1 job already running"}

    #####set start and end blocks######
    #get max block+1 in db if start block is null
    if start_block == None:
        max_job = getDoneMaxJob(pg_db.engine, job_summary['max_id'])
        start_block = max_job['details'].get('end', -1) + 1
    #     start_block = get_max_btc_db_block(clickhouse_client, target)
    #     start_block += 1
    #     start_block = int(start_block)

    #get max block in blockchain and subtract lag if end block is null
    if end_block == None:
        try_counter = 1
        data_pull_succeeded = False

        while try_counter <= 10 and data_pull_succeeded == False:
            try:
                bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(node_uri))
                end_block = bitcoin_rpc.getblockcount()
                end_block -= lag
                end_block = int(end_block)
                data_pull_succeeded = True
            except Exception as e:
                logger.info(f'failed getting max block number from quicknode on try ${try_counter}:', e)
                try_counter += 1
        if try_counter > 10:
            raise AttributeError('failed to get max block number from quicknode after 10 tries')

    #check if blockchain max block > start_block 
    if end_block < start_block:
        logger.info(f"Already caught up!")
        return {'ok': True}
    
    ######extract new btc data############
    # insert new job that is running
    job_details = {
        "type": "getBtcEtl",
        "subtype":target,
        "start": start_block,
        "end": end_block
    }
    job_row = {
        'type': job_details['type'],
        'status': 'running',
        'details': json.dumps(job_details)
    }
    job_row = insertJob(pg_db.engine, job_row)
    logger.info(f'getting bitcoin data... ${job_row}')
    try:
        new_data = get_new_btc_data(clickhouse_client, node_uri, start_block, end_block, target)
    except Exception as e:
        logger.info(f'failed getting bitcoin data... ${job_row}:', e)
        updateJobRow = {
            'id': job_row['row']['id'],
            'status': 'failed',
            'details': json.dumps(job_details)
        }
        updateJob(pg_db.engine, updateJobRow)
        return {'ok':False}

    logger.info(f'completed getting bitcoin data... ${job_row}')

    ######transform and load btc data########## 
    logger.info(f'transforming and loading new bitcoin data... ${job_row}')
    try:
        transform_load_btc_data(new_data, clickhouse_client)
    except Exception as e:
        logger.info(f'failed loading bitcoin data... ${job_row}:', e)
        updateJobRow = {
            'id': job_row['row']['id'],
            'status': 'failed',
            'details': json.dumps(job_details)
        }
        updateJob(pg_db.engine, updateJobRow)
        return {'ok':False}
    # mark job complete, successs
    updateJobRow = {
        'id': job_row['row']['id'],
        'status': 'success',
        'details': json.dumps(job_details)
    }
    updateJob(pg_db.engine, updateJobRow)
    logger.info(f"job done. {job_row}")
    return {'ok': True}
