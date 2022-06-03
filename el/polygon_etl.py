from utils.blockchain_etl_utils import InMemoryObjectExporter, EthInMemoryAdapter
import pandas as pd
from blockchainetl_common.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from polygonetl.thread_local_proxy import ThreadLocalProxy
from polygonetl.providers.auto import get_provider_from_uri
from utils.pg_db_utils import insertJob, updateJob, getJobSummary, getMaxJob
from web3 import Web3, HTTPProvider
import logging
from logger import logger

#function to extract raw polygon data from node
default_item_types = ['block', 'transaction', 'log','token_transfer', 'trace', 'contract', 'token']
def extract_polygon_data(uri, start_block, end_block, lag, item_types = default_item_types):
    
    logging.getLogger('polygonetl.service.token_transfer_extractor').setLevel(logging.ERROR)
    logging.getLogger('evmdasm.disassembler').setLevel(logging.CRITICAL)
    logging.getLogger('eth_token_service').setLevel(logging.CRITICAL)
    logging.getLogger('BatchWorkExecutor').setLevel(logging.CRITICAL)
    
    polygon_adapter = EthInMemoryAdapter(
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(uri, batch=True)),
        item_exporter=InMemoryItemExporter(item_types=item_types),
        batch_size=100,
        max_workers=5,
        entity_types=','.join(item_types)
    )
    
    polygon_exporter = InMemoryObjectExporter(
        blockchain_streamer_adapter=polygon_adapter,
        lag=lag,
        start_block=start_block,
        end_block = end_block,
    )
    
    try_counter = 1
    data_pull_succeeded = False
    while try_counter <= 10 and data_pull_succeeded == False:
        try:
            all_items = polygon_exporter.export()
            data_pull_succeeded = True
        except Exception as e:
            print(f'failed getting polygon data from quicknode on try {try_counter}:', e)
            try_counter += 1
    if try_counter > 10:
        raise AttributeError('failed to get polygon data from quicknode after 10 tries')
    else:
        return all_items


#functions for transforming raw polygon data before loading
def blocks_to_df(blocks):
    blocks_df = pd.DataFrame(blocks)
    keep_cols = [
         'number',
         'hash',
         'parent_hash',
         'nonce',
         'sha3_uncles',
         'logs_bloom',
         'transactions_root',
         'state_root',
         'receipts_root',
         'miner',
         'difficulty',
         'total_difficulty',
         'size',
         'extra_data',
         'gas_limit',
         'gas_used',
         'timestamp',
         'transaction_count',
         'base_fee_per_gas'
    ]
    blocks_df['timestamp'] = pd.to_datetime(blocks_df.timestamp, unit = 's', origin = 'unix')
    blocks_df['timestamp'] = pd.DatetimeIndex(blocks_df.timestamp)
    blocks_df = blocks_df[keep_cols]
    return blocks_df

def transactions_to_dict(transactions):
    keep_keys = [
         'hash',
         'nonce',
         'transaction_index',
         'from_address',
         'to_address',
         'value',
         'gas',
         'gas_price',
         'input',
         'block_timestamp',
         'block_number',
         'block_hash',
         'max_fee_per_gas',
         'max_priority_fee_per_gas',
         'transaction_type',
         'receipt_cumulative_gas_used',
         'receipt_gas_used',
         'receipt_contract_address',
         'receipt_status',
         'effective_gas_price' 
    ]
    transactions_dict = [{k: v for k, v in d.items() if k in keep_keys} for d in transactions]
    transactions_dict = [{k: row[k] for k in keep_keys} for row in transactions_dict]
    return transactions_dict

def traces_to_dict(traces):
    keep_keys = [
         'transaction_hash',
         'transaction_index',
         'from_address',
         'to_address',
         'value',
         'input',
         'output',
         'trace_type',
         'call_type',
         'reward_type',
         'gas',
         'gas_used',
         'subtraces',
         'trace_address',
         'error',
         'status',
         'trace_id',
         'block_number',
         'block_timestamp',
         'block_hash'
    ]
    traces_dict = [{k: v for k, v in d.items() if k in keep_keys} for d in traces]
    traces_dict = [{k: row[k] for k in keep_keys} for row in traces_dict]
    return traces_dict

def tokens_to_dict(tokens):
    keep_keys = [
         'address',
         'symbol',
         'name',
         'decimals',
         'total_supply',
         'block_number',
         'block_timestamp',
         'block_hash' 
    ]

    tokens_dict = [{k: v for k, v in d.items() if k in keep_keys} for d in tokens]
    tokens_dict = [{k: row[k] for k in keep_keys} for row in tokens_dict]
    return tokens_dict

def token_transfers_to_dict(token_transfers):
    keep_keys = [
         'token_address',
         'from_address',
         'to_address',
         'value',
         'transaction_hash',
         'log_index',
         'block_number',
         'block_timestamp',
         'block_hash'
    ]

    token_transfers_dict = [{k: v for k, v in d.items() if k in keep_keys} for d in token_transfers]
    token_transfers_dict = [{k: row[k] for k in keep_keys} for row in token_transfers_dict]
    return token_transfers_dict

def contracts_to_df(contracts):
    keep_cols = [
     'address',
     'bytecode',
     'function_sighashes',
     'is_erc20',
     'is_erc721',
     'block_number',
     'block_timestamp',
     'block_hash'
    ]
    if len(contracts) == 0:
        contracts_df = pd.DataFrame(columns = keep_cols)
    else:
        contracts_df = pd.DataFrame(contracts)
        contracts_df = contracts_df[keep_cols]
        contracts_df['is_erc20'] = contracts_df.is_erc20.map({False:0, True:1})
        contracts_df['is_erc721'] = contracts_df.is_erc721.map({False:0, True:1})
        contracts_df['block_timestamp'] = pd.to_datetime(contracts_df.block_timestamp, unit = 's', origin = 'unix')
        contracts_df['block_timestamp'] = pd.DatetimeIndex(contracts_df.block_timestamp)
    return contracts_df

def logs_to_df(logs):
    logs_df = pd.DataFrame(logs)
    keep_cols = [
         'log_index',
         'transaction_hash',
         'transaction_index',
         'address',
         'data',
         'topics',
         'block_number',
         'block_timestamp',
         'block_hash'
    ]
    logs_df = logs_df[keep_cols]
    logs_df['block_timestamp'] = pd.to_datetime(logs_df.block_timestamp, unit = 's', origin = 'unix')
    logs_df['block_timestamp'] = pd.DatetimeIndex(logs_df.block_timestamp)
    return logs_df

def transform_polygon_data(all_items):
    func_dict = {
        'blocks':blocks_to_df,
        'transactions':transactions_to_dict,
        'logs':logs_to_df,
        'token_transfers':token_transfers_to_dict,
        'traces':traces_to_dict,
        'contracts':contracts_to_df,
        'tokens':tokens_to_dict
    }
    
    all_transformed = {}
    for key in all_items.keys():
        logger.info(f'transforming polygon {key} data')
        all_transformed[key] = func_dict[key](all_items[key])
        
    return all_transformed


#function to load data
def load_polygon_data(all_transformed, clickhouse_client, non_np_clickhouse_client, db = 'polygon'):
    blocks_load_sql = '''
    INSERT INTO {db}.blocks_raw
    (
        number ,
        hash ,
        parent_hash ,
        nonce ,
        sha3_uncles ,
        logs_bloom ,
        transactions_root ,
        state_root ,
        receipts_root ,
        miner ,
        difficulty ,
        total_difficulty ,
        size ,
        extra_data ,
        gas_limit ,
        gas_used ,
        timestamp ,
        transaction_count ,
        base_fee_per_gas
    ) VALUES
    '''.format(db = db)

    txns_load_sql = '''
    INSERT INTO {db}.transactions_raw
    (
        hash ,
        nonce ,
        transaction_index ,
        from_address ,
        to_address ,
        value ,
        gas ,
        gas_price ,
        input ,
        block_timestamp ,
        block_number ,
        block_hash ,
        max_fee_per_gas ,
        max_priority_fee_per_gas ,
        transaction_type ,
        receipt_cumulative_gas_used ,
        receipt_gas_used ,
        receipt_contract_address ,
        receipt_status ,
        effective_gas_price
    ) VALUES
    '''.format(db = db)

    logs_load_sql = '''
    INSERT INTO {db}.logs_raw
    (
        log_index ,
        transaction_hash ,
        transaction_index ,
        address ,
        data ,
        topics ,
        block_number ,
        block_timestamp ,
        block_hash
    ) VALUES
    '''.format(db = db)

    token_transfers_load_sql = '''
    INSERT INTO {db}.token_transfers_raw
    (
        token_address ,
        from_address ,
        to_address ,
        value ,
        transaction_hash ,
        log_index ,
        block_number ,
        block_timestamp ,
        block_hash
    ) VALUES
    '''.format(db = db)

    traces_load_sql = '''
    INSERT INTO {db}.traces_raw
    (
        transaction_hash ,
        transaction_index ,
        from_address ,
        to_address ,
        value ,
        input ,
        output ,
        trace_type ,
        call_type ,
        reward_type ,
        gas ,
        gas_used ,
        subtraces ,
        trace_address ,
        error ,
        status ,
        trace_id ,
        block_number ,
        block_timestamp ,
        block_hash
    ) VALUES
    '''.format(db = db)

    contracts_load_sql = '''
    INSERT INTO {db}.contracts_raw
    (
        address ,
        bytecode ,
        function_sighashes ,
        is_erc20 ,
        is_erc721 ,
        block_number ,
        block_timestamp ,
        block_hash
    ) VALUES
    '''.format(db = db)

    tokens_load_sql = '''
    INSERT INTO {db}.tokens_raw
    (
        address ,
        symbol ,
        name ,
        decimals ,
        total_supply ,
        block_number ,
        block_timestamp ,
        block_hash
    ) VALUES
    '''.format(db = db)
    
    load_dict = {
        'blocks':blocks_load_sql,
        'transactions':txns_load_sql,
        'logs':logs_load_sql,
        'token_transfers':token_transfers_load_sql,
        'traces':traces_load_sql,
        'contracts':contracts_load_sql,
        'tokens':tokens_load_sql
    }
    
    for key in all_transformed.keys():
        logger.info(f'loading polygon {key} data')
        if key == 'transactions':
            non_np_clickhouse_client.execute(load_dict[key], all_transformed[key])
        elif key == 'token_transfers':
            if len(all_transformed[key]) == 0:
                logger.info(f'No token transfers to load!')
                pass
            else:
                non_np_clickhouse_client.execute(load_dict[key], all_transformed[key])
        elif key == 'traces':
            non_np_clickhouse_client.execute(load_dict[key], all_transformed[key])
        elif key == 'tokens':
            if len(all_transformed[key]) == 0:
                logger.info(f'No tokens to load!')
                pass
            else:
                non_np_clickhouse_client.execute(load_dict[key], all_transformed[key])
        else:   
            clickhouse_client.insert_dataframe(load_dict[key], all_transformed[key])


#function to put it all together
def extract_transform_load_polygon(node_uri, 
                                   clickhouse_client, 
                                   non_np_clickhouse_client,
                                   pg_db,
                                   polygon_db = 'polygon',
                                   start_block = None, 
                                   end_block = None,
                                   lag = 100,
                                   max_running = 10,
                                   max_blocks_per_job = 100):
    
    ######check if > max jobs running######
    job_summary = getJobSummary(pg_db, 'getPolygonEtl')
    if job_summary['running'] >= max_running:
        logger.info(f'already another {max_running} jobs running!')
        return {'ok': True, 'status': f"max of {max_running} jobs already running"}
    
    #####set start and end blocks######
    #get max block+1 in latest job if start block is null
    if start_block == None:
        max_job = getMaxJob(db.engine, job_summary['max_id'])
        start_block = max_job['details'].get('end', -1) + 1
    
    #get max blockchain block - lag, and check if block gap exceeds max blocks per job if end block not set
    if end_block == None:
        try_counter = 1
        data_pull_succeeded = False
        
        while try_counter <= 10 and data_pull_succeeded == False:
            try:
                end_block=Web3(HTTPProvider(node_uri)).eth.blockNumber
                data_pull_succeeded = True
            except Exception as e:
                logger.info(f'failed getting max block number from quicknode on try ${try_counter}:', e)
                try_counter += 1
        if try_counter > 10:
            raise AttributeError('failed to get max block number from quicknode after 10 tries')     
        
        end_block -= lag
        end_block = int(end_block)
        if end_block - start_block > max_blocks_per_job:
            end_block = start_block + max_blocks_per_job - 1
            
    #check if blockchain max block > start_block 
    if end_block < start_block:
        logger.info(f"Already caught up!")
        return {'ok': True}
    
    ######extract new polygon data############
    
    # insert new job that is running
    job_details = {
        "type": "getPolygonEtl",
        "start": start_block,
        "end": end_block
    }
    job_row = {
        'type': job_details['type'],
        'status': 'running',
        'details': json.dumps(job_details)
    }
    job_row = insertJob(pg_db.engine, job_row)
    log_details = {
        'type':'getPolygonEtl', 
        'id':job_row['row']['id'], 
        'start':start_block, 
        'end':end_block
    }
    logger.info(f'getting polygon data... ${job_row}', extra={"json_fields":log_details})
    
    try:
        all_items = extract_polygon_data(node_uri, start_block, end_block, lag)
    except Exception as e:
        #mark job as failed if failed
        log_details['error'] = e
        logger.info(f'failed getting polygon data... ${job_row}:', extra={"json_fields":log_details})
        updateJobRow = {
            'id': job_row['row']['id'],
            'status': 'failed',
            'details': json.dumps(job_details)
        }
        updateJob(pg_db.engine, updateJobRow)
        return {'ok':False}
    logger.info(f'completed getting polygon data... ${job_row}', extra={"json_fields":log_details})
    
    ######transform polygon data############
    logger.info(f'transforming new polygon data... ${job_row}', extra={"json_fields":log_details})
    try:
        all_transformed = transform_polygon_data(all_items)
    except Exception as e:
        #mark job as failed if failed
        log_details['error'] = e
        logger.info(f'failed transforming polygon data... ${job_row}:', extra={"json_fields":log_details})
        updateJobRow = {
            'id': job_row['row']['id'],
            'status': 'failed',
            'details': json.dumps(job_details)
        }
        updateJob(pg_db.engine, updateJobRow)
        return {'ok':False}
    logger.info(f'completed transforming polygon data... ${job_row}', extra={"json_fields":log_details})
    
    ######load polygon data############
    logger.info(f'load new polygon data... ${job_row}', extra={"json_fields":log_details})
    try:
        load_polygon_data(all_transformed, clickhouse_client, non_np_clickhouse_client, polygon_db)
    except Exception as e:
        #mark job as failed if failed
        log_details['error'] = e
        logger.info(f'failed transforming polygon data... ${job_row}:', extra={"json_fields":log_details})
        updateJobRow = {
            'id': job_row['row']['id'],
            'status': 'failed',
            'details': json.dumps(job_details)
        }
        updateJob(pg_db.engine, updateJobRow)
        return {'ok':False}
    
    # if made this far mark job complete, successs
    updateJobRow = {
        'id': job_row['row']['id'],
        'status': 'success',
        'details': json.dumps(job_details)
    }
    updateJob(pg_db.engine, updateJobRow)
    logger.info(f"job done. {job_row}", extra={"json_fields":log_details})
    return {'ok': True}
    