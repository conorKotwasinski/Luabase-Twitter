from clickhouse_driver import Client
from google.cloud import bigquery
import json
import os
import pandas as pd
from logger import logger
import sqlalchemy
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask import Flask
import flask
import utils.pg_db_utils as pgu

txn_query_sql = """
    select 
        `hash`,
        size,
        virtual_size,
        version,
        lock_time,
        block_number,
        block_hash,
        block_timestamp,
        case when is_coinbase = True then 1 else 0 end as is_coinbase,
        null as transaction_index,
        input_count,
        output_count,
        cast(input_value as bigint) as input_value,
        cast(output_value as bigint) as output_value,
        cast(fee as bigint) as fee,
        inputs,
        outputs
    from `bigquery-public-data.crypto_bitcoin.transactions` 
    where block_timestamp_month = '{month}'
    order by block_number desc
    ;
    """

txn_insert_sql = """
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
    """

inputs_insert_sql = """
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
    """

outputs_insert_sql = """
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
    """


def get_btc_txn_backlog(data):
    # data is a dictionary that accepts the following arguments below:
    month = data["month"]
    bg_client = data["bg_client"]
    clickhouse_client = data["clickhouse_client"]
    pg_db = data["pg_db"]
    id = data["id"]
    increment = data["increment"]
    try:
        query = txn_query_sql.format(month=month)
        query_job = bg_client.query(query)
        query_result = query_job.result()
        log_details = {"type": "backlogBtcTxns", "month": month}

        logger.info(
            f"starting backlog for {month} with total row count of {query_result.total_rows}",
            extra={"json_fields": log_details},
        )

        row_ct = 1
        transactions_ls = []
        inputs_ls = []
        outputs_ls = []

        for row in query_result:
            row = dict(row)
            transactions_ls.append(row)

            inputs = row["inputs"]
            outputs = row["outputs"]

            if len(inputs) == 0:
                pass
            else:
                for input in inputs:
                    input["transaction_hash"] = row["hash"]
                    input["block_number"] = row["block_number"]
                    input["block_hash"] = row["block_hash"]
                    input["block_timestamp"] = row["block_timestamp"]
                    if input["type"] != "multisig":
                        input["required_signatures"] = 1
                    inputs_ls.append(input)

            if len(outputs) == 0:
                pass
            else:
                for output in outputs:
                    output["transaction_hash"] = row["hash"]
                    output["block_number"] = row["block_number"]
                    output["block_hash"] = row["block_hash"]
                    output["block_timestamp"] = row["block_timestamp"]
                    if output["type"] != "multisig":
                        output["required_signatures"] = 1
                    outputs_ls.append(output)

            if row_ct % increment == 0 or row_ct == query_result.total_rows:
                transactions_cols = [
                    key for key in row.keys() if key not in ["inputs", "outputs"]
                ]
                transactions_df = pd.DataFrame.from_records(
                    transactions_ls, columns=transactions_cols
                )
                inputs_df = pd.DataFrame(inputs_ls)
                outputs_df = pd.DataFrame(outputs_ls)

                transactions_df = transactions_df.rename(
                    columns={"index": "transaction_index"}
                )
                inputs_df = inputs_df.rename(columns={"index": "input_index"})
                outputs_df = outputs_df.rename(columns={"index": "output_index"})

                clickhouse_client.insert_dataframe(txn_insert_sql, transactions_df)
                clickhouse_client.insert_dataframe(inputs_insert_sql, inputs_df)
                clickhouse_client.insert_dataframe(outputs_insert_sql, outputs_df)
                log_details["row_ct"] = row_ct
                logger.info(
                    f"loaded data up to {row_ct} in query result for {month}",
                    extra={"json_fields": log_details},
                )

                transactions_ls = []
                inputs_ls = []
                outputs_ls = []
            row_ct += 1
        # mark job complete, successs
        updateJobRow = {
            "id": id,
            "status": "success",
        }
        pgu.updateJobStatus(pg_db.engine, updateJobRow)
        log_details["row_ct"] = row_ct
        logger.info(f"job done. {updateJobRow}", extra={"json_fields": log_details})
        return {"ok": True}
    except Exception as e:
        # if job fails mark as failed
        log_details["error"] = e
        logger.info(
            f"failed getting backlog data at {month}, row {row_ct}:",
            extra={"json_fields": log_details},
        )
        updateJobRow = {
            "id": id,
            "status": "failed",
        }
        pgu.updateJobStatus(pg_db.engine, updateJobRow)
        return {"ok": False}
