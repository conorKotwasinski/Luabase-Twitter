from luapy.el.polygon_etl import (
    extract_polygon_data,
    transform_polygon_data,
    load_polygon_data,
)
from luapy.utils.pg_db_utils import updateJobStatus
from luapy.logger import logger


def get_node_backlog_polygon(
    node_uri,
    clickhouse_client,
    non_np_clickhouse_client,
    pg_db,
    job_id,
    job_type,
    start_block,
    end_block,
):

    log_details = {
        "type": job_type,
        "start": start_block,
        "end": end_block,
        "id": job_id,
    }
    ######extract new polygon data############

    try:
        logger.info(
            f"getting backlog polygon data for ${log_details}",
            extra={"json_fields": log_details},
        )
        all_items = extract_polygon_data(node_uri, start_block, end_block, lag=0, job_id=job_id)
    except Exception as e:
        # mark job as failed if failed
        log_details["error"] = e
        print(log_details)
        logger.info(
            f"failed getting backlog polygon data for ${log_details}",
            extra={"json_fields": log_details},
        )
        updateJobRow = {
            "id": job_id,
            "status": "failed",
        }
        updateJobStatus(pg_db.engine, updateJobRow)
        return {"ok": False}
    logger.info(
        f"completed getting backlog polygon data for ${log_details}",
        extra={"json_fields": log_details},
    )

    ######transform polygon data############
    logger.info(
        f"transforming backlog polygon data for ${log_details}",
        extra={"json_fields": log_details},
    )
    try:
        all_transformed = transform_polygon_data(all_items)
    except Exception as e:
        # mark job as failed if failed
        log_details["error"] = e
        logger.info(
            f"failed transforming backlog polygon data for ${log_details}",
            extra={"json_fields": log_details},
        )
        updateJobRow = {
            "id": job_id,
            "status": "failed",
        }
        updateJobStatus(pg_db.engine, updateJobRow)
        return {"ok": False}
    logger.info(
        f"completed transforming backlog polygon data for ${log_details}",
        extra={"json_fields": log_details},
    )

    ######load polygon data############

    # check if mainnet or testnet db
    if job_type == "polygonBacklog":
        polygon_db = "polygon"
    elif job_type == "polygonTestnetBacklog":
        polygon_db = "polygon_testnet"

    logger.info(
        f"load backlog polygon data for ${log_details}",
        extra={"json_fields": log_details},
    )
    try:
        load_polygon_data(
            all_transformed, clickhouse_client, non_np_clickhouse_client, polygon_db
        )
    except Exception as e:
        # mark job as failed if failed
        log_details["error"] = e
        logger.info(
            f"failed loading backlog polygon data for ${log_details}",
            extra={"json_fields": log_details},
        )
        updateJobRow = {
            "id": job_id,
            "status": "failed",
        }
        updateJobStatus(pg_db.engine, updateJobRow)
        return {"ok": False}

    # if made this far mark job complete, successs
    updateJobRow = {
        "id": job_id,
        "status": "success",
    }
    updateJobStatus(pg_db.engine, updateJobRow)
    logger.info(
        f"job done for ${log_details}",
        extra={"json_fields": log_details},
    )
    return {"ok": True}
