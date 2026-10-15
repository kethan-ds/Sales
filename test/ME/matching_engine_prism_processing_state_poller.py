# Databricks notebook source
dbutils.widgets.text('client_matching_web_server_base_url', '')
dbutils.widgets.text('prism_processing_state_location', '')
dbutils.widgets.text('client_matched_codes_delta_location', '')
dbutils.widgets.text('cip_client_id', '')
dbutils.widgets.text('cip_client_secret', '')
dbutils.widgets.text('auth_server', '')
dbutils.widgets.text('rivendell_auth_scopes', '')


# COMMAND ----------



# COMMAND ----------

# MAGIC %run "./network_utils.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_constants.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_server_methods.py"

# COMMAND ----------

from datetime import datetime
import logging
import os

from httpx import (
    Client,
    Limits,
    Timeout
)
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)

class CustomFormatter(logging.Formatter):
    def format(self, record):
        now = datetime.now()
        timestamp = now.strftime('%Y-%m-%d %H:%M:%S') + f":{now.microsecond // 1000:03d}"
        method_name = record.funcName
        log_level = record.levelname
        message = record.getMessage()

        return f"{timestamp} {log_level} --- [{method_name}]: {message}"

def get_logger(name=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(CustomFormatter())

    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger

logger = get_logger(__name__)

HTTPX_CLIENT = Client(
    http2=False,
    verify=False,
    timeout=Timeout(
        connect=int(os.getenv("MATCHING_ENGINE_CONNECTION_TIMEOUT", "60")),
        read=int(os.getenv("MATCHING_ENGINE_READ_TIMEOUT", "360")),
        write=int(os.getenv("MATCHING_ENGINE_WRITE_TIMEOUT", "360")),
        pool=int(os.getenv("MATCHING_ENGINE_POOL_LIMIT", "60"))
    ),
    limits=Limits(
        max_connections=int(os.getenv("MATCHING_ENGINE_POOL_LIMIT", "40")),
        max_keepalive_connections=int(os.getenv("MATCHING_ENGINE_KEEPALIVE_CONNECTIONS", "20")),
        keepalive_expiry=int(os.getenv("MATCHING_ENGINE_KEEPALIVE_EXPIRY", "60"))
    )
)

def to_server_state(record):
    return {
        'status': record.status,
        'statusEnum': record.status_enum,
        'message': record.message,
        'runType': record.run_type,
        'cronSchedule': record.cron_schedule,
        'processedDatetime': record['processed_datetime'].strftime('%Y-%m-%dT%H:%M:%SZ'),
        'acknowledgeDatetime': record['acknowledge_datetime'].strftime('%Y-%m-%dT%H:%M:%SZ')
    }

def to_local_obj(record):
    return {
        'id': int(record['id']),
        'processed_datetime': datetime.strptime(record['processedDatetime'], "%Y-%m-%dT%H:%M:%SZ"),
        'status': int(record['status']),
        'status_enum': record['statusEnum'],
        'message': record['message'],
        'run_type': record['runType'],
        'cron_schedule': record['cronSchedule'],
        'acknowledge_datetime': datetime.strptime(record['acknowledgeDatetime'], "%Y-%m-%dT%H:%M:%SZ")
    }

logger.info('Checking prism_processing_state for updates...')

prism_processing_state_df = spark.read.format('delta').table(PRISM_PROCESSING_STATE_LOCATION)
prism_processing_state_df = (
    prism_processing_state_df
        .filter('acknowledge_datetime is null')
        .orderBy('processed_datetime', ascending=False)
)

if prism_processing_state_df.isEmpty():
    logger.info('No new Prism processing times found')
else:
    logger.info('Saving and sending to server')

    recs = get_server_record(HTTPX_CLIENT,
                             f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/prism-processing-state?sort=processedDatetime,desc&size=100',
                             'prismProcessingStates', to_local_obj)

    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("processed_datetime", TimestampType(), nullable=True),
        StructField("status", IntegerType(), nullable=False),
        StructField("status_enum", StringType(), nullable=True),
        StructField("message", StringType(), nullable=True),
        StructField("run_type", StringType(), nullable=True),
        StructField("cron_schedule", StringType(), nullable=True),
        StructField("acknowledge_datetime", TimestampType(), nullable=True),
    ])

    server_df = spark.createDataFrame(recs, schema)
    joined_df = (
        prism_processing_state_df.alias('prism_process')
            .join(server_df.alias('server'), on="processed_datetime",
                  how="leftanti")
        .withColumn('acknowledge_datetime', current_timestamp())
        .withColumn('message', F.coalesce(F.col('message'), F.lit("")))
        .withColumn('message', F.when(F.col('message') == 'null',
                                     F.lit('')).otherwise(F.col('message')))
    )

    logger.info('Joining server and prism processing records')
    records = joined_df.collect()

    records = [to_server_state(rec) for rec in records]

    logger.info(f'Publishing to server: {len(records)}')

    for rec in records:
        _perform_operation(HTTPX_CLIENT,
                           f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/prism-processing-state',
                           rec, 'POST')

    prism_p_dt = DeltaTable.forName(spark, PRISM_PROCESSING_STATE_LOCATION)

    logger.info(f'Saving to delta table')

    prism_p_dt.alias('dt').merge(joined_df.alias('jdf'), 'dt.processed_datetime = jdf.processed_datetime')\
                                      .whenMatchedUpdate(set={
                                          'acknowledge_datetime': 'jdf.acknowledge_datetime'
                                          }).execute()
    logger.info('Done...')

