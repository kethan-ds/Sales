# Databricks notebook source
from datetime import datetime
import logging
import os
import json

from httpx import (
    Client,
    Limits,
    Timeout
)
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# COMMAND ----------

def partition(records, size = 1_000):
    for index in range(0, len(records), size):
        yield records[index:index + size]

# COMMAND ----------

dbutils.widgets.text('prism_web_server_base_url', '')
dbutils.widgets.text('prism_auth_server', '')
dbutils.widgets.text('prism_client_id', '')
dbutils.widgets.text('prism_client_secret', '')

# COMMAND ----------



# COMMAND ----------

# MAGIC %run "./network_utils.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_constants.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_server_methods.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_data_extractors.py"

# COMMAND ----------

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

logger.info('Synchronizing PRISM Delta with PRISM Actual. Any extra records locally will be deleted')

spark = (
    SparkSession.builder
        .appName('ClientMatchingHub') # type: ignore




        .getOrCreate()
)

logger.info('Fetching PRISM records...')

df_last_modified = spark.createDataFrame([(datetime.strptime('1970-01-01', '%Y-%m-%d').date(),)], 'last_modified: date')
prism_records = _get_prism_records(HTTPX_CLIENT, PRISM_WEB_SERVER_BASE_URL, df_last_modified)

logger.info('Converting PRISM records from Spark to Python')

prism_records = [to_prism_rec(r) for r in prism_records]

prism_dt = DeltaTable.forName(spark, PRISM_DELTA_LOCATION)

logger.info('Creating Spark DF for records')

existing_records = spark.createDataFrame(prism_records, PRISM_FIELDS)

logger.info('Deleting any extra rows in the Delta Table NOT in PRISM')
logger.info(f'Current size: {prism_dt.toDF().count()}, Number of records in PRISM: {existing_records.count()}')

prism_insert_setter = to_prism_sql(PRISM_FIELDS, 'prism_raw', [])

prism_dt.alias('stored').merge(
    existing_records.alias('prism_raw'),
    'stored.account_id = prism_raw.account_id'
).whenNotMatchedInsert(values=prism_insert_setter).whenNotMatchedBySourceDelete().execute()

logger.info(f'After insert/delete: {prism_dt.toDF().count()}')

logger.info('Done')

