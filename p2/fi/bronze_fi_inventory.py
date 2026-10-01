# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

import pandas as pd
from datetime import datetime

from pyspark.sql.types import FloatType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import LongType
from pyspark.sql.functions import lit

import os


# COMMAND ----------

# MAGIC %md
# MAGIC # Setup connection settings

# COMMAND ----------

# NOTE: prefix stripped relative to the legacy ADLS-container-relative path
# ('data/raw/fixed_income/inventories/') since VOLUME_BASE_PATH already resolves
# to the volume's bound storage root - confirm the exact destination subpath
# once the volume's storage_location is verified.
dbutils.widgets.text('ADLS_DESTINATION_PATH', 'fixed_income/inventories/')
ADLS_DESTINATION_PATH = dbutils.widgets.get('ADLS_DESTINATION_PATH')
dbutils.widgets.text('TARGET_SCHEMA_NAME', 'bronze')
TARGET_SCHEMA_NAME = dbutils.widgets.get('TARGET_SCHEMA_NAME')
dbutils.widgets.text('CATALOG', '')

# CATALOG_NAME must be set (via os.environ) before %run "./volume_util.py" executes,
# since volume_util.py reads it at module-load time.
CATALOG_NAME = dbutils.widgets.get('CATALOG')
os.environ["CATALOG_NAME"] = CATALOG_NAME

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load the latest file

# COMMAND ----------

# UC does not grant direct storage access - list/read the file from the governed
# Volume instead of via the ADLS SDK's get_latest_file_from_dir.
def get_latest_file_from_volume(dir_path):
    files = [f for f in dbutils.fs.ls(dir_path) if not f.isDir()]
    if not files:
        raise FileNotFoundError(f"No files found in {dir_path}")
    return max(files, key=lambda f: f.modificationTime)

raw_data_folder = f"{VOLUME_BASE_PATH}/{ADLS_DESTINATION_PATH.rstrip('/')}"
file = get_latest_file_from_volume(raw_data_folder)

path = file.path
file_name = file.name

# COMMAND ----------

# MAGIC %md
# MAGIC # MAGIC # Convert to Dataframe and apply schema

# COMMAND ----------

df = (
    spark.read.format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .load(path)
)

inventories_schema = StructType(
    [
        StructField("deskId", LongType(), True),
        StructField("inventoryId", LongType(), True),
        StructField("inventoryName", StringType(), True),
        StructField("inventoryDescription", StringType(), True),
        StructField("inventoryTypeId", LongType(), True),
        StructField("inventoryTypeName", StringType(), True),
        StructField("inventoryTypeDescription", StringType(), True),
        StructField("tradeSystemId", LongType(), True),
        StructField("tradeSystemName", StringType(), True),
        StructField("tradeSystemDescription", StringType(), True)
    ]
)
expected_columns = list(map(lambda x: x.name, inventories_schema))

for field in inventories_schema:
    df = df.withColumn(field.name, df[field.name].cast(field.dataType))

df = df.select(expected_columns)
df = df.withColumn("file_name", lit(file_name))

# COMMAND ----------

# MAGIC %md
# MAGIC # Store in delta format

# COMMAND ----------

FI_INVENTORY_TABLE = f"`{CATALOG_NAME}`.`{TARGET_SCHEMA_NAME}`.`fi_inventory`"

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FI_INVENTORY_TABLE)