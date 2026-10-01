# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

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
# ('data/raw/fixed_income/desks/') since VOLUME_BASE_PATH already resolves
# to the volume's bound storage root - confirm the exact destination subpath
# once the volume's storage_location is verified.
dbutils.widgets.text('ADLS_RAW_DATA_PATH', 'fixed_income/desks/')
ADLS_RAW_DATA_PATH = dbutils.widgets.get('ADLS_RAW_DATA_PATH')
dbutils.widgets.text('TARGET_SCHEMA_NAME', 'bronze')
TARGET_SCHEMA_NAME = dbutils.widgets.get('TARGET_SCHEMA_NAME')
dbutils.widgets.text('CATALOG', '')

# COMMAND ----------

# CATALOG_NAME must be set (via os.environ) before %run "./volume_util.py" executes,
# since volume_util.py reads it at module-load time.
CATALOG_NAME = dbutils.widgets.get('CATALOG')
os.environ["CATALOG_NAME"] = CATALOG_NAME

# COMMAND ----------

# MAGIC %run "./volume_util.py"

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

raw_data_folder = f"{VOLUME_BASE_PATH}/{ADLS_RAW_DATA_PATH.rstrip('/')}"
file = get_latest_file_from_volume(raw_data_folder)
path = file.path
file_name = file.name

# COMMAND ----------

df = (
    spark.read.format("com.crealytics.spark.excel")
    .option("header", "True")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "false")
    .load(path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Convert to Dataframe and apply schema

# COMMAND ----------

desk_schema = StructType(
    [
        StructField("deskId", LongType(), True),
        StructField("deskName", StringType(), True),
        StructField("deskDescription", StringType(), True),
        StructField("businessId", LongType(), True),
        StructField("businessName", StringType(), True),
        StructField("businessDescription", StringType(), True)
    ]
)

expected_columns = list(map(lambda x: x.name, desk_schema))

for field in desk_schema:
    df = df.withColumn(field.name, df[field.name].cast(field.dataType))

df = df.select(expected_columns)
df = df.withColumn("file_name", lit(file_name))

# COMMAND ----------

# MAGIC %md
# MAGIC # Store in delta format

# COMMAND ----------

FI_DESK_TABLE = f"`{CATALOG_NAME}`.`{TARGET_SCHEMA_NAME}`.`fi_desk`"

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FI_DESK_TABLE)