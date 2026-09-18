# Databricks notebook source
import os

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import lit

# COMMAND ----------

# Keep the legacy widget name so existing ADF parameters do not need to change.
dbutils.widgets.text('ADLS_DESTINATION_PATH', 'data/raw/fixed_income/inventories/')
ADLS_DESTINATION_PATH = dbutils.widgets.get('ADLS_DESTINATION_PATH')
dbutils.widgets.text('TARGET_SCHEMA_NAME', 'bronze')
TARGET_SCHEMA_NAME = dbutils.widgets.get('TARGET_SCHEMA_NAME')

# COMMAND ----------

def to_volume_relative_path(path: str) -> str:
    value = path.strip().lstrip('/')
    for prefix in ('data/raw/', 'raw/'):
        if value.startswith(prefix):
            return value[len(prefix):].rstrip('/')
    return value.rstrip('/')

def get_latest_file_from_dir(dir_path: str):
    files = [f for f in dbutils.fs.ls(dir_path) if not f.isDir()]
    if not files:
        raise FileNotFoundError(f"No files found in {dir_path}")
    return max(files, key=lambda f: f.modificationTime)

# COMMAND ----------

raw_data_folder = f"{VOLUME_BASE_PATH}/{to_volume_relative_path(ADLS_DESTINATION_PATH)}"
file = get_latest_file_from_dir(raw_data_folder)
path = file.path
file_name = file.name

# COMMAND ----------

df = (
    spark.read.format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "false")
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
        StructField("tradeSystemDescription", StringType(), True),
    ]
)

expected_columns = [field.name for field in inventories_schema]
for field in inventories_schema:
    df = df.withColumn(field.name, df[field.name].cast(field.dataType))

df = df.select(expected_columns).withColumn("file_name", lit(file_name))

# COMMAND ----------

target_table = f"`{CATALOG_NAME}`.`{TARGET_SCHEMA_NAME}`.`fi_inventory`"
(
    df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)
