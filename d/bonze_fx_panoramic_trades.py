# Databricks notebook source
import os

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
# volume_util.py reads CATALOG_NAME via os.getenv, so the widget value must
# be pushed into the environment BEFORE it runs.
os.environ["CATALOG_NAME"] = dbutils.widgets.get("CATALOG")

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# DBTITLE 1,Imports
import pandas as pd
import logging

from datetime import datetime, date
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import LongType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import lit, col, current_timestamp, regexp_replace, input_file_name

# COMMAND ----------

# DBTITLE 1,Get today's date and make folder hierarchy from it
# Get today's date
today = datetime.today()

# COMMAND ----------

# Format the date into the desired folder structure (year/month/day)
formatted_date = today.strftime('%Y/%m/%d')  # 2025/06/26

# COMMAND ----------

# DBTITLE 1,Set up Variables
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

TARGET_SCHEMA_NAME = 'bronze'
BRONZE_FX_PANORAMIC_TABLE = f"`{CATALOG_NAME}`.`{TARGET_SCHEMA_NAME}`.`fx_panoramic`"

# COMMAND ----------

# DBTITLE 1,Get list of all files from Destination folder (Volume path)
raw_data_folder = f"{VOLUME_BASE_PATH}/trades/fx_panoramic/{formatted_date}/"

# COMMAND ----------

# DBTITLE 1,Define JSON Schema
# Define the schema for the raw data
bronzeFxPanoramicSchema = StructType([
    StructField("TradeDate", DateType(), True),
    StructField("OriginalContractId", LongType(), True),
    StructField("OriginalTradeId", LongType(), True),
    StructField("TradeId", LongType(), True),
    StructField("CustomerLongName", StringType(), True),
    StructField("MurexMnemonic", StringType(), True),
    StructField("SalesDeskType", StringType(), True),
    StructField("SalesRegion", StringType(), True),
    StructField("ProductType", StringType(), True),
    StructField("CurrencyPair", StringType(), True),
    StructField("CurrencyGroup", StringType(), True),
    StructField("Side", StringType(), True),
    StructField("VolumeUsd", DecimalType(18,6), True),
    StructField("ClientValueUsd", DecimalType(18,6), True),
    StructField("MurexMnemonicName", StringType(), True),
    StructField("UsdcadMid", DecimalType(18,6), True),
    StructField("ClientValueCad", DecimalType(18,6), True),
    StructField("VolumeCad", DecimalType(18,6), True),
    StructField("IsClientFacing", BooleanType(), True),
    StructField("LastModified", TimestampType(), True),
    StructField("ExtractDate", DateType(), True)
])

# COMMAND ----------

# DBTITLE 1,Define Bronze Table (managed)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_FX_PANORAMIC_TABLE} (
        TradeDate DATE NOT NULL,
        OriginalContractId LONG NOT NULL,
        OriginalTradeId LONG NOT NULL,
        TradeId LONG,
        CustomerLongName STRING NOT NULL,
        MurexMnemonic STRING NOT NULL,
        SalesDeskType STRING,
        SalesRegion STRING,
        ProductType STRING NOT NULL,
        CurrencyPair STRING NOT NULL,
        CurrencyGroup STRING NOT NULL,
        Side STRING NOT NULL,
        VolumeUsd DECIMAL (18,6) NOT NULL,
        ClientValueUsd DECIMAL (18,6) NOT NULL,
        MurexMnemonicName STRING NOT NULL,
        UsdcadMid DECIMAL (18,6) NOT NULL,
        ClientValueCad DECIMAL (18,6) NOT NULL,
        VolumeCad DECIMAL (18,6) NOT NULL,
        IsClientFacing BOOLEAN,
        LastModified TIMESTAMP,
        ExtractDate DATE,
        file_name_path STRING,
        ingestion_timestamp TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,Define logger
# Configure the logger
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    handlers=[
        logging.StreamHandler()  # Output logs to the console
    ]
)

# COMMAND ----------

# Create a logger instance
logger = logging.getLogger(__name__)

# COMMAND ----------

# DBTITLE 1,Read all files and add them to Delta Table
with capture_errors("bronze_fx_panoramic_ingestion"):
    # Read all JSON files inside the folder (Volume/FUSE path)
    df = spark.read.schema(bronzeFxPanoramicSchema).json(f"{raw_data_folder}*.json.gz")
    logger.info("Successfully read JSON files from folder.")

    # Add the file name as a column, converted to the external (ADLS)
    # lineage path -- matches the convention used for file_name_path
    # elsewhere (Volume path used for reading, external path for lineage).
    df = df.withColumn(
        "file_name_path",
        regexp_replace(
            input_file_name(),
            f"dbfs:{VOLUME_BASE_PATH}",
            EXTERNAL_LOCATION_BASE_PATH,
        ),
    )
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # Check if there is any data to write
    if df is not None and not df.rdd.isEmpty():
        record_count = df.count()
        logger.info(f"Processing {record_count} records.")

        # Write the combined DataFrame to the Delta table (managed)
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(BRONZE_FX_PANORAMIC_TABLE)

        logger.info("Successfully processed and saved all files.")
    else:
        logger.warning("No valid data found to write.")