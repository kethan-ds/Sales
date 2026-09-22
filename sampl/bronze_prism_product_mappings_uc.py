# Databricks notebook source
import logging
import pyspark.sql.functions as F
from delta import DeltaTable

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# MAGIC %run "./volume_util.py"

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    handlers=[
        logging.StreamHandler()  # Output logs to the console
    ]
)

logger = logging.getLogger(__name__)

# COMMAND ----------

SHAREPOINT_TRACKER_TABLE = f"`{CATALOG_NAME}`.`raw`.`sharepoint_ingestion_tracker`"
PRISM_PRODUCT_MAPPINGS_BASE_PATH = f"{VOLUME_BASE_PATH}/product_mappings/prism"
BRONZE_PRISM_PRODUCT_MAPPINGS = f"`{CATALOG_NAME}`.`bronze`.`prism_product_mappings`"

# COMMAND ----------

logger.info(f"Creating table {BRONZE_PRISM_PRODUCT_MAPPINGS} if required...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_PRISM_PRODUCT_MAPPINGS} (
        source_system STRING,
        product_level_0 STRING,
        product_level_1 STRING,
        product_level_2 STRING,
        product_level_3 STRING,
        in_out STRING,
        prism_product_level_0 STRING,
        prism_product_level_1 STRING,
        prism_product_level_2 STRING,
        prism_product_level_3 STRING,
        -- audit columns
        file_name_path STRING,
        ingestion_timestamp TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true'
    )
""")

# COMMAND ----------

tracker_table = DeltaTable.forName(spark, SHAREPOINT_TRACKER_TABLE)

tracker_rows = (
    spark.table(SHAREPOINT_TRACKER_TABLE)
    .filter((F.col("product") == F.lit("prism_product_mappings"))
            & (F.col("is_done_raw"))
            & (~F.col("is_done_bronze"))
    )
    .orderBy(
        F.col("file_name").asc(),
        F.col("trade_date").asc(),
        F.col("upload_timestamp").asc()
    )
    .select("file_name", "trade_date", "upload_timestamp")
    .collect()
)

if tracker_rows:
    logger.info(f"Found {len(tracker_rows)} record(s) to process")
    details = "\n".join(
        f"  file_name={r.file_name}, trade_date={r.trade_date}, upload_timestamp={r.upload_timestamp}"
        for r in tracker_rows
    )
    logger.info(f"Records:\n{details}")
else:
    logger.info("No files to process")

# COMMAND ----------

bronze_table = DeltaTable.forName(spark, BRONZE_PRISM_PRODUCT_MAPPINGS)

for row in tracker_rows:
    trade_date = row["trade_date"]
    upload_ts = row["upload_timestamp"]
    file_name = row["file_name"]

    trade_yyyy = f"{trade_date.year:04d}"
    trade_mm = f"{trade_date.month:02d}"
    trade_dd = f"{trade_date.day:02d}"

    raw_trades_file_path = f"{PRISM_PRODUCT_MAPPINGS_BASE_PATH}/{trade_yyyy}/{trade_mm}/{trade_dd}/{file_name}"

    try:
        dbutils.fs.ls(raw_trades_file_path)
        exists = True
    except Exception:
        exists = False

    if not exists:
        logger.warning(f"File not found, skipping: {raw_trades_file_path}")
        continue

    # New Data to Merge (this file only)
    new_bronze_df = (
        spark.read.csv(raw_trades_file_path, header=True, inferSchema=False)
        .withColumn("file_name_path", F.input_file_name())
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    if new_bronze_df.isEmpty():
        logger.warning(f"Empty file, Skipping: {raw_trades_file_path}")
        continue

    # Overwrite Bronze with new File
    (
        new_bronze_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_PRISM_PRODUCT_MAPPINGS)
    )

    # Update Tracker to is_done_bronze = True for this one record
    tracker_table.update(
        condition=(
            (F.col("file_name") == F.lit(file_name)) &
            (F.col("product") == F.lit("prism_product_mappings")) &
            (F.col("trade_date") == F.lit(trade_date).cast("date")) &
            (F.col("upload_timestamp") == F.lit(upload_ts).cast("timestamp")) &
            (F.col("is_done_raw")) &
            (~F.col("is_done_bronze"))
        ),
        set={"is_done_bronze": F.lit(True)}
    )

    logger.info(f"Merged {file_name} into {BRONZE_PRISM_PRODUCT_MAPPINGS} and marked is_done_bronze TRUE")
