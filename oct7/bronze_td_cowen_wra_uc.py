# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze - TD Cowen WRA (Unity Catalog)
# MAGIC Reads trade-date CSV files from the raw.data volume, merges into the managed bronze table, and marks the ingestion tracker.

# COMMAND ----------

# DBTITLE 1,Catalog context (must run BEFORE volume_util)
import os

CATALOG_NAME = os.getenv("CATALOG_NAME")

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# DBTITLE 1,Imports and logger
import logging
import sys
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import Window

logger = logging.getLogger("bronze_td_cowen_wra")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

# DBTITLE 1,Configuration
SHAREPOINT_TRACKER_TABLE = f"`{CATALOG_NAME}`.raw.sharepoint_ingestion_tracker"
WRA_TRADES_BASE_PATH = f"{VOLUME_BASE_PATH}/trades/td_cowen_wra"
BRONZE_WRA_TABLE = f"`{CATALOG_NAME}`.bronze.td_cowen_wra"

logger.info(f"Catalog         : {CATALOG_NAME}")
logger.info(f"Tracker table   : {SHAREPOINT_TRACKER_TABLE}")
logger.info(f"Raw source base : {WRA_TRADES_BASE_PATH}")
logger.info(f"Bronze table    : {BRONZE_WRA_TABLE}")

# COMMAND ----------

# DBTITLE 1,File-system helpers
# Match raw: permission/connectivity failures must fail the run, not look like missing files.
def _is_not_found(exc: Exception) -> bool:
    msg = str(exc)
    return (
        "FileNotFoundException" in msg
        or "does not exist" in msg.lower()
        or "NOT_FOUND" in msg
    )


def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as exc:
        if _is_not_found(exc):
            return False
        raise

# COMMAND ----------

# DBTITLE 1,Create bronze table (managed)
logger.info(f"Creating table {BRONZE_WRA_TABLE} if required...")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_WRA_TABLE} (
        TradeDate DATE NOT NULL,
        ClientIdty STRING NOT NULL,
        ClientName STRING,
        NetToFirm_USD DECIMAL(18, 2),
        -- audit columns
        file_name_path STRING,
        ingestion_timestamp TIMESTAMP
    ) USING DELTA
      TBLPROPERTIES (
          'delta.enableChangeDataFeed' = 'true',
          'delta.feature.allowColumnDefaults' = 'supported'
      )
"""
)

# COMMAND ----------

def to_decimal(col_name: str, precision: int = 18, scale: int = 2):
    # 1. Remove $ and commas, then trim spaces
    s = F.trim(F.regexp_replace(F.col(col_name).cast("string"), r"[$,]", ""))

    return (
        F.when(s == "", None)                                      # 2. Empty/whitespace -> null
        .when(s.rlike(r"^\(\d+(\.\d+)?\)$"),                       # 3a. (6.87) -> -6.87
              F.concat(F.lit("-"), F.regexp_replace(s, r"[()]", "")))
        .when(s.rlike(r"^-?\d+(\.\d+)?$"), s)                      # 3b. -6.87 or 6.87 -> as-is
        .otherwise(None)                                           # 4. Anything else -> null
        .cast(f"decimal({precision},{scale})")                      # 5. Convert to decimal
    )

# COMMAND ----------

with capture_errors("bronze_td_cowen_wra_read_tracker"):
    tracker_table = DeltaTable.forName(spark, SHAREPOINT_TRACKER_TABLE)

    tracker_rows = (
        spark.table(SHAREPOINT_TRACKER_TABLE)
        .filter((F.col("product") == F.lit("td_cowen_wra"))
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

DEDUP_KEY_COLUMNS = [
    "TradeDate",
    "ClientIdty",
]

def drop_duplicate_keys(df: DataFrame, keys: list[str] = DEDUP_KEY_COLUMNS) -> DataFrame:
    """Remove rows that are duplicates on the key columns, keeping the most recent
    by ingestion_timestamp. Logs a warning with the count and displays the dropped rows."""
    window = Window.partitionBy(*keys).orderBy(F.col("ingestion_timestamp").desc_nulls_last())

    ranked = df.withColumn("_row_num", F.row_number().over(window))

    duplicate_records = ranked.filter(F.col("_row_num") > 1).drop("_row_num")
    duplicate_count = duplicate_records.count()

    if duplicate_count > 0:
        logger.warning(
            f"Dropping {duplicate_count} duplicate record(s) on keys: {keys}"
        )
        duplicate_records.display()

    return ranked.filter(F.col("_row_num") == 1).drop("_row_num")

# COMMAND ----------

with capture_errors("bronze_td_cowen_wra_merge"):
    bronze_table = DeltaTable.forName(spark, BRONZE_WRA_TABLE)

    for row in tracker_rows:
        trade_date = row["trade_date"]
        upload_ts = row["upload_timestamp"]
        file_name = row["file_name"]

        trade_yyyy = f"{trade_date.year:04d}"
        trade_mm = f"{trade_date.month:02d}"
        trade_dd = f"{trade_date.day:02d}"

        raw_trades_file_path = f"{WRA_TRADES_BASE_PATH}/{trade_yyyy}/{trade_mm}/{trade_dd}/{file_name}"

        if not path_exists(raw_trades_file_path):
            logger.warning(f"File not found, skipping: {raw_trades_file_path}")
            continue

        # New Data to Merge (this file only)
        new_bronze_df = (
            spark.read.csv(raw_trades_file_path, header=True, inferSchema=True)
            .withColumn("NetToFirm_USD", to_decimal("NetToFirm_USD", 18, 2))
            .withColumn("file_name_path", F.col("_metadata.file_path"))
            .withColumn("ingestion_timestamp", F.current_timestamp())
        )

        if new_bronze_df.isEmpty():
            logger.warning(f"Empty file, Skipping: {raw_trades_file_path}")
            continue

        new_bronze_df = drop_duplicate_keys(new_bronze_df)

        # Merge to bronze.td_cowen_wra
        logger.info(f"Merging {raw_trades_file_path} to {BRONZE_WRA_TABLE}")
        (
            bronze_table.alias("bronze")
            .merge(
                new_bronze_df.alias("new_bronze"),
                "bronze.TradeDate <=> new_bronze.TradeDate AND bronze.ClientIdty <=> new_bronze.ClientIdty"
            )
            .whenMatchedUpdate(
                condition=(
                    (~F.col("bronze.ClientName").eqNullSafe(F.col("new_bronze.ClientName"))) |
                    (~F.col("bronze.NetToFirm_USD").eqNullSafe(F.col("new_bronze.NetToFirm_USD")))
                ),
                set={
                    "ClientName": F.col("new_bronze.ClientName"),
                    "NetToFirm_USD": F.col("new_bronze.NetToFirm_USD"),
                    "file_name_path": F.lit(raw_trades_file_path),
                    "ingestion_timestamp": F.current_timestamp()
                }
            )
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )

        # Update Tracker to is_done_bronze = True for this one record
        tracker_table.update(
            condition=(
                (F.col("file_name") == F.lit(file_name)) &
                (F.col("product") == F.lit("td_cowen_wra")) &
                (F.col("trade_date") == F.lit(trade_date).cast("date")) &
                (F.col("upload_timestamp") == F.lit(upload_ts).cast("timestamp")) &
                (F.col("is_done_raw")) &
                (~F.col("is_done_bronze"))
            ),
            set={"is_done_bronze": F.lit(True)}
        )

        logger.info(f"Merged {file_name} into {BRONZE_WRA_TABLE} and marked is_done_bronze TRUE")
