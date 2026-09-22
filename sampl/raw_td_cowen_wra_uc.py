# Databricks notebook source
import re
import os
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging
import sys

# COMMAND ----------

# Unity Catalog parameter passed by controller_td_cowen_wra_uc.py
dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG").strip()

if not CATALOG_NAME:
    raise ValueError("CATALOG parameter is required")

# volume_util.py in this project resolves its catalog from CATALOG_NAME.
os.environ["CATALOG_NAME"] = CATALOG_NAME
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

SHAREPOINT_TRACKER_TABLE = (
    f"`{CATALOG_NAME}`.`raw`.`sharepoint_ingestion_tracker`"
)

# Raw files are stored in the UC Raw Volume.
# VOLUME_BASE_PATH is supplied by volume_util.py.
SHAREPOINT_TD_COWEN_WRA_BASE_PATH = (
    f"{VOLUME_BASE_PATH}/sharepoint_ingestion/td_cowen_wra"
)
WRA_TRADES_BASE_PATH = (
    f"{VOLUME_BASE_PATH}/trades/td_cowen_wra"
)

# COMMAND ----------

def _is_dir(entry):
    # dbutils file entries: directories report a name ending with "/"
    return entry.name.endswith("/")


def remove_empty_dirs(path):
    """
    Recursively removes empty directories under `path` (and `path` itself
    if it becomes empty). A directory is removed only when it contains no
    files at any depth below it. Returns True if `path` ended up empty and
    was removed, False otherwise.
    """
    try:
        entries = dbutils.fs.ls(path)
    except Exception:
        # Path no longer exists (already removed) -> treat as empty/gone.
        return True

    is_empty = True
    for entry in entries:
        if _is_dir(entry):
            child_removed = remove_empty_dirs(entry.path)
            if not child_removed:
                is_empty = False
        else:
            # A file exists here -> this branch is not empty.
            is_empty = False

    if is_empty:
        # recurse=False refuses to delete a non-empty dir (safety net).
        dbutils.fs.rm(path, recurse=False)
        logger.info(f"Deleted Empty Directory: {path}")
        return True
    return False


def cleanup_empty_dirs_under(base_path):
    """
    Removes empty subdirectories under `base_path` WITHOUT ever deleting
    `base_path` itself. Only recurses into its children.
    """
    for entry in dbutils.fs.ls(base_path):
        logger.info(f"Scanning for Empty Sub Directories under {base_path} to Delete")
        if _is_dir(entry):
            remove_empty_dirs(entry.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Tracker table, move all files that has is_done_raw = False
# MAGIC ### from UC Raw Volume: sharepoint_ingestion/td_cowen_wra
# MAGIC ### to UC Raw Volume: trades/td_cowen_wra/YYYY/MM/DD (Trade Date)

# COMMAND ----------

tracker_table = DeltaTable.forName(spark, SHAREPOINT_TRACKER_TABLE)

# Keep upload_timestamp so you can update exactly one record
tracker_rows = (
    spark.table(SHAREPOINT_TRACKER_TABLE)
    .filter((F.col("product") == F.lit("td_cowen_wra")) & (~F.col("is_done_raw")))
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

for row in tracker_rows:
    trade_date = row["trade_date"]
    upload_ts = row["upload_timestamp"]
    file_name = row["file_name"]

    upload_yyyy = f"{upload_ts.year:04d}"
    upload_mm = f"{upload_ts.month:02d}"
    upload_dd = f"{upload_ts.day:02d}"

    trade_yyyy = f"{trade_date.year:04d}"
    trade_mm = f"{trade_date.month:02d}"
    trade_dd = f"{trade_date.day:02d}"

    sharepoint_file_path = f"{SHAREPOINT_TD_COWEN_WRA_BASE_PATH}/{upload_yyyy}/{upload_mm}/{upload_dd}/{file_name}"
    raw_trades_file_path = f"{WRA_TRADES_BASE_PATH}/{trade_yyyy}/{trade_mm}/{trade_dd}/{file_name}"

    try:
        dbutils.fs.ls(sharepoint_file_path)
        exists = True
    except Exception:
        exists = False

    if not exists:
        logger.warning(f"File not found, skipping: {sharepoint_file_path}")
        continue

    logger.info(f"Moving File {file_name} from {sharepoint_file_path} to {raw_trades_file_path}")
    dbutils.fs.mv(sharepoint_file_path, raw_trades_file_path)
    logger.info(f"Successfully moved {file_name}")

    # Clean up empty upload folders after this move (base folder is preserved)
    cleanup_empty_dirs_under(SHAREPOINT_TD_COWEN_WRA_BASE_PATH)

    tracker_table.update(
        condition=(
            (F.col("file_name") == F.lit(file_name)) &
            (F.col("product") == F.lit("td_cowen_wra")) &
            (F.col("trade_date") == F.lit(trade_date).cast("date")) &
            (F.col("upload_timestamp") == F.lit(upload_ts).cast("timestamp")) &
            (F.col("is_done_raw") == F.lit(False))
        ),
        set={"is_done_raw": F.lit(True)}
    )

    logger.info(f"Moved {file_name} -> {raw_trades_file_path}, cleaned up empty upload dirs, and marked Tracker is_done_raw as TRUE")
