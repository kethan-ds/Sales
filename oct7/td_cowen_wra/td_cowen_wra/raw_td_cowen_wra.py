# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw - TD Cowen WRA (Unity Catalog)
# MAGIC Moves SharePoint-landed TD Cowen WRA trade files from the upload-date folder structure into the trade-date folder structure under the `raw.data` Volume, then marks the tracker record `is_done_raw = TRUE`.

# COMMAND ----------

# DBTITLE 1,Catalog context (must run BEFORE volume_util)
import os

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG").strip() or os.getenv("CATALOG_NAME", "")

if not CATALOG_NAME:
    raise ValueError("CATALOG widget (or CATALOG_NAME env var) must be set.")

# volume_util.py reads CATALOG_NAME from the environment at %run time
os.environ["CATALOG_NAME"] = CATALOG_NAME
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# DBTITLE 1,Imports and logger
import logging
import sys

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

logger = logging.getLogger("raw_td_cowen_wra")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

# DBTITLE 1,Configuration
PRODUCT = "td_cowen_wra"

SHAREPOINT_TRACKER_TABLE = f"`{CATALOG_NAME}`.raw.sharepoint_ingestion_tracker"

# VOLUME_BASE_PATH (/Volumes/<catalog>/raw/data) maps to <adls_base>/raw,
# so the legacy "raw/" prefix is dropped from the relative paths below.
SHAREPOINT_TD_COWEN_WRA_BASE_PATH = f"{VOLUME_BASE_PATH}/sharepoint_ingestion/td_cowen_wra"
WRA_TRADES_BASE_PATH = f"{VOLUME_BASE_PATH}/trades/td_cowen_wra"

logger.info(f"Catalog          : {CATALOG_NAME}")
logger.info(f"Tracker table    : {SHAREPOINT_TRACKER_TABLE}")
logger.info(f"SharePoint base  : {SHAREPOINT_TD_COWEN_WRA_BASE_PATH}")
logger.info(f"Raw target base  : {WRA_TRADES_BASE_PATH}")

# COMMAND ----------

# DBTITLE 1,File-system helpers
def _is_not_found(exc: Exception) -> bool:
    msg = str(exc)
    return (
        "FileNotFoundException" in msg
        or "does not exist" in msg.lower()
        or "NOT_FOUND" in msg
    )

def path_exists(path: str) -> bool:
    """True if the path exists. Only 'not found' errors return False; any other
    error (e.g. missing READ/WRITE VOLUME grant) is raised instead of being
    silently treated as a missing file."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as exc:
        if _is_not_found(exc):
            return False
        raise


def _is_dir(entry) -> bool:
    return entry.isDir()


def remove_empty_dirs(path: str) -> bool:
    """
    Recursively removes empty directories under `path` (and `path` itself
    if it becomes empty). A directory is removed only when it contains no
    files at any depth below it. Returns True if `path` ended up empty and
    was removed (or no longer exists), False otherwise.
    """
    try:
        entries = dbutils.fs.ls(path)
    except Exception as exc:
        if _is_not_found(exc):
            return True
        raise

    is_empty = True
    for entry in entries:
        if _is_dir(entry):
            if not remove_empty_dirs(entry.path):
                is_empty = False
        else:
            is_empty = False

    if is_empty:
        # recurse=False refuses to delete a non-empty dir (safety net)
        dbutils.fs.rm(path, recurse=False)
        logger.info(f"Deleted empty directory: {path}")
        return True
    return False


def cleanup_empty_dirs_under(base_path: str) -> None:
    """Removes empty subdirectories under `base_path` without deleting `base_path` itself."""
    logger.info(f"Scanning for empty sub-directories under {base_path}")
    for entry in dbutils.fs.ls(base_path):
        if _is_dir(entry):
            remove_empty_dirs(entry.path)


def _parent_dir(path: str) -> str:
    return path.rsplit("/", 1)[0]


# COMMAND ----------

# DBTITLE 1,Read pending tracker records
with capture_errors("raw_td_cowen_wra_read_tracker"):
    tracker_table = DeltaTable.forName(spark, SHAREPOINT_TRACKER_TABLE)

    tracker_rows = (
        spark.table(SHAREPOINT_TRACKER_TABLE)
        .filter((F.col("product") == F.lit(PRODUCT)) & (~F.col("is_done_raw")))
        .orderBy(
            F.col("file_name").asc(),
            F.col("trade_date").asc(),
            F.col("upload_timestamp").asc(),
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

# DBTITLE 1,Move files and update tracker
def mark_done_raw(file_name, trade_date, upload_ts) -> None:
    tracker_table.update(
        condition=(
            (F.col("file_name") == F.lit(file_name))
            & (F.col("product") == F.lit(PRODUCT))
            & (F.col("trade_date") == F.lit(trade_date).cast("date"))
            & (F.col("upload_timestamp") == F.lit(upload_ts).cast("timestamp"))
            & (F.col("is_done_raw") == F.lit(False))
        ),
        set={"is_done_raw": F.lit(True)},
    )


moved, recovered, skipped = 0, 0, 0

with capture_errors("raw_td_cowen_wra_move"):
    for row in tracker_rows:
        trade_date = row["trade_date"]
        upload_ts = row["upload_timestamp"]
        file_name = row["file_name"]

        sharepoint_file_path = (
            f"{SHAREPOINT_TD_COWEN_WRA_BASE_PATH}/"
            f"{upload_ts.year:04d}/{upload_ts.month:02d}/{upload_ts.day:02d}/{file_name}"
        )
        raw_file_path = (
            f"{WRA_TRADES_BASE_PATH}/"
            f"{trade_date.year:04d}/{trade_date.month:02d}/{trade_date.day:02d}/{file_name}"
        )

        source_exists = path_exists(sharepoint_file_path)
        target_exists = path_exists(raw_file_path)

        if not source_exists and not target_exists:
            logger.warning(f"File not found in source or target, skipping: {sharepoint_file_path}")
            skipped += 1
            continue

        if not source_exists and target_exists:
            # Recovery: a previous run moved the file but failed before the
            # tracker update. Mark the record done instead of leaving it stuck.
            logger.warning(
                f"Source missing but target already present, marking tracker done: {raw_file_path}"
            )
            mark_done_raw(file_name, trade_date, upload_ts)
            recovered += 1
            continue

        if target_exists:
            logger.warning(f"Target already exists and will be replaced: {raw_file_path}")

        dbutils.fs.mkdirs(_parent_dir(raw_file_path))
        logger.info(f"Moving {file_name}: {sharepoint_file_path} -> {raw_file_path}")
        dbutils.fs.mv(sharepoint_file_path, raw_file_path)

        # Clean up empty upload folders (base folder is preserved)
        cleanup_empty_dirs_under(SHAREPOINT_TD_COWEN_WRA_BASE_PATH)

        mark_done_raw(file_name, trade_date, upload_ts)
        moved += 1
        logger.info(f"Moved {file_name}, cleaned up empty upload dirs, marked is_done_raw = TRUE")

logger.info(f"Summary: moved={moved}, recovered={recovered}, skipped={skipped}")

# COMMAND ----------

# End of Notebook