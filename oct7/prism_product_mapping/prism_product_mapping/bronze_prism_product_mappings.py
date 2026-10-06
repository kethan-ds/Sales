# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze - Prism Product Mappings (Unity Catalog)
# MAGIC Loads each raw Prism product mapping CSV (tracker: `is_done_raw = TRUE`, `is_done_bronze = FALSE`) into `bronze.prism_product_mappings` as a full snapshot overwrite, then marks the tracker record `is_done_bronze = TRUE`.

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
import re
import sys

import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

logger = logging.getLogger("bronze_prism_product_mappings")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

# DBTITLE 1,Configuration
PRODUCT = "prism_product_mappings"

SHAREPOINT_TRACKER_TABLE = f"`{CATALOG_NAME}`.raw.sharepoint_ingestion_tracker"
BRONZE_PRISM_PRODUCT_MAPPINGS = f"`{CATALOG_NAME}`.bronze.prism_product_mappings"

# VOLUME_BASE_PATH (/Volumes/<catalog>/raw/data) maps to <adls_base>/raw,
# so the legacy "raw/" prefix is dropped.
PRISM_PRODUCT_MAPPINGS_BASE_PATH = f"{VOLUME_BASE_PATH}/product_mappings/prism"

# COMMAND ----------

# Business columns expected in every source CSV (exact header names)
SOURCE_COLUMNS = [
    "source_system",
    "product_level_0",
    "product_level_1",
    "product_level_2",
    "product_level_3",
    "in_out",
    "prism_product_level_0",
    "prism_product_level_1",
    "prism_product_level_2",
    "prism_product_level_3",
]
AUDIT_COLUMNS = ["file_name_path", "ingestion_timestamp"]
TARGET_COLUMNS = SOURCE_COLUMNS + AUDIT_COLUMNS

logger.info(f"Catalog        : {CATALOG_NAME}")
logger.info(f"Tracker table  : {SHAREPOINT_TRACKER_TABLE}")
logger.info(f"Bronze table   : {BRONZE_PRISM_PRODUCT_MAPPINGS}")
logger.info(f"Raw base path  : {PRISM_PRODUCT_MAPPINGS_BASE_PATH}")

# COMMAND ----------

# DBTITLE 1,Create bronze table (managed)
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

# DBTITLE 1,Helpers
def _is_not_found(exc: Exception) -> bool:
    msg = str(exc)
    return (
        "FileNotFoundException" in msg
        or "does not exist" in msg.lower()
        or "NOT_FOUND" in msg
    )


def path_exists(path: str) -> bool:
    """True if the path exists. Only 'not found' errors return False; any other
    error (e.g. missing READ VOLUME grant) is raised instead of being treated
    as a missing file."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as exc:
        if _is_not_found(exc):
            return False
        raise


# COMMAND ----------

_VOLUME_PATH_PATTERN = r"^(dbfs:)?" + re.escape(VOLUME_BASE_PATH)


def lineage_file_path():
    return F.regexp_replace(
        F.col("_metadata.file_path"),
        _VOLUME_PATH_PATTERN,
        EXTERNAL_LOCATION_BASE_PATH,
    )


def validate_columns(df, file_path: str) -> None:
    """Fail with a clear message if the CSV header does not match the bronze schema."""
    actual = set(df.columns)
    expected = set(SOURCE_COLUMNS)
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if missing or unexpected:
        raise ValueError(
            f"Column mismatch in {file_path}. "
            f"Missing: {missing or 'none'}. Unexpected: {unexpected or 'none'}."
        )

# COMMAND ----------

# DBTITLE 1,Read pending tracker records
with capture_errors("bronze_prism_product_mappings_read_tracker"):
    tracker_table = DeltaTable.forName(spark, SHAREPOINT_TRACKER_TABLE)

    tracker_rows = (
        spark.table(SHAREPOINT_TRACKER_TABLE)
        .filter(
            (F.col("product") == F.lit(PRODUCT))
            & (F.col("is_done_raw"))
            & (~F.col("is_done_bronze"))
        )
        .orderBy(
            F.col("file_name").asc(),
            F.col("trade_date").asc(),
            F.col("upload_timestamp").asc(),
        )
        .select("file_name", "trade_date", "upload_timestamp")
        .collect()
    )

# COMMAND ----------

if tracker_rows:
    logger.info(f"Found {len(tracker_rows)} record(s) to process")
    details = "\n".join(
        f"  file_name={r.file_name}, trade_date={r.trade_date}, upload_timestamp={r.upload_timestamp}"
        for r in tracker_rows
    )
    logger.info(f"Records:\n{details}")
    if len(tracker_rows) > 1:
        logger.warning(
            "More than one pending file: each file overwrites bronze, so only the "
            "last file in processing order will remain in bronze."
        )
else:
    logger.info("No files to process")


# COMMAND ----------

# DBTITLE 1,Load files into bronze and update tracker
def mark_done_bronze(file_name, trade_date, upload_ts) -> None:
    tracker_table.update(
        condition=(
            (F.col("file_name") == F.lit(file_name))
            & (F.col("product") == F.lit(PRODUCT))
            & (F.col("trade_date") == F.lit(trade_date).cast("date"))
            & (F.col("upload_timestamp") == F.lit(upload_ts).cast("timestamp"))
            & (F.col("is_done_raw"))
            & (~F.col("is_done_bronze"))
        ),
        set={"is_done_bronze": F.lit(True)},
    )

# COMMAND ----------

loaded, skipped = 0, 0

with capture_errors("bronze_prism_product_mappings_load"):
    for row in tracker_rows:
        trade_date = row["trade_date"]
        upload_ts = row["upload_timestamp"]
        file_name = row["file_name"]

        raw_file_path = (
            f"{PRISM_PRODUCT_MAPPINGS_BASE_PATH}/"
            f"{trade_date.year:04d}/{trade_date.month:02d}/{trade_date.day:02d}/{file_name}"
        )

        if not path_exists(raw_file_path):
            logger.warning(f"File not found, skipping: {raw_file_path}")
            skipped += 1
            continue

        source_df = spark.read.csv(raw_file_path, header=True, inferSchema=False)
        validate_columns(source_df, raw_file_path)

        new_bronze_df = (
            source_df
            .withColumn("file_name_path", lineage_file_path())
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .select(*TARGET_COLUMNS)
        )

        if new_bronze_df.isEmpty():
            logger.warning(f"Empty file, skipping: {raw_file_path}")
            skipped += 1
            continue

        # Full snapshot: overwrite bronze with this file
        (
            new_bronze_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(BRONZE_PRISM_PRODUCT_MAPPINGS)
        )

        mark_done_bronze(file_name, trade_date, upload_ts)
        loaded += 1
        logger.info(f"Overwrote {BRONZE_PRISM_PRODUCT_MAPPINGS} with {file_name}, marked is_done_bronze = TRUE")

logger.info(f"Summary: loaded={loaded}, skipped={skipped}")


# COMMAND ----------

# End of Notebook