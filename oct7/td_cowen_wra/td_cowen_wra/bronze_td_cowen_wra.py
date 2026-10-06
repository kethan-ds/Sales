# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze - TD Cowen WRA (Unity Catalog)
# MAGIC Loads each raw TD Cowen WRA CSV (tracker: `is_done_raw = TRUE`, `is_done_bronze = FALSE`) into `bronze.td_cowen_wra` via MERGE on (TradeDate, ClientIdty), then marks the tracker record `is_done_bronze = TRUE`.
# MAGIC
# MAGIC **Note:** the MERGE uses `whenNotMatchedBySourceDelete`, so after each file bronze contains only the rows present in that file (see review notes).

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
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import NumericType

# COMMAND ----------

logger = logging.getLogger("bronze_td_cowen_wra")
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
BRONZE_WRA_TABLE = f"`{CATALOG_NAME}`.bronze.td_cowen_wra"

# VOLUME_BASE_PATH (/Volumes/<catalog>/raw/data) maps to <adls_base>/raw,
# so the legacy "raw/" prefix is dropped.
WRA_TRADES_BASE_PATH = f"{VOLUME_BASE_PATH}/trades/td_cowen_wra"

SOURCE_COLUMNS = ["TradeDate", "ClientIdty", "ClientName", "NetToFirm_USD"]
AUDIT_COLUMNS = ["file_name_path", "ingestion_timestamp"]
TARGET_COLUMNS = SOURCE_COLUMNS + AUDIT_COLUMNS

logger.info(f"Catalog        : {CATALOG_NAME}")
logger.info(f"Tracker table  : {SHAREPOINT_TRACKER_TABLE}")
logger.info(f"Bronze table   : {BRONZE_WRA_TABLE}")
logger.info(f"Raw base path  : {WRA_TRADES_BASE_PATH}")

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
""")

# COMMAND ----------

# DBTITLE 1,Parsing helpers
def to_decimal(col_name: str, precision: int = 18, scale: int = 2):
    """Parse a text amount: strips $ and commas, handles (6.87) as -6.87,
    returns NULL for empty or unparseable values."""
    s = F.trim(F.regexp_replace(F.col(col_name).cast("string"), r"[$,]", ""))

    return (
        F.when(s == "", None)
        .when(
            s.rlike(r"^\(\d+(\.\d+)?\)$"),
            F.concat(F.lit("-"), F.regexp_replace(s, r"[()]", "")),
        )
        .when(s.rlike(r"^-?\d+(\.\d+)?$"), s)
        .otherwise(None)
        .cast(f"decimal({precision},{scale})")
    )


def amount_to_decimal(df: DataFrame, col_name: str, precision: int = 18, scale: int = 2):
    """If inferSchema already produced a numeric type, cast directly. Casting a
    double to string first can give scientific notation (e.g. '1.0E7'), which
    to_decimal() would silently turn into NULL."""
    if isinstance(df.schema[col_name].dataType, NumericType):
        return F.col(col_name).cast(f"decimal({precision},{scale})")
    return to_decimal(col_name, precision, scale)

# COMMAND ----------

# DBTITLE 1,File and lineage helpers
def _is_not_found(exc: Exception) -> bool:
    msg = str(exc)
    return (
        "FileNotFoundException" in msg
        or "does not exist" in msg.lower()
        or "NOT_FOUND" in msg
    )


def path_exists(path: str) -> bool:
    """True if the path exists. Only 'not found' errors return False; any other
    error (e.g. missing READ VOLUME grant) is raised."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as exc:
        if _is_not_found(exc):
            return False
        raise


# Lineage: store the external ADLS path (as input_file_name() did under HMS),
# not the /Volumes FUSE path. _metadata.file_path may carry a "dbfs:" prefix.
_VOLUME_PATH_PATTERN = r"^(dbfs:)?" + re.escape(VOLUME_BASE_PATH)


def lineage_file_path():
    return F.regexp_replace(
        F.col("_metadata.file_path"),
        _VOLUME_PATH_PATTERN,
        EXTERNAL_LOCATION_BASE_PATH,
    )


def validate_columns(df: DataFrame, file_path: str) -> None:
    """Fail with a clear message if the CSV header does not match the expected columns."""
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

# DBTITLE 1,De-duplication helper
DEDUP_KEY_COLUMNS = ["TradeDate", "ClientIdty"]


def drop_duplicate_keys(df: DataFrame, keys: list[str] = DEDUP_KEY_COLUMNS) -> DataFrame:
    """Remove rows duplicated on the key columns, keeping the most recent by
    ingestion_timestamp. Logs a warning with the count and displays dropped rows."""
    window = Window.partitionBy(*keys).orderBy(F.col("ingestion_timestamp").desc_nulls_last())

    ranked = df.withColumn("_row_num", F.row_number().over(window))

    duplicate_records = ranked.filter(F.col("_row_num") > 1).drop("_row_num")
    duplicate_count = duplicate_records.count()

    if duplicate_count > 0:
        logger.warning(f"Dropping {duplicate_count} duplicate record(s) on keys: {keys}")
        duplicate_records.display()

    return ranked.filter(F.col("_row_num") == 1).drop("_row_num")

# COMMAND ----------

# DBTITLE 1,Read pending tracker records
with capture_errors("bronze_td_cowen_wra_read_tracker"):
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

# DBTITLE 1,Read pending tracker records
with capture_errors("bronze_td_cowen_wra_read_tracker"):
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

# End of Notebook