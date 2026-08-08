# Databricks notebook source
import logging
import re
from datetime import date, datetime
from typing import TypedDict

import pyspark.sql.functions as func

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constants

# COMMAND ----------

DATE_FORMAT = "%m-%d-%Y"
CAD_PRIME_RAW_TRACKER_DELTA = f"`{CATALOG_NAME}`.`raw`.`cad_prime_raw_tracker_delta`"
BRONZE_PRIME_SL_TABLE_NAME = f"`{CATALOG_NAME}`.`bronze`.`cad_prime_securities_lending`"
SL_FILE_TYPE = "sec_lending_file"

SL_REGEX = re.compile(
    r"cv_sb_fytd_([^.]*)\.x[ls]+.*",
    flags=re.IGNORECASE,
)

REGEX_REPLACE_SYMBOLS_FOR_DECIMAL_CASTING = r"[$,%]"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create new table for CV securities lending (managed table)

# COMMAND ----------

logger.info(
    "Creating bronze Delta table, if required: %s",
    BRONZE_PRIME_SL_TABLE_NAME,
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_PRIME_SL_TABLE_NAME} (
        cptyname STRING,
        contra_party STRING,
        period_client_value_sum DECIMAL(20, 2),
        period_borrow_fees_sum DECIMAL(20, 2),
        period_loan_fees_sum DECIMAL(20, 2),
        Source_File STRING,
        Processing_Time TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query unprocessed CV Securities lending files from the raw tracker

# COMMAND ----------

unprocessed_raw_cad_prime_sl = spark.sql(
    f"""
    SELECT DISTINCT
        file_name,
        file_path,
        file_type,
        file_date
    FROM {CAD_PRIME_RAW_TRACKER_DELTA}
    WHERE file_type = '{SL_FILE_TYPE}'
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Type definitions

# COMMAND ----------

class CADPrimeRow(TypedDict):
    file_name: str
    file_path: str  # Volume (FUSE) path -- used to actually read the file
    file_type: str
    file_date: date
    timestamp: datetime

# COMMAND ----------

def to_external_path(volume_path: str) -> str:
    """Derive the ADLS lineage path from a Volume (FUSE) path."""
    return volume_path.replace(VOLUME_BASE_PATH, EXTERNAL_LOCATION_BASE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC # validate file function

# COMMAND ----------

@enhanced_errors()
def validate_sl_file(file_name: str, file_date: date) -> bool:
    """Validate that the file name matches the expected pattern and tracker date."""

    sl_file_match = SL_REGEX.match(file_name)

    if not sl_file_match:
        logger.warning(
            "File %s does not match the expected CV_SB_FYTD_* pattern; skipping.",
            file_name,
        )
        return False

    file_date_from_name = datetime.strptime(
        sl_file_match.group(1),
        DATE_FORMAT,
    ).date()

    if file_date and file_date_from_name != file_date:
        logger.warning(
            "File date mismatch for %s: filename date %s != tracker date %s",
            file_name,
            file_date_from_name,
            file_date,
        )
        return False

    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process file function

# COMMAND ----------

@enhanced_errors()
def process_sl_file(sl_file: CADPrimeRow) -> bool:
    """Process one Sec Lending file and merge its data into the bronze table."""

    volume_file_path = sl_file["file_path"]
    external_file_path = to_external_path(volume_file_path)

    logger.info("Reading CV Securities file: %s", volume_file_path)

    df = (
        spark.read
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "false")
        .load(volume_file_path)
    )

    expected_cols = [
        "cptyname",
        "contra_party",
        "period_client_value_sum",
        "period_borrow_fees_sum",
        "period_loan_fees_sum",
    ]

    # Some Excel extracts arrive with generic headers A-E.
    if "period_client_value_sum" not in df.columns:
        generic_cols = ["A", "B", "C", "D", "E"]

        if all(col in df.columns for col in generic_cols):
            logger.info(
                "Detected generic headers (A-E). "
                "Renaming them to the expected Sec Lending columns."
            )

            rename_map = {
                "A": "cptyname",
                "B": "contra_party",
                "C": "period_client_value_sum",
                "D": "period_borrow_fees_sum",
                "E": "period_loan_fees_sum",
            }

            for source_column, target_column in rename_map.items():
                df = df.withColumnRenamed(source_column, target_column)

    missing_expected = [
        column for column in expected_cols if column not in df.columns
    ]

    if missing_expected:
        raise ValueError(
            "Missing expected columns after normalization: "
            f"{missing_expected}. Available columns: {df.columns}"
        )

    # Normalize Excel numeric-like contra-party values such as 12.0 and
    # ensure numeric values are represented as four-digit zero-padded codes.
    contra_party_trimmed = func.trim(
        func.col("contra_party").cast("string")
    )

    contra_party_numeric_part = func.regexp_extract(
        contra_party_trimmed,
        r"^([0-9]+)(?:\.[0-9]+)?$",
        1,
    )

    df = df.withColumn(
        "contra_party",
        func.when(
            func.col("contra_party").isNull()
            | (contra_party_trimmed == ""),
            contra_party_trimmed,
        )
        .when(
            contra_party_numeric_part != "",
            func.lpad(contra_party_numeric_part, 4, "0"),
        )
        .otherwise(contra_party_trimmed),
    )

    decimal_cols = [
        "period_client_value_sum",
        "period_borrow_fees_sum",
        "period_loan_fees_sum",
    ]

    for column in decimal_cols:
        df = df.withColumn(
            column,
            func.regexp_replace(
                func.col(column),
                REGEX_REPLACE_SYMBOLS_FOR_DECIMAL_CASTING,
                "",
            ),
        )

        df = df.withColumn(
            column,
            func.col(column).cast("decimal(20, 2)"),
        )

    df = (
        df.withColumn(
            "Source_File",
            func.lit(external_file_path),
        )
        .withColumn(
            "Processing_Time",
            func.current_timestamp(),
        )
    )

    # Reload semantics: remove any existing rows for this source file first.
    existing_count = spark.sql(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {BRONZE_PRIME_SL_TABLE_NAME}
        WHERE Source_File = '{external_file_path}'
        """
    ).collect()[0]["cnt"]

    if existing_count > 0:
        logger.info(
            "Found %s existing rows for source file %s; deleting before reload.",
            existing_count,
            external_file_path,
        )

        spark.sql(
            f"""
            DELETE FROM {BRONZE_PRIME_SL_TABLE_NAME}
            WHERE Source_File = '{external_file_path}'
            """
        )

        logger.info(
            "Deleted existing rows for %s",
            external_file_path,
        )

    logger.info(
        "Inserting CV Securities file: %s",
        sl_file["file_name"],
    )

    df = df.select(
        "cptyname",
        "contra_party",
        "period_client_value_sum",
        "period_borrow_fees_sum",
        "period_loan_fees_sum",
        "Source_File",
        "Processing_Time",
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_PRIME_SL_TABLE_NAME)
    )

    logger.info(
        "Successfully inserted data from %s",
        sl_file["file_name"],
    )

    return True

# COMMAND ----------

with capture_errors(section="cad_prime_cv_securities_lending_merge"):
    raw_cad_prime_sl_files: list[CADPrimeRow] = (
        unprocessed_raw_cad_prime_sl.collect()
    )

    if not raw_cad_prime_sl_files:
        logger.info("No CV Securities Lending files to process")

    else:
        processed_files: list[CADPrimeRow] = []

        for sl_file in raw_cad_prime_sl_files:
            logger.info("Processing file: %s", sl_file)

            if not validate_sl_file(
                sl_file["file_name"],
                sl_file["file_date"],
            ):
                continue

            if process_sl_file(sl_file):
                processed_files.append(sl_file)

        # Batch-delete successfully processed files from the tracker.
        if processed_files:
            file_conditions = " OR ".join(
                [
                    (
                        f"(file_name = '{file['file_name']}' "
                        f"AND file_path = '{file['file_path']}')"
                    )
                    for file in processed_files
                ]
            )

            spark.sql(
                f"""
                DELETE FROM {CAD_PRIME_RAW_TRACKER_DELTA}
                WHERE {file_conditions}
                """
            )

            logger.info(
                "Removed %s CV Securities Lending files from the raw tracker table",
                len(processed_files),
            )