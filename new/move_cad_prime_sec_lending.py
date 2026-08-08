# Databricks notebook source
import json
import logging
import re
from datetime import date, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, StringType, StructField, StructType

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# MAGIC %run "./dateutils.py"

# COMMAND ----------

# File name example expected by the original notebook:
# cv_sb_fytd_<trade-date-or-identifier>.xls / .xlsx
SL_REGEX = re.compile(r"cv_sb_fytd_([^.]*)\.x[ls]+.*", flags=re.IGNORECASE)
DATE_FORMAT = "%m-%d-%Y"

SL_FILE_TYPE = "sec_lending_file"
CAD_PRIME_RAW_TRACKER_DELTA = "cad_prime_raw_tracker_delta"

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

CAD_PRIME_RAW_TRACKER_TABLE = f"`{CATALOG_NAME}`.`raw`.`{CAD_PRIME_RAW_TRACKER_DELTA}`"
BRONZE_HOLIDAY_CALENDAR_TABLE = f"`{CATALOG_NAME}`.`bronze`.`holiday_calendar`"

# COMMAND ----------

# Volume (FUSE) paths -- used for all actual read/list/move operations.
CAD_PRIME_TMP_PATH = f"{VOLUME_BASE_PATH}/tmp/cad_prime"
SL_OUTPUT_DIR_BASE_PATH = f"{VOLUME_BASE_PATH}/trades/cad_prime"

# COMMAND ----------

dbutils.widgets.text("region", "TOR")
dbutils.widgets.text("trade_date", "")

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tracking table

# COMMAND ----------

logger.info(
    "Creating raw CAD Prime tracker Delta table, if required: %s",
    CAD_PRIME_RAW_TRACKER_TABLE,
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CAD_PRIME_RAW_TRACKER_TABLE} (
        file_name STRING,
        file_path STRING,
        file_type STRING,
        file_date DATE,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
    """
)

# COMMAND ----------

tracker_schema = StructType(
    [
        StructField("file_name", StringType(), False),
        StructField("file_path", StringType(), False),
        StructField("file_date", DateType(), True),
        StructField("file_type", StringType(), False),
    ]
)

# COMMAND ----------

@enhanced_errors()
def save_to_delta(
    file_name: str,
    file_path: str,
    file_date: date,
    file_type: str,
) -> None:
    """Append a processed-file record to the CAD Prime tracker table."""
    tracker_row = [(file_name, file_path, file_date, file_type)]
    tracker_df = spark.createDataFrame(tracker_row, tracker_schema)

    (
        tracker_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(CAD_PRIME_RAW_TRACKER_TABLE)
    )

# COMMAND ----------

@enhanced_errors()
def is_not_holiday(
    region: str,
    run_date: date,
    holiday_table: DataFrame,
) -> bool:
    """Return True when the supplied date is not a holiday for the region."""
    matching_holidays = (
        holiday_table
        .filter(
            (holiday_table.code == region.upper())
            & (holiday_table.date == run_date)
        )
        .limit(1)
        .collect()
    )
    return not matching_holidays

# COMMAND ----------

@enhanced_errors()
def get_sl_new_path(file_date: date, file_name: str) -> str:
    """Build the year/month/day destination path for a Sec Lending file."""
    year = file_date.year
    month = f"{file_date.month:02d}"
    day = f"{file_date.day:02d}"

    return f"{SL_OUTPUT_DIR_BASE_PATH}/{year}/{month}/{day}/{file_name}"

# COMMAND ----------

with capture_errors(section="file moving process in cad prime sec_lending"):
    logger.info("Starting file-moving process for sec_lending...")

    dates_processed = []
    files_processed = []
    file_processed = None

    region = dbutils.widgets.get("region")
    trade_date = string_to_date(dbutils.widgets.get("trade_date"))

    holiday_calendar_df = (
        DeltaTable.forName(
            sparkSession=spark,
            tableOrViewName=BRONZE_HOLIDAY_CALENDAR_TABLE,
        )
        .toDF()
    )

    # Get all matching Sec Lending files from the temporary landing directory.
    all_files = dbutils.fs.ls(CAD_PRIME_TMP_PATH)
    files_to_process = [
        file_info
        for file_info in all_files
        if not file_info.isDir() and SL_REGEX.match(file_info.name)
    ]

    # Keep only files whose embedded date matches the requested trade date.
    run_date_files = []

    for file_info in files_to_process:
        sl_file_match = SL_REGEX.match(file_info.name)
        file_date = datetime.strptime(
            sl_file_match.group(1),
            DATE_FORMAT,
        ).date()

        if file_date == trade_date:
            run_date_files.append((file_info, file_date))

    if run_date_files:
        logger.info(
            "Found %s file(s) for run date %s. "
            "Processing without holiday check.",
            len(run_date_files),
            trade_date,
        )

        for file_info, file_date in sorted(
            run_date_files,
            key=lambda item: item[0].name,
        ):
            logger.info("Processing Sec Lending file: %s", file_info.name)

            sl_new_path = get_sl_new_path(file_date, file_info.name)
            destination_directory = (
                f"{SL_OUTPUT_DIR_BASE_PATH}/"
                f"{file_date.year}/"
                f"{file_date.month:02d}/"
                f"{file_date.day:02d}"
            )

            dbutils.fs.mkdirs(destination_directory)
            dbutils.fs.mv(
                f"{CAD_PRIME_TMP_PATH}/{file_info.name}",
                sl_new_path,
            )

            logger.info(
                "Moved file %s/%s to location %s",
                CAD_PRIME_TMP_PATH,
                file_info.name,
                sl_new_path,
            )

            save_to_delta(
                file_name=file_info.name,
                file_path=sl_new_path,
                file_date=file_date,
                file_type=SL_FILE_TYPE,
            )

            file_processed = {
                "name": sl_new_path,
                "type": SL_FILE_TYPE,
            }
            files_processed.append(file_processed)
            dates_processed.append(file_date.strftime("%Y-%m-%d"))

    else:
        logger.info(
            "No file found for run date %s. Checking holiday calendar.",
            trade_date,
        )

        is_holiday = not is_not_holiday(
            region,
            trade_date,
            holiday_calendar_df,
        )

        if is_holiday:
            logger.info("Trade date %s is a holiday.", trade_date)
        else:
            logger.warning(
                "No Sec Lending file found in %s for non-holiday run date %s",
                CAD_PRIME_TMP_PATH,
                trade_date,
            )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Display raw data

# COMMAND ----------

if file_processed:
    df = (
        spark.read
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "false")
        .load(file_processed["name"])
    )

    display(df)

# COMMAND ----------

results = {
    "has_results": file_processed is not None,
    "files_processed": files_processed,
    "dates_processed": dates_processed,
}

dbutils.notebook.exit(json.dumps(results))