# Databricks notebook source
import re
import json
import logging
from re import Match
from datetime import datetime, date
from collections import namedtuple

from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql import DataFrame

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

dbutils.widgets.text("region", "TOR")
dbutils.widgets.text("trade_date", "")

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# MAGIC %run "./dateutils.py"

# COMMAND ----------

ACCOUNT_5J5_REGEX = re.compile(
    r"pb_5j5_active_accts.*x[ls]+.*",
    flags=re.IGNORECASE,
)
PNL_REGEX = re.compile(
    r"ytd_pnl_summary_([^-]+).*x[ls]+.*",
    flags=re.IGNORECASE,
)

DATE_FORMAT = "%m-%d-%Y"
ACCOUNT_FILE_TYPE = "accountFile"
PNL_FILE_TYPE_DAILY = "pnlFileDaily"
PNL_FILE_TYPE_RESTATEMENT = "pnlFileRestatement"
PNL_DAILY_PREFIX = "d_"
PNL_RESTATEMENT_PREFIX = "r_"
CAD_PRIME_RAW_TRACKER_DELTA = "cad_prime_raw_tracker_delta"

# COMMAND ----------

CAD_PRIME_RAW_TRACKER_TABLE = f"`{CATALOG_NAME}`.`raw`.`{CAD_PRIME_RAW_TRACKER_DELTA}`"
BRONZE_HOLIDAY_CALENDAR_TABLE = f"`{CATALOG_NAME}`.`bronze`.`holiday_calendar`"

# COMMAND ----------

# Volume (FUSE) paths -- used for all actual read/list/move operations.
# file_path values written to the tracker table are Volume paths; bronze
# derives the ADLS lineage path from these via substitution when needed.
CAD_PRIME_TMP_PATH = f"{VOLUME_BASE_PATH}/tmp/cad_prime"
PNL_OUTPUT_DIR_BASE_PATH = f"{VOLUME_BASE_PATH}/trades/cad_prime"
PNL_CLIENTS_DIR_BASE_PATH = (
    f"{VOLUME_BASE_PATH}/reference_data/clients/cad_prime_accounts"
)

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
    f"Creating raw cad prime tracker Delta table (if required) "
    f"{CAD_PRIME_RAW_TRACKER_TABLE}"
)

# COMMAND ----------

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

# MAGIC %md
# MAGIC ##Handle moving of files and storing in tracking table

# COMMAND ----------

schema = StructType(
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
):
    df = spark.createDataFrame(
        [(file_name, file_path, file_date, file_type)],
        schema,
    )

    df.write.format("delta").mode("append").saveAsTable(
        CAD_PRIME_RAW_TRACKER_TABLE
    )

# COMMAND ----------

def dir_exists(dir_path: str):
    try:
        dbutils.fs.ls(dir_path)
        return True
    except Exception:
        return False

# COMMAND ----------

@enhanced_errors()
def is_restatement_file(file_name: str, file_path: str):
    if not dir_exists(file_path):
        return False

    last_index = file_name.rfind(".")
    search_name = file_name[:last_index]

    if last_index < 0:
        search_name = file_name

    return len(
        [
            f.name
            for f in dbutils.fs.ls(file_path)
            if search_name in f.name
        ]
    ) > 0

# COMMAND ----------

PNLFileData = namedtuple(
    "PNLFileData",
    [
        "date",
        "file_path",
        "file_prefix",
        "pnl_file_type",
        "pnl_new_path",
        "stringfied_date_obj",
    ],
)

# COMMAND ----------

@enhanced_errors()
def get_pnl_file_data(
    date_format: str,
    pnl_file_match: Match,
) -> PNLFileData:
    file = pnl_file_match.string
    date_value = datetime.strptime(
        pnl_file_match.group(1),
        date_format,
    ).date()
    stringfied_date_obj = stringified_date(date_value)

    file_path = (
        f"{PNL_OUTPUT_DIR_BASE_PATH}/"
        f"{stringfied_date_obj.year}/"
        f"{stringfied_date_obj.month}"
    )

    file_prefix = (
        PNL_RESTATEMENT_PREFIX
        if is_restatement_file(
            f"{PNL_DAILY_PREFIX}{file}",
            file_path,
        )
        else PNL_DAILY_PREFIX
    )

    pnl_file_type = (
        PNL_FILE_TYPE_DAILY
        if file_prefix == PNL_DAILY_PREFIX
        else PNL_FILE_TYPE_RESTATEMENT
    )

    pnl_new_path = f"{file_path}/{file_prefix}{file}"

    return PNLFileData(
        date=date_value,
        file_path=file_path,
        file_prefix=file_prefix,
        pnl_file_type=pnl_file_type,
        pnl_new_path=pnl_new_path,
        stringfied_date_obj=stringfied_date_obj,
    )

# COMMAND ----------

@enhanced_errors()
def is_not_holiday(
    region: str,
    run_date: date,
    holiday_table: DataFrame,
) -> bool:
    return not holiday_table.filter(
        (holiday_table.code == region.upper())
        & (holiday_table.date == run_date)
    ).collect()

# COMMAND ----------

with capture_errors(section="file moving process in cad prime"):
    logger.info("Starting file moving process...")

    dates_processed = []
    files_processed = {}

    trade_date = string_to_date(
        dbutils.widgets.get("trade_date")
    )

    holiday_calendar_df = DeltaTable.forName(
        sparkSession=spark,
        tableOrViewName=BRONZE_HOLIDAY_CALENDAR_TABLE,
    ).toDF()

    files_to_process = [
        file
        for file in dbutils.fs.ls(CAD_PRIME_TMP_PATH)
        if not file.isDir()
    ]
    sorted_files = sorted(
        files_to_process,
        key=lambda f: f.name,
    )

    if not sorted_files:
        raise ValueError(
            f"Missing PNL files when searching for tplus date "
            f"{trade_date}; no files found"
        )

    for file in sorted_files:
        pnl_file_match = PNL_REGEX.match(file.name)
        accounts_file_match = ACCOUNT_5J5_REGEX.match(file.name)

        if pnl_file_match:
            logger.info(
                f"Found match, moving file {file.name}"
            )

            pnl_file_data = get_pnl_file_data(
                DATE_FORMAT,
                pnl_file_match,
            )

            dbutils.fs.mv(
                f"{CAD_PRIME_TMP_PATH}/{file.name}",
                pnl_file_data.pnl_new_path,
            )

            logger.info(
                f"Moved file {CAD_PRIME_TMP_PATH}/{file.name} "
                f"to location: {pnl_file_data.pnl_new_path}"
            )

            save_to_delta(
                file.name,
                pnl_file_data.pnl_new_path,
                pnl_file_data.date,
                pnl_file_data.pnl_file_type,
            )

            logger.info(
                f"Move logged in Delta table: "
                f"{CAD_PRIME_RAW_TRACKER_TABLE}"
            )

            files_processed[file.name] = {
                "name": pnl_file_data.pnl_new_path,
                "type": pnl_file_data.pnl_file_type,
            }

            dates_processed.append(
                f"{pnl_file_data.stringfied_date_obj.year}/"
                f"{pnl_file_data.stringfied_date_obj.month}"
            )

        if accounts_file_match:
            logger.info(
                f"Found match for accounts file: {file.name}"
            )

            accounts_new_path = (
                f"{PNL_CLIENTS_DIR_BASE_PATH}/{file.name}"
            )

            dbutils.fs.mv(
                f"{CAD_PRIME_TMP_PATH}/{file.name}",
                accounts_new_path,
            )

            logger.info(
                f"Moved file {CAD_PRIME_TMP_PATH}/{file.name} "
                f"to location: {accounts_new_path}"
            )

            save_to_delta(
                file.name,
                accounts_new_path,
                None,
                ACCOUNT_FILE_TYPE,
            )

            logger.info(
                f"Move logged in Delta table: "
                f"{CAD_PRIME_RAW_TRACKER_TABLE}"
            )

            files_processed[file.name] = {
                "name": accounts_new_path,
                "type": ACCOUNT_FILE_TYPE,
            }

# COMMAND ----------

for file_processed in files_processed.values():
    df = (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "false")
        .load(file_processed["name"])
    )
    display(df)

# COMMAND ----------

results = {
    "has_results": len(files_processed) > 0
    or len(dates_processed) > 0,
    "files_processed": files_processed,
    "dates_processed": dates_processed,
}

dbutils.notebook.exit(json.dumps(results))