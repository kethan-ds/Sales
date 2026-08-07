# Databricks notebook source
import traceback
import logging
from typing import TypedDict, Any
from datetime import datetime, date

import pyspark.sql.functions as func
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    IntegerType,
)
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ],
)

logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constants

# COMMAND ----------

DATE_FORMAT = "%m-%d-%Y"
ACCOUNT_FILE_TYPE = "accountFile"
PNL_FILE_TYPE_DAILY = "pnlFileDaily"
PNL_FILE_TYPE_RESTATEMENT = "pnlFileRestatement"
PNL_FILE_TYPE_PREFIX = "pnlFile"

CAD_PRIME_RAW_TRACKER_DELTA = f"`{CATALOG_NAME}`.`raw`.`cad_prime_raw_tracker_delta`"
BRONZE_PRIME_TABLE_NAME = f"`{CATALOG_NAME}`.`bronze`.`cad_prime_pnl_summary`"
BRONZE_PRIME_ACCTS_TABLE_NAME = f"`{CATALOG_NAME}`.`bronze`.`cad_prime_accts`"
REGEX_REPLACE_SYMBOLS_FOR_DECIMAL_CASTING = r"[$,%]"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create new tables for summary and accounts (managed tables)

# COMMAND ----------

logger.info(
    f"Creating bronze Delta table (if required) {BRONZE_PRIME_TABLE_NAME}"
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_PRIME_TABLE_NAME} (
        `Firm_Name` STRING,
        `c360_Firm_Name` STRING,
        `Account_Type` STRING,
        `Stock_Loan_P&L_CAD` DECIMAL(20, 2),
        `Adj_Interest_CAD` DECIMAL(20, 2),
        `Commission_CAD` DECIMAL(20, 2),
        `Custody_Fee_No_Tax` DECIMAL(20, 2),
        `Total_Transaction_Fee` DECIMAL(20, 2),
        `Option_GiveUps` DECIMAL(20, 2),
        `PB_PnL_CAD` DECIMAL(20, 2),
        `Pct` DECIMAL(20, 2),
        `Running_Pct` DECIMAL(20, 2),
        `AVG_Equity_CAD` DECIMAL(20, 2),
        `ROE` DECIMAL(20, 4),
        `PnL_CAD` DECIMAL(20, 2),
        `Trade_Date` DATE,
        `Source_File` STRING,
        `Processing_Time` TIMESTAMP,
        `Fiscal_Year` INT
    )
    USING DELTA
    PARTITIONED BY (Fiscal_Year)
    """
)

# COMMAND ----------

logger.info(
    f"Creating bronze Delta table (if required) {BRONZE_PRIME_ACCTS_TABLE_NAME}"
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_PRIME_ACCTS_TABLE_NAME} (
        `name` STRING,
        `firm` STRING,
        `ism_id` STRING,
        `ism_omnibus_id` STRING,
        `Status` STRING,
        `processing_time` TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------

unprocessed_raw_cad_prime_records = spark.sql(
    f"""
    SELECT DISTINCT
        file_name,
        file_path,
        file_type,
        file_date,
        timestamp
    FROM {CAD_PRIME_RAW_TRACKER_DELTA}
    WHERE file_type IN ('{PNL_FILE_TYPE_DAILY}', '{PNL_FILE_TYPE_RESTATEMENT}')
      AND timestamp IN (
          SELECT MAX(timestamp)
          FROM {CAD_PRIME_RAW_TRACKER_DELTA}
          WHERE file_type IN ('{PNL_FILE_TYPE_DAILY}', '{PNL_FILE_TYPE_RESTATEMENT}')
          GROUP BY file_date
      )
    ORDER BY file_date
    """
)

# COMMAND ----------

unprocessed_raw_cad_prime_accounts = spark.sql(
    f"""
    SELECT DISTINCT
        file_name,
        file_path,
        file_type,
        file_date
    FROM {CAD_PRIME_RAW_TRACKER_DELTA}
    WHERE file_type = '{ACCOUNT_FILE_TYPE}'
    """
)

# COMMAND ----------

class CADPrimeRow(TypedDict):
    file_name: str
    file_path: str  # Volume (FUSE) path -- used to actually read the file
    file_type: str
    file_date: date
    timestamp: datetime


def to_external_path(volume_path: str) -> str:
    """Derive the ADLS lineage path from a Volume (FUSE) path."""
    return volume_path.replace(VOLUME_BASE_PATH, EXTERNAL_LOCATION_BASE_PATH)



# COMMAND ----------

@enhanced_errors()
def process_pnl_file(
    pnl_file: CADPrimeRow,
    pnl_summary_delta_table: DeltaTable,
    field_setters: dict[str, Any],
    regex_replace_symbols: str,
) -> DataFrame:
    logger.info(f"Processing PNL file: {pnl_file} for bronze")

    volume_file_path = pnl_file["file_path"]
    external_file_path = to_external_path(volume_file_path)

    df = (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "false")
        .load(volume_file_path)
    )

    df = df.withColumn("Trade_Date", func.lit(pnl_file["file_date"]))
    df = df.withColumn(
        "Fiscal_Year",
        func.when(
            func.month(func.col("Trade_Date")) < 11,
            func.year(func.col("Trade_Date")),
        ).otherwise(
            func.year(func.col("Trade_Date")) + 1
        ),
    )

    df = df.withColumn("Source_File", func.lit(external_file_path))
    df = df.withColumn("Processing_Time", func.current_timestamp())

    # Rename columns to remove characters not allowed in Delta column names.
    df = df.withColumnRenamed("Firm Name", "Firm_Name")
    df = df.withColumnRenamed("Account Type", "Account_Type")
    df = df.withColumnRenamed("Stock Loan P&L (CAD)", "Stock_Loan_P&L_CAD")
    df = df.withColumnRenamed("Adj Interest (CAD)", "Adj_Interest_CAD")
    df = df.withColumnRenamed("Commission (CAD)", "Commission_CAD")
    df = df.withColumnRenamed("Custody Fee No Tax", "Custody_Fee_No_Tax")
    df = df.withColumnRenamed("Total Transaction Fee", "Total_Transaction_Fee")
    df = df.withColumnRenamed("PB PnL (CAD)", "PB_PnL_CAD")
    df = df.withColumnRenamed("Running Pct", "Running_Pct")
    df = df.withColumnRenamed("AVG Equity (CAD)", "AVG_Equity_CAD")
    df = df.withColumnRenamed("PnL (CAD)", "PnL_CAD")

    # Clean up text columns.
    df = df.withColumn(
        "c360_Firm_Name",
        func.lower(func.trim(func.col("Firm_Name"))),
    )
    df = df.withColumn(
        "Account_Type",
        func.trim(func.col("Account_Type")),
    )

    # Remove symbols that can cause decimal conversion failures.
    decimal_columns = [
        "Stock_Loan_P&L_CAD",
        "Adj_Interest_CAD",
        "Commission_CAD",
        "Custody_Fee_No_Tax",
        "Total_Transaction_Fee",
        "Option_GiveUps",
        "PB_PnL_CAD",
        "Pct",
        "Running_Pct",
        "AVG_Equity_CAD",
        "ROE",
        "PnL_CAD",
    ]

    for column_name in decimal_columns:
        df = df.withColumn(
            column_name,
            func.regexp_replace(
                column_name,
                regex_replace_symbols,
                "",
            ),
        )

    logger.info("Merging raw to bronze...")

    (
        pnl_summary_delta_table.alias("target")
        .merge(
            source=df.alias("source"),
            condition=(
                "target.`Firm_Name` = source.`Firm_Name` "
                "AND target.`Trade_Date` = source.`Trade_Date`"
            ),
        )
        .whenMatchedUpdate(set=field_setters)
        .whenNotMatchedInsert(values=field_setters)
        .execute()
    )

    logger.info(
        "Deleting raw tracker DeltaTable row for processed file: "
        f"{pnl_file['file_name']}, file path: {pnl_file['file_path']}"
    )

    spark.sql(
        f"""
        DELETE FROM {CAD_PRIME_RAW_TRACKER_DELTA}
        WHERE file_name = '{pnl_file["file_name"]}'
          AND file_path = '{pnl_file["file_path"]}'
        """
    )

    logger.info(f"Processing file {pnl_file} done")
    return df

# COMMAND ----------

@enhanced_errors()
def remove_trades_not_present(
    pnl_summary_delta_table: DeltaTable,
    frames: list[DataFrame],
):
    df = frames[0] if frames else None

    if not df:
        return

    for frame in frames[1:]:
        df = df.union(frame)

    df = df.withColumn(
        "ClientDate",
        func.concat(
            func.col("Firm_Name"),
            func.col("Trade_Date"),
        ),
    )

    trade_dates = [
        row.Trade_Date
        for row in df.select(df.Trade_Date).distinct().collect()
        if row.Trade_Date
    ]

    trades_for_dates_df = (
        pnl_summary_delta_table.toDF()
        .filter(func.col("Trade_Date").isin(trade_dates))
        .withColumn(
            "ClientDate",
            func.concat(
                func.col("Firm_Name"),
                func.col("Trade_Date"),
            ),
        )
    )

    client_dates = [
        row.ClientDate
        for row in df.collect()
        if row.ClientDate
    ]

    trades_for_dates_df = trades_for_dates_df.filter(
        ~trades_for_dates_df.ClientDate.isin(client_dates)
    )

    logger.info(
        "Removing trades not present in the source from bronze (if any)..."
    )

    (
        pnl_summary_delta_table.alias("target")
        .merge(
            trades_for_dates_df.alias("source"),
            (
                "target.Firm_Name = source.Firm_Name "
                "AND target.Trade_Date = source.Trade_Date"
            ),
        )
        .whenMatchedDelete()
        .execute()
    )

# COMMAND ----------

def _sql_in_list(values: list[str]) -> str:
    """Build a valid SQL string literal list, safe for a single-element list."""
    escaped = [v.replace("'", "''") for v in values]
    return "(" + ", ".join(f"'{v}'" for v in escaped) + ")"

# COMMAND ----------

with capture_errors(section="cad_prime_pnl_processing_and_cleanup"):
    raw_cad_prime_pnl_files: list[CADPrimeRow] = (
        unprocessed_raw_cad_prime_records.collect()
    )

    pnl_summary_delta_table = DeltaTable.forName(
        spark,
        BRONZE_PRIME_TABLE_NAME,
    )

    field_setters = {
        "Firm_Name": func.col("source.Firm_Name"),
        "c360_Firm_Name": func.col("source.c360_Firm_Name"),
        "Account_Type": func.col("source.Account_Type"),
        "Stock_Loan_P&L_CAD": func.col("source.Stock_Loan_P&L_CAD"),
        "Adj_Interest_CAD": func.col("source.Adj_Interest_CAD"),
        "Commission_CAD": func.col("source.Commission_CAD"),
        "Custody_Fee_No_Tax": func.col("source.Custody_Fee_No_Tax"),
        "Total_Transaction_Fee": func.col("source.Total_Transaction_Fee"),
        "Option_GiveUps": func.col("source.Option_GiveUps"),
        "PB_PnL_CAD": func.col("source.PB_PnL_CAD"),
        "Pct": func.col("source.Pct"),
        "Running_Pct": func.col("source.Running_Pct"),
        "AVG_Equity_CAD": func.col("source.AVG_Equity_CAD"),
        "ROE": func.col("source.ROE"),
        "PnL_CAD": func.col("source.PnL_CAD"),
        "Trade_Date": func.col("source.Trade_Date"),
        "Source_File": func.col("source.Source_File"),
        "Processing_Time": func.col("source.Processing_Time"),
        "Fiscal_Year": func.col("source.Fiscal_Year"),
    }

    frames: list[DataFrame] = []
    failed_files: list[str] = []

    for pnl_file in raw_cad_prime_pnl_files:
        try:
            frames.append(
                process_pnl_file(
                    pnl_file,
                    pnl_summary_delta_table,
                    field_setters,
                    REGEX_REPLACE_SYMBOLS_FOR_DECIMAL_CASTING,
                )
            )
        except Exception:
            traceback.print_exc()
            failed_files.append(pnl_file["file_path"])

    if failed_files:
        spark.sql(
            f"""
            DELETE FROM {CAD_PRIME_RAW_TRACKER_DELTA}
            WHERE file_type LIKE '{PNL_FILE_TYPE_PREFIX}%'
              AND file_path NOT IN {_sql_in_list(failed_files)}
            """
        )
    else:
        spark.sql(
            f"""
            DELETE FROM {CAD_PRIME_RAW_TRACKER_DELTA}
            WHERE file_type LIKE '{PNL_FILE_TYPE_PREFIX}%'
            """
        )

    remove_trades_not_present(
        pnl_summary_delta_table,
        frames,
    )

    logger.info(
        f"Cleaning up {BRONZE_PRIME_TABLE_NAME} where firm_name is null"
    )

    # Remove summary rows that always have a null Firm Name.
    spark.sql(
        f"""
        DELETE FROM {BRONZE_PRIME_TABLE_NAME}
        WHERE FIRM_NAME IS NULL
        """
    )

# COMMAND ----------

with capture_errors(section="cad_prime_accounts_merge"):
    raw_cad_prime_pnl_accounts: list[CADPrimeRow] = (
        unprocessed_raw_cad_prime_accounts.collect()
    )

    accounts_delta_table = DeltaTable.forName(
        spark,
        BRONZE_PRIME_ACCTS_TABLE_NAME,
    )

    field_setters = {
        "name": func.col("source.name"),
        "firm": func.col("source.firm"),
        "ism_id": func.col("source.ism_id"),
        "ism_omnibus_id": func.col("source.ism_omnibus_id"),
        "Status": func.col("source.Status"),
        "processing_time": func.col("source.processing_time"),
    }

    for accounts_file in raw_cad_prime_pnl_accounts:
        logger.info(
            f"Reading accounts file: {accounts_file['file_path']}"
        )

        df = (
            spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("treatEmptyValuesAsNulls", "true")
            .option("inferSchema", "false")
            .load(accounts_file["file_path"])
        )

        df = df.withColumn(
            "processing_time",
            func.current_timestamp(),
        )

        logger.info(f"Merging account file: {accounts_file}")

        (
            accounts_delta_table.alias("target")
            .merge(
                source=df.alias("source"),
                condition="target.ism_id = source.ism_id",
            )
            .whenMatchedUpdate(set=field_setters)
            .whenNotMatchedInsert(values=field_setters)
            .whenNotMatchedBySourceDelete()
            .execute()
        )

        spark.sql(
            f"""
            DELETE FROM {CAD_PRIME_RAW_TRACKER_DELTA}
            WHERE file_name = '{accounts_file["file_name"]}'
              AND file_path = '{accounts_file["file_path"]}'
            """
        )

        logger.info(
            "Remove account file from raw tracker table "
            f"{CAD_PRIME_RAW_TRACKER_DELTA}: {accounts_file['file_name']}"
        )