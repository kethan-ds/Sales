# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import os
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

#dbutils.widgets.text("CATALOG", "")

# COMMAND ----------

CATALOG_NAME = os.getenv("CATALOG_NAME")

# COMMAND ----------

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

def process_pnl_file(
    pnl_file: CADPrimeRow,
    pnl_summary_delta_table: DeltaTable,
    field_setters: dict[str, Any],
    regex_replace_symbols: str,
) -> DataFrame:
    logger.info(f"Processing PNL file: {pnl_file} for bronze")

    volume_file_path = pnl_file["file_path"]
    external_file_path = to_external_path(volume_file_path)

    # Databricks Runtime 17.1+ has native Excel support.
    # DBR 17.3 is therefore used here instead of the external
    # com.crealytics.spark.excel datasource.
    df = (
        spark.read
        .format("excel")
        .option("headerRows", 1)
        .load(volume_file_path)
    )

    logger.info(f"Excel columns: {df.columns}")
    logger.info(f"Excel row count: {df.count()}")

    required_pnl_columns = [
        "Firm Name",
        "Account Type",
        "Stock Loan P&L (CAD)",
        "Adj Interest (CAD)",
        "Commission (CAD)",
        "Custody Fee No Tax",
        "Total Transaction Fee",
        "Option_GiveUps",
        "PB PnL (CAD)",
        "Pct",
        "Running Pct",
        "AVG Equity (CAD)",
        "ROE",
        "PnL (CAD)",
    ]
    missing = [c for c in required_pnl_columns if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required PNL columns in {volume_file_path}: {missing}. "
            f"Available columns: {df.columns}"
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
        scale = 4 if column_name == "ROE" else 2
        cleaned = func.trim(
            func.regexp_replace(
                func.col(column_name).cast("string"),
                regex_replace_symbols,
                "",
            )
        )
        df = df.withColumn(
            column_name,
            func.when(
                cleaned.isNull() | (cleaned == ""),
                func.lit(None).cast(DecimalType(20, scale)),
            ).otherwise(
                cleaned.cast(DecimalType(20, scale))
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

def remove_trades_not_present(
    pnl_summary_delta_table: DeltaTable,
    frames: list[DataFrame],
):
    if not frames:
        return

    df = frames[0]

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

# COMMAND ----------

raw_cad_prime_pnl_files: list[CADPrimeRow] = (
    unprocessed_raw_cad_prime_records.collect()
)

logger.info(f"PNL files selected for processing: {len(raw_cad_prime_pnl_files)}")

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
failure_details: list[str] = []

for pnl_file in raw_cad_prime_pnl_files:
    try:
        processed_df = process_pnl_file(
            pnl_file,
            pnl_summary_delta_table,
            field_setters,
            REGEX_REPLACE_SYMBOLS_FOR_DECIMAL_CASTING,
        )
        frames.append(processed_df)
        logger.info(f"SUCCESS: {pnl_file['file_name']}")
    except Exception as exc:
        # Keep the tracker row for the failed file so the next run can retry it.
        # Do NOT hide the real exception behind a generic RuntimeError.
        error_text = f"{type(exc).__name__}: {exc}"
        logger.error(
            f"FAILED: {pnl_file['file_name']} | "
            f"path={pnl_file['file_path']} | {error_text}",
            exc_info=True,
        )
        failed_files.append(pnl_file["file_path"])
        failure_details.append(
            f"{pnl_file['file_name']} -> {error_text}"
        )

# Successful files are already removed from the raw tracker inside
# process_pnl_file() immediately after their MERGE succeeds.
# Failed files remain in the tracker intentionally so they can be retried.

if failed_files:
    logger.warning(
        "Some PNL files failed. Their raw-tracker rows were intentionally "
        "kept for retry. Successful files were processed normally."
    )

    # Do not run remove_trades_not_present() against a partial set of source
    # files. If one file for a date failed, using only the successful files
    # could incorrectly delete valid Bronze rows belonging to the failed file.
    logger.warning(
        "Skipping stale-trade cleanup because at least one PNL file failed."
    )

    raise RuntimeError(
        "PNL processing failed. The line below is only the final summary; "
        "the real errors are listed below. Failed tracker rows were kept "
        "for retry.\n\n"
        + "\n".join(failure_details)
    )

# Only perform stale-trade cleanup when ALL selected PNL files succeeded.
remove_trades_not_present(
    pnl_summary_delta_table,
    frames,
)

logger.info(
    f"Cleaning up {BRONZE_PRIME_TABLE_NAME} where firm_name is null"
)

spark.sql(
    f"""
    DELETE FROM {BRONZE_PRIME_TABLE_NAME}
    WHERE FIRM_NAME IS NULL
    """
)

logger.info(
    f"PNL processing complete. Files processed: {len(frames)}"
)

# COMMAND ----------

raw_cad_prime_pnl_accounts: list[CADPrimeRow] = (
    unprocessed_raw_cad_prime_accounts.collect()
)

logger.info(
    f"Account files selected for processing: {len(raw_cad_prime_pnl_accounts)}"
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
    try:
        logger.info(f"Reading accounts file: {accounts_file['file_path']}")

        df = (
            spark.read
            .format("excel")
            .option("headerRows", 1)
            .load(accounts_file["file_path"])
        )

        required_account_columns = [
            "name",
            "firm",
            "ism_id",
            "ism_omnibus_id",
            "Status",
        ]
        missing = [c for c in required_account_columns if c not in df.columns]
        if missing:
            raise ValueError(
                f"Missing required account columns: {missing}. "
                f"Available columns: {df.columns}"
            )

        df = df.select(*required_account_columns).withColumn(
            "processing_time",
            func.current_timestamp(),
        )

        row_count = df.count()
        logger.info(
            f"Account Excel rows read: {row_count}; columns: {df.columns}"
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
            "SUCCESS: account file removed from raw tracker: "
            f"{accounts_file['file_name']}"
        )

    except Exception as exc:
        logger.error(
            f"FAILED account file: {accounts_file['file_name']} | "
            f"path={accounts_file['file_path']} | error={exc}",
            exc_info=True,
        )
        raise RuntimeError(
            f"Account processing failed for {accounts_file['file_name']}"
        ) from exc

logger.info("CAD Prime Bronze notebook completed successfully.")

