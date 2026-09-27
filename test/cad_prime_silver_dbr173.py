# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Reconstructed from the supplied notebook screenshots.
# Only cv_post_processing_commit contains the merge-source patch.
# Import as a Databricks notebook, not as a standalone Python job/script:
# the %run cells below require the existing companion notebooks.
# Keep CATALOG_NAME configured in the compute environment as in the original.
#
# IMPORTANT: Conflicting duplicate updates still fail explicitly. This version
# does not alter window calculations, select an arbitrary row, or change keys.
# A previous bronze merge may already have committed before the CV failure;
# a retry with no new CDF changes may not recalculate the failed fiscal year.
# The original fiscal_year_override parsing is retained, but it was not wired
# into processing in the supplied notebook and remains unused here.

import os
import logging
from typing import Optional
from collections import namedtuple
from datetime import datetime, timedelta, date
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, FloatType, DateType,
    BooleanType,
)
from pyspark.sql import Window, WindowSpec
from pyspark.sql import Row, DataFrame
from delta.tables import DeltaTable

# COMMAND ----------

# dbutils.widgets.text("CATALOG", "")

# COMMAND ----------

# CATALOG_NAME = dbutils.widgets.get("CATALOG")
CATALOG_NAME = os.getenv("CATALOG_NAME")

# COMMAND ----------

spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

dbutils.widgets.text("fiscal_year_override", "")

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./dateutils.py"

# COMMAND ----------

# MAGIC %run "./delta_table_utils_uc.py"

# COMMAND ----------

BRONZE_PRIME_TABLE_NAME = f"`{CATALOG_NAME}`.`bronze`.`cad_prime_pnl_summary`"
BRONZE_PRIME_ACCTS_TABLE_NAME = f"`{CATALOG_NAME}`.`bronze`.`cad_prime_accts`"
SILVER_PRIME_TABLE_NAME = f"`{CATALOG_NAME}`.`silver`.`cad_prime_pnl_summary`"

# COMMAND ----------

SILVER_CONDITION = """
(
    silver.client_display_name = bronze.C360_Firm_Name
    and silver.trade_date = bronze.Trade_Date
)
"""

# COMMAND ----------

UPDATE_CONDITION = """
trim(lower(silver.account_type)) != trim(lower(bronze.Account_Type))
or round(coalesce(silver.pb_pnl_cad, 0), 2) != round(coalesce(bronze.PB_PnL_CAD, 0), 2)
or round(coalesce(silver.stock_loan_p_and_l_cad, 0), 2) != round(coalesce(bronze.`Stock_Loan_P&L_CAD`, 0), 2)
or round(coalesce(silver.adj_interest_cad, 0), 2) != round(coalesce(bronze.Adj_Interest_CAD, 0), 2)
or round(coalesce(silver.commission_cad, 0), 2) != round(coalesce(bronze.Commission_CAD, 0), 2)
or round(coalesce(silver.custody_fee_no_tax, 0), 2) != round(coalesce(bronze.Custody_Fee_No_Tax, 0), 2)
or round(coalesce(silver.total_transaction_fee, 0), 2) != round(coalesce(bronze.Total_Transaction_Fee, 0), 2)
or round(coalesce(silver.option_giveups, 0), 2) != round(coalesce(bronze.Option_GiveUps, 0), 2)
or round(coalesce(silver.pct, 0), 2) != round(coalesce(bronze.Pct, 0), 2)
or round(coalesce(silver.running_pct, 0), 2) != round(coalesce(bronze.Running_Pct, 0), 2)
or round(coalesce(silver.avg_equity_cad, 0), 2) != round(coalesce(bronze.AVG_Equity_CAD, 0), 2)
or round(coalesce(silver.roe, 0), 2) != round(coalesce(bronze.ROE, 0), 2)
or round(coalesce(silver.pnl_cad, 0), 2) != round(coalesce(bronze.PnL_CAD, 0), 2)
"""

# COMMAND ----------

CLIENT_HEDGE = "CLIENT_HEDGE"
CLIENT_LENDING = "CLIENT_LENDING"
PRIME_BROKERAGE = "Prime Brokerage"
SECURITY_LENDING = "Securities Lending"
DEFAULT_FUTURE_DATE = "5000-01-01"

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional variable setting

# COMMAND ----------

TRADE_DATE_COLUMN = "trade_date"
CLIENT_VALUE_COLUMN = "pb_pnl_cad"

# COMMAND ----------

FISCAL_YEAR_OVERRIDE = (
    int(dbutils.widgets.get("fiscal_year_override"))
    if dbutils.widgets.get("fiscal_year_override")
    else -1
)

# COMMAND ----------

FailedDates = namedtuple(
    "FailedDates", ["fiscal_year", "trade_cv_date", "previous_cv_date"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create silver CAD Prime table if does not exist (managed table)

# COMMAND ----------

logger.info(f"Creating table {SILVER_PRIME_TABLE_NAME} if required...")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_PRIME_TABLE_NAME} (
        `trade_id` STRING,
        `trade_date` DATE,
        `client_display_name` STRING,
        `client_value` DECIMAL(20,2),
        `client_value_src` STRING DEFAULT 'CAD',
        `trade_currency` STRING DEFAULT 'CAD',
        `trade_system` STRING DEFAULT 'CAD_PRIME',
        `business_name` STRING DEFAULT 'Prime Brokerage',
        `desk_name` STRING DEFAULT 'Canadian Prime Brokerage',
        `product_class` STRING DEFAULT 'Prime',
        `product_class_type` STRING,
        `fiscal_year` INT,
        `account_type` STRING,
        `stock_loan_p_and_l_cad` DECIMAL(20,2),
        `adj_interest_cad` DECIMAL(20,2),
        `commission_cad` DECIMAL(20,2),
        `custody_fee_no_tax` DECIMAL(20,2),
        `total_transaction_fee` DECIMAL(20,2),
        `option_giveups` DECIMAL(20,2),
        `pct` DECIMAL(20,2),
        `running_pct` DECIMAL(20,2),
        `avg_equity_cad` DECIMAL(20,2),
        `roe` DECIMAL(20,4),
        `pb_pnl_cad` DECIMAL(20,2),
        `pnl_cad` DECIMAL(20,2),
        `processing_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        `update_time` TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.feature.allowColumnDefaults' = 'supported'
    )
    PARTITIONED BY (fiscal_year)
""")

# COMMAND ----------

logger.info(
    f"Altering table {SILVER_PRIME_TABLE_NAME} to add column "
    "`counterparty_code` if it does not exist..."
)

# COMMAND ----------

columns = [
    row["col_name"]
    for row in spark.sql(f"DESCRIBE TABLE {SILVER_PRIME_TABLE_NAME}")
    .select("col_name").collect()
]
print(columns)

# COMMAND ----------

if "counterparty_code" not in columns:
    spark.sql(f"""ALTER TABLE {SILVER_PRIME_TABLE_NAME}
        ADD COLUMNS (counterparty_code STRING)""")
if "file_name_path" not in columns:
    spark.sql(f"""ALTER TABLE {SILVER_PRIME_TABLE_NAME}
        ADD COLUMNS (file_name_path STRING)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weekend check method

# COMMAND ----------

def can_skip(current_date: date, previous_date: date) -> bool:
    delta = timedelta(days=1)
    last_date = current_date - delta
    while last_date != previous_date:
        is_weekend = last_date.weekday() in (5, 6)
        if is_weekend:
            last_date -= delta
        else:
            return False
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## CV Calculation functions

# COMMAND ----------

def calculate_client_value(
    silver_delta_df: DataFrame, fiscal_year: int, window_spec: WindowSpec
) -> DataFrame:
    logger.info(f"Processing CV for fiscal year {fiscal_year}")
    cv_df = silver_delta_df.filter(F.col("fiscal_year") == fiscal_year)
    cv_df = cv_df.withColumn(
        "date_diff",
        F.date_diff(
            F.col(TRADE_DATE_COLUMN),
            F.lag(F.col(TRADE_DATE_COLUMN), 1, DEFAULT_FUTURE_DATE).over(
                window=window_spec
            ),
        ),
    )
    cv_df = cv_df.withColumn("current_date", F.col(TRADE_DATE_COLUMN))
    cv_df = cv_df.withColumn(
        "current_day_of_week", F.date_format(F.col(TRADE_DATE_COLUMN), "EEEE")
    )
    cv_df = cv_df.withColumn(
        "previous_date",
        F.lag(F.col(TRADE_DATE_COLUMN), 1, DEFAULT_FUTURE_DATE).over(
            window=window_spec
        ),
    )
    cv_df = cv_df.withColumn(
        "previous_date_day_of_week",
        F.date_format(
            F.lag(F.col(TRADE_DATE_COLUMN), 1, DEFAULT_FUTURE_DATE).over(
                window=window_spec
            ),
            "EEEE",
        ),
    )
    cv_df = cv_df.withColumn(
        "client_value",
        F.col(CLIENT_VALUE_COLUMN)
        - F.lag(F.col(CLIENT_VALUE_COLUMN), 1, 0).over(window=window_spec),
    )
    cv_df = cv_df.withColumn("client_value_src", F.lit("CAD"))
    cv_df = cv_df.withColumn("trade_currency", F.lit("CAD"))
    cv_df = cv_df.withColumn(
        "product_class_type",
        F.when(F.upper(F.col("account_type")) == CLIENT_HEDGE, PRIME_BROKERAGE)
        .when(F.upper(F.col("account_type")) == CLIENT_LENDING, SECURITY_LENDING)
        .otherwise(""),
    )
    return cv_df

# COMMAND ----------

@enhanced_errors()
def process_cv(
    silver_delta_table: DeltaTable, fiscal_years_to_process: set[int]
) -> tuple[list[DataFrame], set[int]]:
    if not fiscal_years_to_process:
        return [], set()
    silver_delta_df = silver_delta_table.toDF()
    window_spec = Window.partitionBy("client_display_name").orderBy(
        TRADE_DATE_COLUMN
    )
    cv_dfs_processed: list[DataFrame] = []
    for fiscal_year in fiscal_years_to_process:
        cv_dfs_processed.append(
            calculate_client_value(silver_delta_df, fiscal_year, window_spec)
        )
    return cv_dfs_processed, set()

# COMMAND ----------

@enhanced_errors()
def cv_post_processing_commit(
    silver_delta_table: DeltaTable,
    cv_dfs_processed: list[DataFrame],
    failed_years: set[int],
) -> None:
    if not cv_dfs_processed:
        return

    logger.info("Gathering successfully processed fiscal years...")
    successfully_processed = [cv_df for cv_df in cv_dfs_processed]

    if failed_years:
        logger.info(
            "Removing failing fiscal years from set of successfully processed "
            f"fiscal years: {failed_years}"
        )
        successfully_processed = [
            cv_df
            for cv_df in cv_dfs_processed
            if not cv_df.filter(F.col("fiscal_year").isin(failed_years)).collect()
        ]

    union_df = successfully_processed[0] if successfully_processed else None
    for index, processed in enumerate(successfully_processed):
        if index != 0:
            union_df = union_df.union(processed)

    if union_df is None:
        return

    merge_keys = ["client_display_name", "trade_date"]
    update_columns = [
        "client_value",
        "client_value_src",
        "trade_currency",
        "pb_pnl_cad",
        "product_class_type",
    ]

    # Distinct across BOTH keys AND every updated value. Never deduplicate by
    # merge_keys alone: doing that could silently choose a different CV value.
    merge_source = union_df.select(*(merge_keys + update_columns)).distinct()

    # Validate only keys that can match the target under the ORIGINAL equality
    # predicate. Unmatched/null keys cannot update anything and remain unchanged.
    target_keys = silver_delta_table.toDF().select(*merge_keys).distinct()
    matching_source = merge_source.join(target_keys, on=merge_keys, how="left_semi")
    conflicting_keys = (
        matching_source.groupBy(*merge_keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(10)
        .collect()
    )
    if conflicting_keys:
        samples = [row.asDict() for row in conflicting_keys]
        raise ValueError(
            "CV_MERGE_CONFLICT: multiple different update payloads exist for "
            "the same (client_display_name, trade_date). Identical payloads "
            "have already been removed. This function has not updated silver. "
            "Resolving these remaining rows requires correcting the input or "
            "confirming a business rule; no first/latest/summed value was chosen. "
            f"Up to 10 conflicting keys and distinct payload counts: {samples}"
        )

    logger.info("Merging interim DataFrame into silver to update client value...")
    (
        silver_delta_table.alias("silver_old")
        .merge(
            merge_source.alias("processed_silver"),
            "silver_old.client_display_name = "
            "processed_silver.client_display_name and "
            "silver_old.trade_date = processed_silver.trade_date",
        )
        .whenMatchedUpdate(
            set={
                "client_value": F.col("processed_silver.client_value"),
                "client_value_src": F.col("processed_silver.client_value_src"),
                "trade_currency": F.col("processed_silver.trade_currency"),
                "pb_pnl_cad": F.col("processed_silver.pb_pnl_cad"),
                "product_class_type": F.col("processed_silver.product_class_type"),
            }
        )
        .execute()
    )
    logger.info("Silver merge for client value done...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy PNL CAD Prime Bronze to CAD Prime Silver

# COMMAND ----------

with capture_errors(section="cad_prime_bronze_to_silver_cv_processing"):
    logger.info(
        "Kicking off Bronze-to-Silver client value processing for CAD Prime "
        "dailies"
    )
    bronze_cad_table = spark.table(BRONZE_PRIME_TABLE_NAME)
    field_setters = {
        "client_display_name": F.col("bronze.C360_Firm_Name"),
        "account_type": F.col("bronze.Account_Type"),
        "stock_loan_p_and_l_cad": F.col("bronze.`Stock_Loan_P&L_CAD`"),
        "adj_interest_cad": F.col("bronze.Adj_Interest_CAD"),
        "commission_cad": F.col("bronze.Commission_CAD"),
        "custody_fee_no_tax": F.col("bronze.Custody_Fee_No_Tax"),
        "total_transaction_fee": F.col("bronze.Total_Transaction_Fee"),
        "option_giveups": F.col("bronze.Option_GiveUps"),
        "pb_pnl_cad": F.col("bronze.PB_PnL_CAD"),
        "pct": F.col("bronze.Pct"),
        "running_pct": F.col("bronze.Running_Pct"),
        "avg_equity_cad": F.col("bronze.AVG_Equity_CAD"),
        "roe": F.col("bronze.ROE"),
        "pnl_cad": F.col("bronze.PnL_CAD"),
        "trade_date": F.col("bronze.Trade_Date"),
        "fiscal_year": F.col("bronze.Fiscal_Year"),
        "update_time": F.lit(datetime.now()),
        "file_name_path": F.col("bronze.Source_File"),
    }
    with perform_delta(
        SILVER_PRIME_TABLE_NAME,
        bronze_cad_table,
        field_setters=field_setters,
        outer_alias_name="silver",
        inner_alias_name="bronze",
        condition_for_matched=SILVER_CONDITION,
        when_matched_update_condition=UPDATE_CONDITION,
        delete_if_source_not_matched=True,
    ) as silver_delta_table:
        logger.info("Getting Delta changes between bronze and silver...")
        delta_row_changes_rows: list[Row] = get_row_changes(
            silver_delta_table,
            SILVER_PRIME_TABLE_NAME,
            change_types=["insert", "update", "delete"],
            select_columns=["fiscal_year", "client_display_name", "trade_date"],
            with_post_image=True,
        )
        fiscal_years_to_process = set(
            [r.fiscal_year for r in delta_row_changes_rows]
        )
        logger.info(f"Processing CV for fiscal years: {fiscal_years_to_process}")
        cv_dfs_processed, failed_years = process_cv(
            silver_delta_table, fiscal_years_to_process
        )
        logger.info(
            f"Post processing CV.. Failed fiscal years encountered: {failed_years}"
        )
        cv_post_processing_commit(
            silver_delta_table, cv_dfs_processed, failed_years
        )

# COMMAND ----------

# Update the silver table to ensure counterparty_code is updated from the
# bronze accounts table where the firm name matches. Original logic retained.
spark.sql(f"""
    MERGE INTO {SILVER_PRIME_TABLE_NAME} AS silver
    USING (
        SELECT
            firm,
            MIN(ism_id) AS min_ism_id
        FROM {BRONZE_PRIME_ACCTS_TABLE_NAME}
        GROUP BY firm
    ) AS bronze_accts
    ON lower(silver.client_display_name) = lower(bronze_accts.firm)
    WHEN MATCHED
    AND (
        silver.counterparty_code IS NULL
        OR NOT (
            silver.counterparty_code LIKE '5%'
            AND silver.counterparty_code RLIKE '^[A-Za-z0-9]{{6}}$'
        )
    )
    THEN UPDATE SET
        silver.counterparty_code = bronze_accts.min_ism_id
""")
