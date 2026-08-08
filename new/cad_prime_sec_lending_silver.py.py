# Databricks notebook source
import logging
from collections import namedtuple
from datetime import datetime

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Row
from pyspark.sql.window import Window, WindowSpec

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./dateutils.py"

# COMMAND ----------

# MAGIC %run "./delta_table_utils.py"

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

BRONZE_PRIME_SL_TABLE_NAME = f"`{CATALOG_NAME}`.`bronze`.`cad_prime_securities_lending`"
SILVER_PRIME_SL_TABLE_NAME = f"`{CATALOG_NAME}`.`silver`.`cad_prime_securities_lending`"

# COMMAND ----------

SL_SILVER_CONDITION = """
(
    silver.raw_client_name = bronze.cptyname
    AND silver.counterparty_code = bronze.contra_party
    AND silver.trade_date = bronze.trade_date
    AND silver.client_value_src = bronze.period_client_value_sum
)
"""

# COMMAND ----------

SL_UPDATE_CONDITION = """
round(coalesce(silver.client_value_src, 0), 2)
    != round(coalesce(bronze.period_client_value_sum, 0), 2)
"""

# COMMAND ----------

SL_TRADE_DATE_COLUMN = "trade_date"
SL_CLIENT_VALUE_COLUMN = "client_value_src"

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

dbutils.widgets.text("fiscal_year_override", "")

# COMMAND ----------

fiscal_year_override_value = dbutils.widgets.get("fiscal_year_override")
FISCAL_YEAR_OVERRIDE = (
    int(fiscal_year_override_value)
    if fiscal_year_override_value
    else -1
)

FailedDates = namedtuple(
    "FailedDates",
    ["fiscal_year", "trade_cv_date", "previous_cv_date"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create silver CV securities lending table if it does not exists (managed table)

# COMMAND ----------

logger.info(
    "Creating table %s if required...",
    SILVER_PRIME_SL_TABLE_NAME,
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SILVER_PRIME_SL_TABLE_NAME} (
        counterparty_code STRING,
        raw_client_name STRING,
        client_display_name STRING,
        product_level_0 STRING DEFAULT 'Prime',
        product_level_1 STRING DEFAULT 'Canadian Prime Broker',
        product_level_2 STRING DEFAULT 'Securities Lending',
        is_client_facing BOOLEAN DEFAULT true,
        trade_date DATE,
        trade_currency STRING DEFAULT 'CAD',
        client_value DECIMAL(20, 2),
        fiscal_year INT,
        fiscal_trade_quarter INT,
        source_system STRING DEFAULT 'CAD_PRIME',
        client_value_src DECIMAL(20, 2),
        source_file STRING,
        processing_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        update_time TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.feature.allowColumnDefaults' = 'supported'
    )
    PARTITIONED BY (fiscal_year)
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CV calculation functions

# COMMAND ----------

def calculate_sl_client_value(
    silver_delta_df: DataFrame,
    fiscal_year: int,
    window_spec: WindowSpec,
) -> DataFrame:
    """
    Calculate client value as the difference from the previous day's
    period_client_value_sum for all dates in the supplied fiscal year.

    The window is partitioned by client
    (raw_client_name + counterparty_code) and ordered by trade_date.
    """
    logger.info(
        "Processing CV Securities for fiscal year %s",
        fiscal_year,
    )

    cv_df = silver_delta_df.filter(
        F.col("fiscal_year") == fiscal_year
    )

    previous_value = F.lag(
        F.col(SL_CLIENT_VALUE_COLUMN),
        1,
        0,
    ).over(window_spec)

    cv_df = (
        cv_df
        .withColumn(
            "client_value",
            F.col(SL_CLIENT_VALUE_COLUMN) - previous_value,
        )
        .withColumn(
            "trade_currency",
            F.lit("CAD"),
        )
    )

    return cv_df


# COMMAND ----------

@enhanced_errors()
def process_sl(
    silver_delta_table: DeltaTable,
    fiscal_years_to_process: set[int],
) -> list[DataFrame]:
    """
    Process client-value calculations independently for each fiscal year.

    All dates inside each fiscal year are included in the calculation.
    Returns one processed DataFrame per fiscal year.
    """
    if not fiscal_years_to_process:
        return []

    silver_delta_df = silver_delta_table.toDF()

    window_spec = (
        Window
        .partitionBy("raw_client_name", "counterparty_code")
        .orderBy(SL_TRADE_DATE_COLUMN)
    )

    processed_dataframes: list[DataFrame] = []

    for fiscal_year in fiscal_years_to_process:
        processed_dataframes.append(
            calculate_sl_client_value(
                silver_delta_df,
                fiscal_year,
                window_spec,
            )
        )

    return processed_dataframes

# COMMAND ----------

@enhanced_errors()
def sl_post_processing_commit(
    cv_dfs_processed: list[DataFrame],
    silver_delta_table: DeltaTable,
) -> None:
    """
    Union all per-fiscal-year DataFrames and merge calculated client values
    back into the silver table.
    """
    if not cv_dfs_processed:
        logger.info("No DataFrames to merge")
        return

    logger.info(
        "Gathering successfully processed fiscal years for CV Securities..."
    )

    union_df = cv_dfs_processed[0]

    for processed_df in cv_dfs_processed[1:]:
        union_df = union_df.union(processed_df)

    logger.info(
        "Merging calculated client values into silver for CV Securities..."
    )

    (
        silver_delta_table.alias("silver_old")
        .merge(
            union_df.alias("processed_silver"),
            """
            silver_old.raw_client_name =
                processed_silver.raw_client_name
            AND silver_old.counterparty_code =
                processed_silver.counterparty_code
            AND silver_old.trade_date =
                processed_silver.trade_date
            """,
        )
        .whenMatchedUpdate(
            set={
                "client_value": F.col(
                    "processed_silver.client_value"
                ),
                "trade_currency": F.col(
                    "processed_silver.trade_currency"
                ),
            }
        )
        .execute()
    )

    logger.info(
        "Silver merge for Securities Lending client value completed."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## copy CV securities lending bronze to silver

# COMMAND ----------

with capture_errors(
    section="cad_prime_cv_securities_lending_bronze_to_silver_processing"
):
    logger.info(
        "Kicking off Bronze-to-Silver client value processing "
        "for Securities Lending"
    )

    bronze_sl_table = spark.table(BRONZE_PRIME_SL_TABLE_NAME)

    bronze_sl_table = bronze_sl_table.withColumn(
        "client_display_name",
        F.col("cptyname"),
    )

    # Extract trade_date from Source_File.
    # Expected file-name date format: MM-dd-yyyy.
    bronze_sl_table = bronze_sl_table.withColumn(
        "trade_date",
        F.to_date(
            F.regexp_extract(
                F.col("Source_File"),
                r"(\d{2}-\d{2}-\d{4})",
                1,
            ),
            "MM-dd-yyyy",
        ),
    )

    # Fiscal year starts in November.
    bronze_sl_table = bronze_sl_table.withColumn(
        "fiscal_year",
        F.when(
            F.month(F.col("trade_date")) < 11,
            F.year(F.col("trade_date")),
        ).otherwise(
            F.year(F.col("trade_date")) + 1,
        ),
    )

    bronze_sl_table = bronze_sl_table.withColumn(
        "fiscal_trade_quarter",
        F.when(
            F.month(F.col("trade_date")).isin(11, 12, 1),
            1,
        )
        .when(
            F.month(F.col("trade_date")).isin(2, 3, 4),
            2,
        )
        .when(
            F.month(F.col("trade_date")).isin(5, 6, 7),
            3,
        )
        .otherwise(4),
    )

    field_setters = {
        "counterparty_code": F.col("bronze.contra_party"),
        "raw_client_name": F.col("bronze.cptyname"),
        "client_display_name": F.col("bronze.cptyname"),
        "trade_date": F.col("bronze.trade_date"),
        "fiscal_year": F.col("bronze.fiscal_year"),
        "fiscal_trade_quarter": F.col(
            "bronze.fiscal_trade_quarter"
        ),
        "client_value_src": F.col(
            "bronze.period_client_value_sum"
        ),
        "source_file": F.col("bronze.Source_File"),
        "update_time": F.lit(datetime.now()),
    }

    with perform_delta(
        SILVER_PRIME_SL_TABLE_NAME,
        bronze_sl_table,
        field_setters=field_setters,
        outer_alias_name="silver",
        inner_alias_name="bronze",
        condition_for_matched=SL_SILVER_CONDITION,
        when_matched_update_condition=SL_UPDATE_CONDITION,
        delete_if_source_not_matched=True,
    ) as sl_silver_delta_table:
        logger.info(
            "Getting Delta changes between bronze and silver "
            "for Securities Lending..."
        )

        delta_row_changes_rows: list[Row] = get_row_changes(
            sl_silver_delta_table,
            SILVER_PRIME_SL_TABLE_NAME,
            change_types=["insert", "update", "delete"],
            select_columns=[
                "fiscal_year",
                "raw_client_name",
                "counterparty_code",
                "trade_date",
                "client_value_src",
            ],
            with_post_image=True,
        )

        fiscal_years_to_process = {
            row.fiscal_year
            for row in delta_row_changes_rows
            if row.fiscal_year
        }

        if FISCAL_YEAR_OVERRIDE > 0:
            fiscal_years_to_process.add(FISCAL_YEAR_OVERRIDE)

        if not fiscal_years_to_process:
            logger.info(
                "No fiscal years to process for CV calculation"
            )
        else:
            logger.info(
                "Processing CV for Securities Lending fiscal years: %s",
                fiscal_years_to_process,
            )

            sl_dfs_processed = process_sl(
                sl_silver_delta_table,
                fiscal_years_to_process,
            )

            sl_post_processing_commit(
                sl_dfs_processed,
                sl_silver_delta_table,
            )