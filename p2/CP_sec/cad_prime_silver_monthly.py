# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

from pyspark.sql.types import *
import re
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# MAGIC %run "./delta_table_utils.py"

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

cad_prime_monthly_silver_schema = StructType([
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("file_name_path", StringType(), True),
    StructField("trade_date", StringType(), True),
    StructField("trade_id", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("book", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("volume_trade_currency", StringType(), True),
    StructField("price", DecimalType(18, 5), True),
    StructField("trade_currency", StringType(), True),
    StructField("security_id", StringType(), True),
    StructField("security_id_type", StringType(), True),
    StructField("security_description", StringType(), True),
    StructField("maturity_date", StringType(), True),
    StructField("product_level_0", StringType(), True),
    StructField("product_level_1", StringType(), True),
    StructField("product_level_2", StringType(), True),
    StructField("counterparty_code", StringType(), True),
    StructField("client_display_name", StringType(), True),
    StructField("client_display_name_top_level", StringType(), True),
    StructField("client_value_cad", DecimalType(18, 5), True),
    StructField("client_value_usd", DecimalType(18, 5), True),
    StructField("salesperson", StringType(), True),
])

# COMMAND ----------

create_delta_table(
    catalog_name=CATALOG_NAME,
    layer="silver",
    table_name="cad_prime_monthly",
    schema=cad_prime_monthly_silver_schema,
    enableChangeDataFeed=True,
)

# COMMAND ----------

column_mapping = {
    "trade_date": F.col("TradeDate"),
    "trade_id": F.col("TradeID"),
    "source_system": F.col("SourceSystem"),
    "book": F.col("Book"),
    "direction": F.col("Direction"),
    "volume_trade_currency": F.col("Notional"),
    "price": F.expr("try_cast(Price as Decimal(18,5))"),
    "trade_currency": F.col("TradeCurrency"),
    "security_id": F.col("securityID"),
    "security_id_type": F.col("securityIDType"),
    "security_description": F.col("SecurityDescription"),
    "maturity_date": F.col("MaturityDate"),
    "product_level_0": F.col("ProductLevel0"),
    "product_level_1": F.col("ProductLevel1"),
    "product_level_2": F.col("ProductLevel2"),
    "counterparty_code": F.col("CounterpartyCode"),
    "client_display_name": F.col("ClientDisplayName"),
    "client_display_name_top_level": F.col("ClientDisplayName_TopLevel"),
    "client_value_cad": F.col("ClientValue_CAD"),
    "client_value_usd": F.col("ClientValue_USD"),
    "salesperson": F.col("Salesperson"),
    "ingestion_timestamp": F.col("ingestion_timestamp"),
    "file_name_path": F.col("file_name_path"),
}

# COMMAND ----------

X_DEALER_DELTA_TABLE = f"`{CATALOG_NAME}`.`bronze`.`x_dealer_delta`"
CAD_PRIME_ACCTS_TABLE = f"`{CATALOG_NAME}`.`bronze`.`cad_prime_accts`"
SILVER_CAD_PRIME_MONTHLY_TABLE = f"`{CATALOG_NAME}`.`silver`.`cad_prime_monthly`"

delta_table = spark.sql(
    f"""
    SELECT *
    FROM {X_DEALER_DELTA_TABLE}
    WHERE isDoneADLS = TRUE
      AND isDoneBronze = TRUE
      AND isDoneSilver = FALSE
      AND product = 'CAD_PRIME'
    ORDER BY uploadDate
    """
)

# COMMAND ----------

adls_delta = delta_table.toPandas()

# COMMAND ----------

with capture_errors("cad_prime_monthly_silver_load"):
    for index, row in adls_delta.iterrows():
        product = row["product"]
        year = row["year"]
        month = row["month"]
        uploadDate = row["uploadDate"]

        bronze_table_name = f"`{CATALOG_NAME}`.`bronze`.`{product.lower()}`"

        # Lineage path -- must match how `file_name_path` was populated in
        # the bronze layer (external/ADLS path, not the Volume path).
        file_path = (
            EXTERNAL_LOCATION_BASE_PATH
            + f"raw/trades/{product.lower()}/{year}/{month:02d}/m_trades_{product.lower()}_{uploadDate}.csv"
        )

        # Read Bronze Data
        bronze_df = spark.table(bronze_table_name)

        # Trim String Values
        for col_name, col_type in bronze_df.dtypes:
            if col_type == "string":
                bronze_df = bronze_df.withColumn(
                    col_name,
                    F.trim(F.col(col_name)),
                )

        # "NA" and blank changed to Nulls
        cleaned_bronze_df = bronze_df.replace(
            ["NA", ""],
            None,
            subset=[
                col
                for col, dtype in bronze_df.dtypes
                if dtype == "string"
            ],
        )

        # Filter for the new set of trades to load
        cleaned_bronze_df = cleaned_bronze_df.filter(
            F.col("file_name_path") == file_path
        )

        # Column Mapping
        cleaned_bronze_df = cleaned_bronze_df.select(
            *[
                column_mapping[col].alias(col)
                for col in column_mapping
            ]
        )

        # Update Ingestion Timestamp
        cleaned_bronze_df = cleaned_bronze_df.withColumn(
            "ingestion_timestamp",
            F.current_timestamp(),
        )

        # Delete trades from the same Year-Month being loaded.
        # NOTE: CHARINDEX is T-SQL, not Spark SQL -- replaced with locate(),
        # which takes the same (substr, str[, pos]) argument order.
        spark.sql(
            f"""
            DELETE FROM {SILVER_CAD_PRIME_MONTHLY_TABLE} AS silver
            WHERE SUBSTRING(
                silver.file_name_path,
                locate('/trades/', silver.file_name_path) + 8,
                locate(
                    '_',
                    silver.file_name_path,
                    locate('/trades/', silver.file_name_path) + 8
                ) - (locate('/trades/', silver.file_name_path) + 8)
            ) = '{product.lower()}/{year}/{month:02d}'
            """
        )

        # Write new Bronze data to Silver (managed table -- no explicit path)
        (
            cleaned_bronze_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(SILVER_CAD_PRIME_MONTHLY_TABLE)
        )

        # Update Delta tracking table to indicate Silver load is complete
        spark.sql(
            f"""
            UPDATE {X_DEALER_DELTA_TABLE}
            SET isDoneSilver = TRUE
            WHERE product = '{product}'
              AND year = '{year}'
              AND month = '{month}'
              AND uploadDate = '{uploadDate}'
            """
        )

# COMMAND ----------

# Update counterparty_code from the bronze accounts table
# where the firm name matches.

# COMMAND ----------

spark.sql(
    f"""
    MERGE INTO {SILVER_CAD_PRIME_MONTHLY_TABLE} AS silver
    USING (
        SELECT
            firm,
            MIN(ism_id) AS min_ism_id
        FROM {CAD_PRIME_ACCTS_TABLE}
        GROUP BY firm
    ) AS bronze_accts
    ON silver.client_display_name = bronze_accts.firm

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
    """
)