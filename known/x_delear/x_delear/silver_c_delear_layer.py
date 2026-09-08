# Databricks notebook source
import os

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
# volume_util.py reads CATALOG_NAME via os.getenv, so the widget value must
# be pushed into the environment BEFORE it runs.
os.environ["CATALOG_NAME"] = dbutils.widgets.get("CATALOG")

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# MAGIC %run "./delta_table_utils.py"

# COMMAND ----------

from pyspark.sql.types import *
import re
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("environment", "dev")

# COMMAND ----------

X_DEALER_DELTA_TABLE = f"`{CATALOG_NAME}`.`bronze`.`x_dealer_delta`"
SILVER_X_DEALER_TABLE = f"`{CATALOG_NAME}`.`silver`.`x_dealer`"
BRONZE_IE_RR_ISM_TABLE = f"`{CATALOG_NAME}`.`bronze`.`ie_rr_ism`"

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Table with schema

# COMMAND ----------

x_dealer_silver_schema = StructType([
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("file_name_path", StringType(), True),
    StructField("trade_date", StringType(), True),
    StructField("trade_id", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("book", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("volume_trade_currency", StringType(), True),
    StructField("price", DecimalType(18,5), True),
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
    StructField("client_value_cad", DecimalType(18,5), True),
    StructField("client_value_usd", DecimalType(18,5), True),
    StructField("salesperson", StringType(), True),
])

# COMMAND ----------

# Table Name : Schema
tables_schema = {
    "x_dealer": x_dealer_silver_schema,
}

# COMMAND ----------

create_delta_table(catalog_name=CATALOG_NAME, layer="silver", table_name="x_dealer", schema=x_dealer_silver_schema, enableChangeDataFeed=False)

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

delta_table = spark.sql(f"""
    SELECT * FROM {X_DEALER_DELTA_TABLE}
    WHERE isDoneADLS = TRUE
      AND isDoneBronze = TRUE
      AND isDoneSilver = FALSE
      AND product != 'CAD_PRIME'
    ORDER BY uploadDate
""")
adls_delta = delta_table.toPandas()

# COMMAND ----------

with capture_errors("x_dealer_silver_load"):
    for index, row in adls_delta.iterrows():
        product = row["product"]
        year = row["year"]
        month = row["month"]
        uploadDate = row["uploadDate"]

        bronze_table_name = f"`{CATALOG_NAME}`.`bronze`.`{product.lower()}`"

        # Lineage path -- must match how file_name_path was populated in
        # the bronze layer (external/ADLS path, not the Volume path).
        file_path = (
            EXTERNAL_LOCATION_BASE_PATH
            + f"raw/trades/{product.lower()}/{year}/{month:02d}/"
              f"m_trades_{product.lower()}_{uploadDate}.csv"
        )

        # Read Bronze Data
        bronze_df = spark.table(bronze_table_name)

        # Filter for the set of trades I want
        cleaned_bronze_df = bronze_df.filter(
            F.col("file_name_path") == file_path
        )

        if product == "GED_SWAPS":
            cleaned_bronze_df = cleaned_bronze_df.withColumn(
                "TradeDate",
                F.lit(f"{year}-{month:02d}-01")
            )

        # Trim String Values
        for col_name, col_type in cleaned_bronze_df.dtypes:
            if col_type == "string":
                cleaned_bronze_df = cleaned_bronze_df.withColumn(
                    col_name,
                    F.trim(F.col(col_name))
                )

        # "NA" and blank changed to Nulls
        cleaned_bronze_df = cleaned_bronze_df.replace(
            ["NA", ""],
            None,
            subset=[
                col for col, dtype in cleaned_bronze_df.dtypes
                if dtype == "string"
            ]
        )

        # Column Mapping
        cleaned_bronze_df = cleaned_bronze_df.select(
            *[column_mapping[col].alias(col) for col in column_mapping]
        )

        # Update Ingestion Timestamp
        cleaned_bronze_df = cleaned_bronze_df.withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )

        # Delete any trades from the same Year-Month I'm trying to load.
        # NOTE: CHARINDEX is T-SQL, not Spark SQL -- replaced with locate(),
        # which takes the same (substr, str[, pos]) argument order.
        spark.sql(f"""
            DELETE FROM {SILVER_X_DEALER_TABLE} as silver
            WHERE SUBSTRING(
                silver.file_name_path,
                locate('/trades/', silver.file_name_path) + 8,
                locate('/m_', silver.file_name_path, locate('/trades/', silver.file_name_path))
                - (locate('/trades/', silver.file_name_path) + 8)
            ) = '{product.lower()}/{year}/{month:02d}'
        """)

        # Write New Bronze Data to Silver (managed table -- no explicit path)
        cleaned_bronze_df.write.format("delta") \
            .mode("append") \
            .saveAsTable(SILVER_X_DEALER_TABLE)

        # Update delta table to indicate it's been loaded to Silver
        spark.sql(f"""
            UPDATE {X_DEALER_DELTA_TABLE} SET isDoneSilver = True
            WHERE product = '{product}'
              AND year = '{year}'
              AND month = '{month}'
              AND uploadDate = '{uploadDate}'
        """)

# COMMAND ----------

existing_data = spark.sql(f"""
    select Counterparty_Code
    from {SILVER_X_DEALER_TABLE}
    where Source_System = 'EQUITIES'
      and Product_Level_0 = 'Institutional Equities'
      and Client_Display_Name is not null
      and len(Counterparty_Code) = 4
""").collect()

# COMMAND ----------

table_exists = spark.catalog.tableExists(BRONZE_IE_RR_ISM_TABLE)

# COMMAND ----------

if existing_data and table_exists and False:
    spark.sql(f"""
        MERGE INTO {SILVER_X_DEALER_TABLE} AS x
        USING (
            SELECT
                representative,
                MIN(acid) AS min_acid
            FROM {BRONZE_IE_RR_ISM_TABLE}
            WHERE acid IS NOT NULL
              AND acid RLIKE '^5[A-Z0-9]*[A-Z][A-Z0-9]*$'
            GROUP BY representative
            HAVING min_acid RLIKE '^5[A-Z0-9]*[A-Z][A-Z0-9]*$'
        ) AS src
        ON x.Counterparty_Code = src.representative
        AND x.Source_System = 'EQUITIES'
        AND x.Product_Level_0 = 'Institutional Equities'
        AND x.Client_Display_Name IS NOT NULL
        AND LENGTH(x.Counterparty_Code) = 4
        WHEN MATCHED THEN
        UPDATE SET
            x.Counterparty_Code = src.min_acid;
    """)