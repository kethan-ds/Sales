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
from pyspark.sql.functions import current_timestamp, lit, regexp_replace
import re

# COMMAND ----------

dbutils.widgets.text("environment", "dev")

# COMMAND ----------

CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

env = dbutils.widgets.get("environment").lower()

X_DEALER_DELTA_TABLE = f"`{CATALOG_NAME}`.`bronze`.`x_dealer_delta`"

# COMMAND ----------

x_dealer_schema = StructType([
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("file_name_path", StringType(), True),
    StructField("TradeDate", StringType(), True),
    StructField("TradeID", StringType(), True),
    StructField("SourceSystem", StringType(), True),
    StructField("Book", StringType(), True),
    StructField("Direction", StringType(), True),
    StructField("Notional", StringType(), True),
    StructField("Price", StringType(), True),
    StructField("TradeCurrency", StringType(), True),
    StructField("securityID", StringType(), True),
    StructField("securityIDType", StringType(), True),
    StructField("SecurityDescription", StringType(), True),
    StructField("MaturityDate", StringType(), True),
    StructField("security_bondInformation_marketSectorDesc", StringType(), True),
    StructField("security_bondInformation_tdInstrumentCreditRating", StringType(), True),
    StructField("security_bondInformation_tdVolckerProdTyp", StringType(), True),
    StructField("security_bondInformation_calcTypeDesc", StringType(), True),
    StructField("security_bondInformation_issuer", StringType(), True),
    StructField("security_bondInformation_securityType", StringType(), True),
    StructField("security_bondInformation_securityType2", StringType(), True),
    StructField("Business", StringType(), True),
    StructField("Desk", StringType(), True),
    StructField("productClassName", StringType(), True),
    StructField("productClassTypeName", StringType(), True),
    StructField("ProductLevel0", StringType(), True),
    StructField("ProductLevel1", StringType(), True),
    StructField("ProductLevel2", StringType(), True),
    StructField("CounterpartyCode", StringType(), True),
    StructField("ClientDisplayName", StringType(), True),
    StructField("ClientDisplayName_TopLevel", StringType(), True),
    StructField("ClientValue_SRC", StringType(), True),
    StructField("ClientValue_CAD", DecimalType(18,5), True),
    StructField("ClientValue_USD", DecimalType(18,5), True),
    StructField("Salesperson", StringType(), True),
])

# COMMAND ----------

create_delta_table(catalog_name=CATALOG_NAME, layer="bronze", table_name="GED_NOTES", schema=x_dealer_schema, enableChangeDataFeed=False)
create_delta_table(catalog_name=CATALOG_NAME, layer="bronze", table_name="GED_SWAPS", schema=x_dealer_schema, enableChangeDataFeed=False)
create_delta_table(catalog_name=CATALOG_NAME, layer="bronze", table_name="GED_OPTIONS", schema=x_dealer_schema, enableChangeDataFeed=False)
create_delta_table(catalog_name=CATALOG_NAME, layer="bronze", table_name="high_yield", schema=x_dealer_schema, enableChangeDataFeed=False)
create_delta_table(catalog_name=CATALOG_NAME, layer="bronze", table_name="Inst_Eq", schema=x_dealer_schema, enableChangeDataFeed=False)
create_delta_table(catalog_name=CATALOG_NAME, layer="bronze", table_name="Gov_Finance", schema=x_dealer_schema, enableChangeDataFeed=False)
create_delta_table(catalog_name=CATALOG_NAME, layer="bronze", table_name="US_MUNIS_GENERAL", schema=x_dealer_schema, enableChangeDataFeed=False)
create_delta_table(catalog_name=CATALOG_NAME, layer="bronze", table_name="METAL", schema=x_dealer_schema, enableChangeDataFeed=False)
create_delta_table(catalog_name=CATALOG_NAME, layer="bronze", table_name="ENERGY", schema=x_dealer_schema, enableChangeDataFeed=False)
create_delta_table(catalog_name=CATALOG_NAME, layer="bronze", table_name="US_PRIME_GENERAL", schema=x_dealer_schema, enableChangeDataFeed=False)
create_delta_table(catalog_name=CATALOG_NAME, layer="bronze", table_name="CAD_PRIME", schema=x_dealer_schema, enableChangeDataFeed=False)
create_delta_table(catalog_name=CATALOG_NAME, layer="bronze", table_name="TDSAT", schema=x_dealer_schema, enableChangeDataFeed=False)

# COMMAND ----------

delta_table = spark.sql(f"""
    SELECT * FROM {X_DEALER_DELTA_TABLE}
    WHERE isDoneADLS = TRUE AND isDoneBronze = FALSE
    ORDER BY uploadDate
""")
adls_delta = delta_table.toPandas()

# COMMAND ----------

adls_delta

# COMMAND ----------

with capture_errors("x_dealer_bronze_load"):
    # Load to Bronze
    for index, row in adls_delta.iterrows():
        product = row["product"]
        bronze_product_path = f"`{CATALOG_NAME}`.`bronze`.`{product.lower()}`"
        year = row["year"]
        month = row["month"]
        uploadDate = row["uploadDate"]

        # Volume (FUSE) path -- used for the actual read.
        volume_raw_path = (
            f"{VOLUME_BASE_PATH}/trades/{product.lower()}/{year:04d}/{month:02d}/"
            f"m_trades_{product.lower()}_{uploadDate}.csv"
        )

        # External (ADLS) path -- used only for the file_name_path lineage column.
        external_raw_path = volume_raw_path.replace(
            VOLUME_BASE_PATH, EXTERNAL_LOCATION_BASE_PATH
        )

        raw_df = spark.read.csv(volume_raw_path, header=True)

        raw_df = raw_df.withColumn("ingestion_timestamp", current_timestamp())
        raw_df = raw_df.withColumn("file_name_path", lit(external_raw_path))

        # Assign Schema Datatypes
        for col in x_dealer_schema:
            if col.name in raw_df.columns:
                raw_df = raw_df.withColumn(
                    col.name,
                    raw_df[col.name].cast(col.dataType)
                )

        # Only Keep columns in Schema and enforce Column Order
        raw_df = raw_df.select(*[f.name for f in x_dealer_schema])
        schema = raw_df.schema.fields

        raw_df.createOrReplaceTempView("raw_temp_view")

        # NOTE: CHARINDEX is T-SQL, not Spark SQL -- replaced with locate(),
        # which takes the same (substr, str[, pos]) argument order.
        spark.sql(f"""
            DELETE FROM {bronze_product_path} as bronze
            WHERE SUBSTRING(
                bronze.file_name_path,
                locate('/trades/', bronze.file_name_path) + 8,
                locate('/m_', bronze.file_name_path, locate('/trades/', bronze.file_name_path))
                - (locate('/trades/', bronze.file_name_path) + 8)
            ) = '{product.lower()}/{year}/{month:02d}'
        """)

        spark.sql(f"""
            INSERT INTO {bronze_product_path}
            SELECT * FROM raw_temp_view
        """)

        spark.catalog.dropTempView("raw_temp_view")

        # Update delta table to indicate it's been loaded to Bronze
        spark.sql(f"""
            UPDATE {X_DEALER_DELTA_TABLE} SET isDoneBronze = True
            WHERE product = '{product}'
              AND year = '{year}'
              AND month = '{month}'
              AND uploadDate = '{uploadDate}'
        """)