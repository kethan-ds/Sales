# Databricks notebook source
import logging
import pyspark.sql.functions as F
from delta import DeltaTable

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    handlers=[
        logging.StreamHandler()  # Output logs to the console
    ]
)

logger = logging.getLogger(__name__)

# COMMAND ----------

BRONZE_WRA_TABLE = f"`{CATALOG_NAME}`.`bronze`.`td_cowen_wra`"
SILVER_WRA_TABLE = f"`{CATALOG_NAME}`.`silver`.`td_cowen_wra`"

# COMMAND ----------

logger.info(f"Creating table {SILVER_WRA_TABLE} if required...")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_WRA_TABLE} (
    source_system STRING DEFAULT 'COWEN',
    trade_date DATE,
    counterparty_code STRING,
    raw_client_name STRING,
    product_level_0 STRING DEFAULT 'Cowen - Equities',
    product_level_1 STRING DEFAULT 'Affiliate',
    product_level_2 STRING DEFAULT 'WRA',
    product_level_3 STRING DEFAULT 'WRA',
    client_value_usd DECIMAL(18,2),
    trade_currency STRING,
    is_client_facing STRING,
    file_name_path STRING,
    ingestion_timestamp TIMESTAMP
) USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.feature.allowColumnDefaults' = 'supported'
    )
""")

# COMMAND ----------

logger.info("Merging bronze to silver...")

bronze_wra = spark.table(BRONZE_WRA_TABLE)
silver_wra = DeltaTable.forName(spark, SILVER_WRA_TABLE)

rename_columns = {
    'TradeDate': 'trade_date',
    'ClientIdty': 'counterparty_code',
    'ClientName': 'raw_client_name',
    'NetToFirm_USD': 'client_value_usd',
}

bronze_wra = (
    bronze_wra.withColumnsRenamed(rename_columns)
    .withColumn("source_system", F.lit("COWEN"))
    .withColumn("product_level_0", F.lit("Cowen - Equities"))
    .withColumn("product_level_1", F.lit("Affiliate"))
    .withColumn("product_level_2", F.lit("WRA"))
    .withColumn("product_level_3", F.lit("WRA"))
    .withColumn("trade_currency", F.lit("USD"))
    .withColumn("is_client_facing", F.lit("1"))
)

(
    silver_wra.alias("silver")
    .merge(
        bronze_wra.alias("bronze"),
        "silver.trade_date <=> bronze.trade_date AND silver.counterparty_code <=> bronze.counterparty_code"
    )
    .whenMatchedUpdate(
        condition=(
            (~F.col("silver.raw_client_name").eqNullSafe(F.col("bronze.raw_client_name"))) |
            (~F.col("silver.client_value_usd").eqNullSafe(F.col("bronze.client_value_usd")))
        ),
        set={
            "raw_client_name": "bronze.raw_client_name",
            "client_value_usd": "bronze.client_value_usd",
            "file_name_path": "bronze.file_name_path",
            "ingestion_timestamp": "bronze.ingestion_timestamp",
            "trade_currency": "bronze.trade_currency",
            "is_client_facing": "bronze.is_client_facing"
        }
    )
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
)
