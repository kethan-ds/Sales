# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver - TD Cowen WRA (Unity Catalog)
# MAGIC Renames and enriches `bronze.td_cowen_wra`, then MERGEs it into `silver.td_cowen_wra` on (trade_date, counterparty_code). Silver mirrors bronze: rows no longer present in bronze are deleted (`whenNotMatchedBySourceDelete`).

# COMMAND ----------

# DBTITLE 1,Catalog context
import os

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG").strip() or os.getenv("CATALOG_NAME", "")

if not CATALOG_NAME:
    raise ValueError("CATALOG widget (or CATALOG_NAME env var) must be set.")

os.environ["CATALOG_NAME"] = CATALOG_NAME
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# DBTITLE 1,Imports and logger
import logging
import sys

import pyspark.sql.functions as F
from delta.tables import DeltaTable

logger = logging.getLogger("silver_td_cowen_wra")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

# DBTITLE 1,Configuration
BRONZE_WRA_TABLE = f"`{CATALOG_NAME}`.bronze.td_cowen_wra"
SILVER_WRA_TABLE = f"`{CATALOG_NAME}`.silver.td_cowen_wra"

logger.info(f"Catalog       : {CATALOG_NAME}")
logger.info(f"Bronze table  : {BRONZE_WRA_TABLE}")
logger.info(f"Silver table  : {SILVER_WRA_TABLE}")

# COMMAND ----------

# DBTITLE 1,Create silver table (managed)
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

# DBTITLE 1,Prepare source
RENAME_COLUMNS = {
    "TradeDate": "trade_date",
    "ClientIdty": "counterparty_code",
    "ClientName": "raw_client_name",
    "NetToFirm_USD": "client_value_usd",
}

with capture_errors("silver_td_cowen_wra_prepare"):
    bronze_wra = (
        spark.table(BRONZE_WRA_TABLE)
        .withColumnsRenamed(RENAME_COLUMNS)
        .withColumn("source_system", F.lit("COWEN"))
        .withColumn("product_level_0", F.lit("Cowen - Equities"))
        .withColumn("product_level_1", F.lit("Affiliate"))
        .withColumn("product_level_2", F.lit("WRA"))
        .withColumn("product_level_3", F.lit("WRA"))
        .withColumn("trade_currency", F.lit("USD"))
        .withColumn("is_client_facing", F.lit("1"))
        .cache()
    )
    source_count = bronze_wra.count()

logger.info(f"Bronze rows for merge: {source_count}")

# COMMAND ----------

# DBTITLE 1,Merge bronze into silver
# Safety guard: the MERGE deletes every silver row not present in bronze.
# An empty bronze (e.g. freshly created, not-yet-seeded UC table) would wipe
# silver, so the merge is skipped.
if source_count == 0:
    logger.warning(
        f"{BRONZE_WRA_TABLE} is empty. Skipping MERGE to protect "
        f"{SILVER_WRA_TABLE} from a full delete."
    )
else:
    with capture_errors("silver_td_cowen_wra_merge"):
        logger.info("Merging bronze to silver...")

        silver_wra = DeltaTable.forName(spark, SILVER_WRA_TABLE)

        (
            silver_wra.alias("silver")
            .merge(
                bronze_wra.alias("bronze"),
                "silver.trade_date <=> bronze.trade_date AND "
                "silver.counterparty_code <=> bronze.counterparty_code",
            )
            .whenMatchedUpdate(
                condition=(
                    (~F.col("silver.raw_client_name").eqNullSafe(F.col("bronze.raw_client_name")))
                    | (~F.col("silver.client_value_usd").eqNullSafe(F.col("bronze.client_value_usd")))
                ),
                set={
                    "raw_client_name": "bronze.raw_client_name",
                    "client_value_usd": "bronze.client_value_usd",
                    "file_name_path": "bronze.file_name_path",
                    "ingestion_timestamp": "bronze.ingestion_timestamp",
                    "trade_currency": "bronze.trade_currency",
                    "is_client_facing": "bronze.is_client_facing",
                },
            )
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )

        metrics = silver_wra.history(1).select("operationMetrics").first()["operationMetrics"]
        logger.info(
            "Merge complete: "
            f"inserted={metrics.get('numTargetRowsInserted')}, "
            f"updated={metrics.get('numTargetRowsUpdated')}, "
            f"deleted={metrics.get('numTargetRowsDeleted')}"
        )

bronze_wra.unpersist()

# COMMAND ----------

# End of Notebook