# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver - Prism Product Mappings (Unity Catalog)
# MAGIC Cleans, standardises, validates and de-duplicates `bronze.prism_product_mappings`, then MERGEs it into `silver.prism_product_mappings`. Silver mirrors bronze: rows no longer present in bronze are deleted (`whenNotMatchedBySourceDelete`).

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
from functools import reduce

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window

# COMMAND ----------

logger = logging.getLogger("silver_prism_product_mappings")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logger.addHandler(handler)
logger.propagate = False


# COMMAND ----------

# DBTITLE 1,Configuration
BRONZE_PRISM_PRODUCT_MAPPINGS = f"`{CATALOG_NAME}`.bronze.prism_product_mappings"
SILVER_PRISM_PRODUCT_MAPPINGS = f"`{CATALOG_NAME}`.silver.prism_product_mappings"

logger.info(f"Catalog       : {CATALOG_NAME}")
logger.info(f"Bronze table  : {BRONZE_PRISM_PRODUCT_MAPPINGS}")
logger.info(f"Silver table  : {SILVER_PRISM_PRODUCT_MAPPINGS}")

# COMMAND ----------

# DBTITLE 1,Create silver table (managed)
logger.info(f"Creating table {SILVER_PRISM_PRODUCT_MAPPINGS} if required...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_PRISM_PRODUCT_MAPPINGS} (
        source_system STRING NOT NULL,
        product_level_0 STRING,
        product_level_1 STRING,
        product_level_2 STRING,
        product_level_3 STRING,
        in_out STRING NOT NULL,
        prism_product_level_0 STRING NOT NULL,
        prism_product_level_1 STRING NOT NULL,
        prism_product_level_2 STRING NOT NULL,
        prism_product_level_3 STRING NOT NULL,
        -- audit columns
        file_name_path STRING,
        ingestion_timestamp TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true'
    )
""")

# COMMAND ----------

# DBTITLE 1,Cleaning helpers
AUDIT_COLUMNS = ["file_name_path", "ingestion_timestamp"]

# Tokens that should be treated as real NULL (compared case-insensitively)
NULL_LIKE_TOKENS = ["", "null", "na", "n/a"]

# COMMAND ----------

def clean_string_value(column: str):
    """Trim, collapse internal whitespace runs to a single space,
    and convert empty/null-like tokens to real NULL."""
    cleaned = F.trim(F.regexp_replace(F.col(column), r"\s+", " "))
    return (
        F.when(F.lower(cleaned).isin(NULL_LIKE_TOKENS), F.lit(None))
        .otherwise(cleaned)
        .alias(column)
    )


def clean_string_columns(df: DataFrame, exclude: list[str] = AUDIT_COLUMNS) -> DataFrame:
    """Apply whitespace cleanup and null normalisation to all non-audit columns."""
    return df.select(*[
        clean_string_value(c) if c not in exclude else F.col(c)
        for c in df.columns
    ])

# COMMAND ----------

# DBTITLE 1,Standardisation helpers
UNMAPPED_PRODUCT = "Unmapped Product"

STANDARDIZE_COLUMNS = [
    "product_level_0",
    "product_level_1",
    "product_level_2",
    "product_level_3",
]

# COMMAND ----------

def standardize_product_columns(df: DataFrame, columns: list[str] = STANDARDIZE_COLUMNS) -> DataFrame:
    """Map NULL or any casing of 'Unmapped Product' to the exact string 'Unmapped Product'
    for the given columns; all other columns pass through unchanged."""
    return df.select(*[
        F.when(
            F.col(c).isNull() | (F.lower(F.col(c)) == UNMAPPED_PRODUCT.lower()),
            F.lit(UNMAPPED_PRODUCT),
        )
        .otherwise(F.col(c))
        .alias(c)
        if c in columns
        else F.col(c)
        for c in df.columns
    ])

# COMMAND ----------

# DBTITLE 1,Validation helpers
NOT_NULL_COLUMNS = [
    "source_system",
    "in_out",
    "prism_product_level_0",
    "prism_product_level_1",
    "prism_product_level_2",
    "prism_product_level_3",
]


# COMMAND ----------

def filter_null_required_columns(df: DataFrame, columns: list[str] = NOT_NULL_COLUMNS) -> DataFrame:
    """Drop rows with a NULL in at least one required column.
    Logs a warning with the count and displays the filtered records."""
    null_condition = reduce(
        lambda a, b: a | b,
        [F.col(c).isNull() for c in columns],
    )

    invalid_records = df.filter(null_condition)
    invalid_count = invalid_records.count()

    if invalid_count > 0:
        logger.warning(
            f"Filtering out {invalid_count} record(s) with a NULL in at least one of: {columns}"
        )
        invalid_records.display()

    return df.filter(~null_condition)

# COMMAND ----------

DEDUP_KEY_COLUMNS = [
    "source_system",
    "product_level_0",
    "product_level_1",
    "product_level_2",
    "product_level_3",
]

# COMMAND ----------

def drop_duplicate_keys(df: DataFrame, keys: list[str] = DEDUP_KEY_COLUMNS) -> DataFrame:
    """Remove rows duplicated on the key columns, keeping the most recent by
    ingestion_timestamp. Logs a warning with the count and displays dropped rows."""
    window = Window.partitionBy(*keys).orderBy(F.col("ingestion_timestamp").desc_nulls_last())

    ranked = df.withColumn("_row_num", F.row_number().over(window))

    duplicate_records = ranked.filter(F.col("_row_num") > 1).drop("_row_num")
    duplicate_count = duplicate_records.count()

    if duplicate_count > 0:
        logger.warning(f"Dropping {duplicate_count} duplicate record(s) on keys: {keys}")
        duplicate_records.display()

    return ranked.filter(F.col("_row_num") == 1).drop("_row_num")

# COMMAND ----------

# DBTITLE 1,Prepare source
with capture_errors("silver_prism_product_mappings_prepare"):
    bronze_raw = spark.table(BRONZE_PRISM_PRODUCT_MAPPINGS)
    bronze_raw_count = bronze_raw.count()

    bronze_prism = clean_string_columns(bronze_raw)
    bronze_prism = standardize_product_columns(bronze_prism)
    bronze_prism = filter_null_required_columns(bronze_prism)
    bronze_prism = drop_duplicate_keys(bronze_prism)
    bronze_prism = bronze_prism.withColumn("in_out", F.lower(F.col("in_out")))

    bronze_prism = bronze_prism.cache()
    source_count = bronze_prism.count()

logger.info(f"Bronze rows read: {bronze_raw_count}, valid rows for merge: {source_count}")

# COMMAND ----------

# DBTITLE 1,Merge bronze into silver
# Safety guard: the MERGE deletes every silver row not present in the source.
# An empty source (e.g. freshly created, not-yet-loaded UC bronze table, or a file
# where every row failed validation) would wipe silver, so the merge is skipped.
if source_count == 0:
    logger.warning(
        f"No valid source rows (bronze rows read: {bronze_raw_count}). "
        f"Skipping MERGE to protect {SILVER_PRISM_PRODUCT_MAPPINGS} from a full delete."
    )
else:
    with capture_errors("silver_prism_product_mappings_merge"):
        logger.info("Merging bronze to silver...")

        silver_prism = DeltaTable.forName(spark, SILVER_PRISM_PRODUCT_MAPPINGS)

        (
            silver_prism.alias("silver")
            .merge(
                bronze_prism.alias("bronze"),
                "silver.source_system <=> bronze.source_system AND "
                "silver.product_level_0 <=> bronze.product_level_0 AND "
                "silver.product_level_1 <=> bronze.product_level_1 AND "
                "silver.product_level_2 <=> bronze.product_level_2 AND "
                "silver.product_level_3 <=> bronze.product_level_3"
            )
            .whenMatchedUpdate(
                condition=(
                    (~F.col("silver.in_out").eqNullSafe(F.col("bronze.in_out")))
                    | (~F.col("silver.prism_product_level_0").eqNullSafe(F.col("bronze.prism_product_level_0")))
                    | (~F.col("silver.prism_product_level_1").eqNullSafe(F.col("bronze.prism_product_level_1")))
                    | (~F.col("silver.prism_product_level_2").eqNullSafe(F.col("bronze.prism_product_level_2")))
                    | (~F.col("silver.prism_product_level_3").eqNullSafe(F.col("bronze.prism_product_level_3")))
                ),
                set={
                    "in_out": "bronze.in_out",
                    "prism_product_level_0": "bronze.prism_product_level_0",
                    "prism_product_level_1": "bronze.prism_product_level_1",
                    "prism_product_level_2": "bronze.prism_product_level_2",
                    "prism_product_level_3": "bronze.prism_product_level_3",
                    "file_name_path": "bronze.file_name_path",
                    "ingestion_timestamp": "bronze.ingestion_timestamp",
                },
            )
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )

        metrics = (
            silver_prism.history(1)
            .select("operationMetrics")
            .first()["operationMetrics"]
        )
        logger.info(
            "Merge complete: "
            f"inserted={metrics.get('numTargetRowsInserted')}, "
            f"updated={metrics.get('numTargetRowsUpdated')}, "
            f"deleted={metrics.get('numTargetRowsDeleted')}"
        )

bronze_prism.unpersist()