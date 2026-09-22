# Databricks notebook source
import logging
import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql import DataFrame
from functools import reduce
from pyspark.sql import Window

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

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

BRONZE_PRISM_PRODUCT_MAPPINGS = f"`{CATALOG_NAME}`.`bronze`.`prism_product_mappings`"
SILVER_PRISM_PRODUCT_MAPPINGS = f"`{CATALOG_NAME}`.`silver`.`prism_product_mappings`"

# COMMAND ----------

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

AUDIT_COLUMNS = ["file_name_path", "ingestion_timestamp"]

# Tokens that should be treated as real NULL (compared case-insensitively)
NULL_LIKE_TOKENS = ["", "null", "na", "n/a"]

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
    """Apply whitespace cleanup and null normalization to all columns
    except the excluded (audit) ones."""
    return df.select(*[
        clean_string_value(c) if c not in exclude else F.col(c)
        for c in df.columns
    ])

# COMMAND ----------

UNMAPPED_PRODUCT = "Unmapped Product"

STANDARDIZE_COLUMNS = [
    "product_level_0",
    "product_level_1",
    "product_level_2",
    "product_level_3",
]

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

NOT_NULL_COLUMNS = [
    "source_system",
    "in_out",
    "prism_product_level_0",
    "prism_product_level_1",
    "prism_product_level_2",
    "prism_product_level_3",
]

def filter_null_required_columns(df: DataFrame, columns: list[str] = NOT_NULL_COLUMNS) -> DataFrame:
    """Drop rows that have a NULL in at least one required column.
    Logs a warning with the count and displays the records being filtered out."""
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


def drop_duplicate_keys(df: DataFrame, keys: list[str] = DEDUP_KEY_COLUMNS) -> DataFrame:
    """Remove rows that are duplicates on the key columns, keeping the most recent
    by ingestion_timestamp. Logs a warning with the count and displays the dropped rows."""
    window = Window.partitionBy(*keys).orderBy(F.col("ingestion_timestamp").desc_nulls_last())

    ranked = df.withColumn("_row_num", F.row_number().over(window))

    duplicate_records = ranked.filter(F.col("_row_num") > 1).drop("_row_num")
    duplicate_count = duplicate_records.count()

    if duplicate_count > 0:
        logger.warning(
            f"Dropping {duplicate_count} duplicate record(s) on keys: {keys}"
        )
        duplicate_records.display()

    return ranked.filter(F.col("_row_num") == 1).drop("_row_num")

# COMMAND ----------

logger.info(f"Merging bronze to silver...")

bronze_prism = spark.table(BRONZE_PRISM_PRODUCT_MAPPINGS)
bronze_prism = clean_string_columns(bronze_prism)
bronze_prism = standardize_product_columns(bronze_prism)
bronze_prism = filter_null_required_columns(bronze_prism)
bronze_prism = drop_duplicate_keys(bronze_prism)
bronze_prism = bronze_prism.withColumn("in_out", F.lower(F.col("in_out")))

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
            (~F.col("silver.in_out").eqNullSafe(F.col("bronze.in_out"))) |
            (~F.col("silver.prism_product_level_0").eqNullSafe(F.col("bronze.prism_product_level_0"))) |
            (~F.col("silver.prism_product_level_1").eqNullSafe(F.col("bronze.prism_product_level_1"))) |
            (~F.col("silver.prism_product_level_2").eqNullSafe(F.col("bronze.prism_product_level_2"))) |
            (~F.col("silver.prism_product_level_3").eqNullSafe(F.col("bronze.prism_product_level_3")))
        ),
        set={
            "in_out": "bronze.in_out",
            "prism_product_level_0": "bronze.prism_product_level_0",
            "prism_product_level_1": "bronze.prism_product_level_1",
            "prism_product_level_2": "bronze.prism_product_level_2",
            "prism_product_level_3": "bronze.prism_product_level_3",
            "file_name_path": "bronze.file_name_path",
            "ingestion_timestamp": "bronze.ingestion_timestamp",
        }
    )
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
)
