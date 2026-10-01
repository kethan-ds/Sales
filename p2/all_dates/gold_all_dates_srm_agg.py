# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

#Create all_dates_silver table
all_dates_gold_srm_agg_schema = StructType([
    StructField("year_month", StringType(), True),
    StructField("fiscal_trade_quarter", StringType(), True),
    StructField("maturity_bucket", StringType(), True),
    StructField("volume_cad_mm", DecimalType(18,10), True),
    StructField("volume_usd_mm", DecimalType(18,10), True),
    StructField("product_level_0", StringType(), True),
    StructField("product_level_1", StringType(), True),
    StructField("product_level_2", StringType(), True),
    StructField("product_level_3", StringType(), True),
    StructField("client_display_name", StringType(), True),
    StructField("focused_account", StringType(), True),
    StructField("client_value_cad", DecimalType(18,7), True),
    StructField("client_value_usd", DecimalType(18,7), True),
])

# COMMAND ----------

# Create Table (managed table - no LOCATION clause under UC)
GOLD_ALL_DATES_SRM_AGG_TABLE = f"`{CATALOG_NAME}`.`gold`.`all_dates_srm_agg`"

_schema_ddl = ",\n".join(
    [f"`{f.name}` {f.dataType.simpleString()}" for f in all_dates_gold_srm_agg_schema.fields]
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_ALL_DATES_SRM_AGG_TABLE}
(
{_schema_ddl}
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# NOTE: this reads from `silver.all_dates_gfi_panoramic`, whose UC-compliant version
# is a separate converted notebook - confirm that notebook has been run (and the
# table populated) in this catalog before running this one.
SILVER_ALL_DATES_PATH = f"`{CATALOG_NAME}`.`silver`.`all_dates_gfi_panoramic`"


# COMMAND ----------

all_dates = spark.table(SILVER_ALL_DATES_PATH)

# COMMAND ----------

client_facing_trades = all_dates.filter(F.col("is_client_facing")=="1")

# COMMAND ----------

srm_agg = client_facing_trades.groupby('client_display_name','product_level_0','product_level_1','product_level_2','product_level_3',
    'year_month','fiscal_trade_quarter','focused_account','maturity_bucket').agg(
        F.sum("client_value_cad").alias("client_value_cad"),
        F.sum("client_value_usd").alias("client_value_usd"),
        F.sum("volume_cad_mm").alias("volume_cad_mm"),
        F.sum("volume_usd_mm").alias("volume_usd_mm"),
    )

# COMMAND ----------

@enhanced_errors()
def apply_schema_data_types(df, target_schema):
    existing_columns = set(df.columns)

    schema_dict = {field.name: field.dataType for field in target_schema.fields}

    for column_name, data_type in schema_dict.items():
        if column_name in existing_columns:
            df = df.withColumn(column_name, F.col(column_name).cast(data_type))

    return df

# COMMAND ----------

srm_agg = apply_schema_data_types(srm_agg, all_dates_gold_srm_agg_schema)

# COMMAND ----------

srm_agg.write.format("delta").mode("overwrite").saveAsTable(GOLD_ALL_DATES_SRM_AGG_TABLE)