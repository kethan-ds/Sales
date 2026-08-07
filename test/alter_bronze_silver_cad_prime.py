# Databricks notebook source
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

BRONZE_PRIME_TABLE_NAME = f"`{CATALOG_NAME}`.`bronze`.`cad_prime_pnl_summary`"
SILVER_PRIME_TABLE_NAME = f"`{CATALOG_NAME}`.`silver`.`cad_prime_pnl_summary`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add `c360_Firm_Name` column to bronze if it doesn't already exist (fresh catalogs already have this column from table creation)

# COMMAND ----------

bronze_columns = (
    spark.sql(f"DESCRIBE TABLE {BRONZE_PRIME_TABLE_NAME}")
    .select("col_name")
    .rdd.flatMap(lambda x: x)
    .collect()
)

# COMMAND ----------

if "c360_Firm_Name" not in bronze_columns:
    spark.sql(f"""
        ALTER TABLE {BRONZE_PRIME_TABLE_NAME}
        ADD COLUMN c360_Firm_Name STRING
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Backfill values into the bronze column for pre-existing rows

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT *
    FROM {BRONZE_PRIME_TABLE_NAME}
    """
)

# COMMAND ----------

df = df.withColumn(
    "New_Firm_Name",
    F.lower(F.trim(F.col("Firm_Name")))
)

# COMMAND ----------

delta = DeltaTable.forName(spark, BRONZE_PRIME_TABLE_NAME)

(
    delta.alias("orig")
    .merge(
        df.alias("other"),
        """
        orig.Firm_Name = other.Firm_Name
        AND orig.Trade_Date = other.Trade_Date
        """
    )
    .whenMatchedUpdate(
        set={
            "c360_Firm_Name": F.col("other.New_Firm_Name")
        }
    )
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lower case client display name for all client in silver

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT *
    FROM {SILVER_PRIME_TABLE_NAME}
    """
)

# COMMAND ----------

df = df.withColumn(
    "new_client_display_name",
    F.lower(F.trim(F.col("client_display_name")))
)

# COMMAND ----------

delta = DeltaTable.forName(spark, SILVER_PRIME_TABLE_NAME)

# COMMAND ----------

(
    delta.alias("orig")
    .merge(
        df.alias("other"),
        """
        orig.client_display_name = other.client_display_name
        AND orig.trade_date = other.trade_date
        """
    )
    .whenMatchedUpdate(
        set={
            "client_display_name": F.col("other.new_client_display_name")
        }
    )
    .execute()
)