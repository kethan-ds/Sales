# Databricks notebook source
# MAGIC %run "./volume_util.py"

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

from pyspark.sql.types import (
    StructField, StructType, StringType, BooleanType, IntegerType,
)
from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup configuration (Unity Catalog Volume based)
# MAGIC
# MAGIC REMOVED vs hive_metastore version: ADLS_CONTAINER_NAME / ADLS_ACCOUNT_URI env
# MAGIC vars and the abfss:// path. ADLS_DESTINATION_PATH is now relative to the raw
# MAGIC Volume defined in volume_util.py, and must match the raw ingestion notebook's
# MAGIC destination path exactly.

# COMMAND ----------

dbutils.widgets.text(
    "ADLS_DESTINATION_PATH",
    "reference_data/clients/c360_reference_data_service",
)
ADLS_DESTINATION_PATH = dbutils.widgets.get("ADLS_DESTINATION_PATH")

dbutils.widgets.text("TARGET_SCHEMA_NAME", "bronze")
TARGET_SCHEMA_NAME = dbutils.widgets.get("TARGET_SCHEMA_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load the latest file from the Volume

# COMMAND ----------

raw_data_folder = f"{VOLUME_BASE_PATH}/{ADLS_DESTINATION_PATH.rstrip('/')}"


@enhanced_errors()
def get_latest_file_from_dir(dir_path: str):
    files = [f for f in dbutils.fs.ls(dir_path) if not f.isDir()]
    if not files:
        raise NotebookExecutionException(f"No files found in {dir_path}")
    return max(files, key=lambda f: f.modificationTime)


file = get_latest_file_from_dir(raw_data_folder)

path = file.path
file_name = file.name

# COMMAND ----------

# MAGIC %md
# MAGIC # Convert to DataFrame and apply schema

# COMMAND ----------

clients_schema = StructType([
    StructField("client", StructType([
        StructField("id", IntegerType(), True),
        StructField("displayName", StringType(), True),
        StructField("isTopLevelClient", BooleanType(), True),
        StructField("active", BooleanType(), True),
        StructField("topLevelClient", StructType([
            StructField("id", IntegerType(), True),
            StructField("displayName", StringType(), True),
            StructField("focusedAccount", BooleanType(), True),
            StructField("active", BooleanType(), True),
            StructField("type", StructType([
                StructField("title", StringType(), True)
            ]), True),
        ]), True),
        StructField("type", StructType([
            StructField("title", StringType(), True)
        ]), True),
        StructField("focusedAccount", BooleanType(), True),
    ]), True),
    StructField("shortCode", StringType(), True),
    StructField("sourceSystem", StringType(), True)
])

# COMMAND ----------

df = spark.read.json(path, schema=clients_schema)

# Keep only the file name here (not the /Volumes/... mount path) so lineage stays
# consistent with the storage-location-relative naming used elsewhere in bronze.
df = df.withColumn("file_name", lit(file_name))

# COMMAND ----------

# MAGIC %md
# MAGIC # Store in Unity Catalog managed table
# MAGIC
# MAGIC REMOVED vs hive_metastore version: unqualified `TARGET_SCHEMA_NAME + ".clients"`
# MAGIC table name. Writing without LOCATION makes this a UC-managed table under the
# MAGIC catalog, consistent with the other bronze/silver conversions in this project.

# COMMAND ----------

target_table = f"`{CATALOG_NAME}`.`{TARGET_SCHEMA_NAME}`.`clients`"

(df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table))

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{CATALOG_NAME}`.`bronze`.`tdsci_client_mappings_raw`")

# COMMAND ----------

# End of Notebook