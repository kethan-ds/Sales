# Databricks notebook source
import os

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

dbutils.widgets.text("cad_prime_source_file", "")
cad_prime_source_file = dbutils.widgets.get("cad_prime_source_file")

# COMMAND ----------

if not cad_prime_source_file:
    raise ValueError(
        "cad_prime_source_file path not supplied, exiting run..."
    )

# COMMAND ----------

# Legacy container-relative paths (pre-UC convention) started with this prefix.
_LEGACY_PREFIXES = ("data/raw/", "/data/raw/")

# COMMAND ----------

def to_volume_path(path: str) -> str:
    """Normalize a supplied path -- which may be an external ADLS lineage
    path, an already-Volume-rooted path, or a legacy container-relative
    path -- to the Volume (FUSE) path needed to actually read/copy the file.
    """
    if path.startswith(EXTERNAL_LOCATION_BASE_PATH):
        return path.replace(EXTERNAL_LOCATION_BASE_PATH, VOLUME_BASE_PATH)
    if path.startswith(VOLUME_BASE_PATH):
        return path
    for prefix in _LEGACY_PREFIXES:
        if path.startswith(prefix):
            return f"{VOLUME_BASE_PATH}/{path[len(prefix):]}"
    raise ValueError(
        f"Unrecognized cad_prime_source_file path format: {path}"
    )

# COMMAND ----------

target_name = os.path.basename(
    cad_prime_source_file.replace("d_", "").replace("c_", "")
)

source_volume_path = to_volume_path(cad_prime_source_file)
target_volume_path = f"{VOLUME_BASE_PATH}/tmp/cad_prime/{target_name}"

dbutils.fs.cp(source_volume_path, target_volume_path)