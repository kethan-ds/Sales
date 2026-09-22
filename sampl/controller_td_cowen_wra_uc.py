# Databricks notebook source

# Unity Catalog parameter
dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG").strip()

if not CATALOG_NAME:
    raise ValueError("CATALOG parameter is required")

NOTEBOOK_ARGUMENTS = {
    "CATALOG": CATALOG_NAME
}

# COMMAND ----------

try:
    dbutils.notebook.run(
        "./raw_td_cowen_wra.py",
        600,
        NOTEBOOK_ARGUMENTS
    )
except Exception as e:
    raise Exception(f"Error occurred while running raw notebook: {e}")

# COMMAND ----------

try:
    dbutils.notebook.run(
        "./bronze_td_cowen_wra.py",
        600,
        NOTEBOOK_ARGUMENTS
    )
except Exception as e:
    raise Exception(f"Error occurred while running bronze notebook: {e}")

# COMMAND ----------

try:
    dbutils.notebook.run(
        "./silver_td_cowen_wra.py",
        600,
        NOTEBOOK_ARGUMENTS
    )
except Exception as e:
    raise Exception(f"Error occurred while running silver notebook: {e}")
