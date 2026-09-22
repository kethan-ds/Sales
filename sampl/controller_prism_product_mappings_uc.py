# Databricks notebook source
dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")

# COMMAND ----------

try:
    dbutils.notebook.run(
        "./raw_prism_product_mappings.py",
        600,
        {"CATALOG": CATALOG_NAME}
    )
except Exception as e:
    raise Exception(f"Error occurred while running bronze notebook: {e}")

# COMMAND ----------

try:
    dbutils.notebook.run(
        "./bronze_prism_product_mappings.py",
        600,
        {"CATALOG": CATALOG_NAME}
    )
except Exception as e:
    raise Exception(f"Error occurred while running bronze notebook: {e}")

# COMMAND ----------

try:
    dbutils.notebook.run(
        "./silver_prism_product_mappings.py",
        600,
        {"CATALOG": CATALOG_NAME}
    )
except Exception as e:
    raise Exception(f"Error occurred while running silver notebook: {e}")
