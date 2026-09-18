# Databricks notebook source
# MAGIC %md
# MAGIC # Create view used to join Inventory, Business and Desk details

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

SILVER_FI_DESK_INVENTORY_VIEW = f"`{CATALOG_NAME}`.`silver`.`fi_desk_inventory`"
BRONZE_FI_INVENTORY_TABLE = f"`{CATALOG_NAME}`.`bronze`.`fi_inventory`"
BRONZE_FI_DESK_TABLE = f"`{CATALOG_NAME}`.`bronze`.`fi_desk`"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {SILVER_FI_DESK_INVENTORY_VIEW} AS
SELECT
  i.inventoryId,
  i.inventoryName,
  i.tradeSystemId,
  i.tradeSystemName,
  d.deskId,
  d.deskName,
  d.businessId,
  d.businessName
FROM {BRONZE_FI_INVENTORY_TABLE} i
INNER JOIN {BRONZE_FI_DESK_TABLE} d
  ON i.deskId = d.deskId
""")
