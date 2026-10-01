# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Create view used to join Inventory, Business and Desk details

# COMMAND ----------

dbutils.widgets.text('CATALOG', '')
CATALOG_NAME = dbutils.widgets.get('CATALOG')
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW `{CATALOG_NAME}`.`silver`.`fi_desk_inventory` AS
SELECT
  i.inventoryId,
  i.inventoryName,
  i.tradeSystemId,
  i.tradeSystemName,
  d.deskId,
  d.deskName,
  d.businessId,
  d.businessName
FROM
  `{CATALOG_NAME}`.`bronze`.`fi_inventory` i INNER JOIN
  `{CATALOG_NAME}`.`bronze`.`fi_desk` d ON i.deskId = d.deskId
""")