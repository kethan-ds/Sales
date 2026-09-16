# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Silver Mapping for Client Codes (Unity Catalog)

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# NOTE (UC migration): the only change here is qualifying both the view and the
# source table with the catalog name instead of relying on the current
# hive_metastore default catalog.

spark.sql(f"""
CREATE OR REPLACE VIEW `{CATALOG_NAME}`.silver.clients AS
SELECT
    client.id AS clientId,
    client.topLevelClient.id AS topLevelClientId,
    client.displayName,
    client.type.title,
    client.focusedAccount AS isFocusedAccount,
    client.isTopLevelClient,
    client.active,
    client.topLevelClient.displayName AS topLevelClientDisplayName,
    client.topLevelClient.type.title AS topLevelClientType,
    client.topLevelClient.focusedAccount AS topLevelClientIsFocusedAccount,
    client.topLevelClient.active AS topLevelClientActive,
    sourceSystem,
    shortCode
FROM `{CATALOG_NAME}`.bronze.clients
""")

# COMMAND ----------

# End of Notebook