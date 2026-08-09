# Databricks notebook source
# DBTITLE 1,Set catalog context
dbutils.widgets.text("CATALOG", "")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${CATALOG};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create the Silver Table
# MAGIC CREATE TABLE IF NOT EXISTS `${CATALOG}`.silver.fx_panoramic (
# MAGIC trade_date DATE,
# MAGIC original_contract_id BIGINT,
# MAGIC original_trade_id BIGINT,
# MAGIC trade_id BIGINT,
# MAGIC customer_long_name STRING,
# MAGIC murex_mnemonic STRING,
# MAGIC sales_desk_type STRING,
# MAGIC sales_region STRING,
# MAGIC product_type STRING,
# MAGIC currency_pair STRING,
# MAGIC currency_group STRING,
# MAGIC side STRING,
# MAGIC volume_usd DECIMAL (18,6),
# MAGIC client_value_usd DECIMAL (18,6),
# MAGIC murex_mnemonic_name STRING,
# MAGIC usdcad_mid DECIMAL (18,6),
# MAGIC client_value_cad DECIMAL (18,6),
# MAGIC volume_cad DECIMAL (18,6),
# MAGIC is_client_facing BOOLEAN,
# MAGIC last_modified TIMESTAMP,
# MAGIC extract_date DATE,
# MAGIC file_name_path STRING,
# MAGIC brnz_ingestion_timestamp TIMESTAMP,
# MAGIC slvr_ingestion_timestamp TIMESTAMP,
# MAGIC source_system STRING,
# MAGIC trade_year STRING,
# MAGIC trade_month STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (trade_year, trade_month);

# COMMAND ----------

# DBTITLE 1,Populate Data in Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Identify Updated Trade Dates
# MAGIC CREATE OR REPLACE TEMP VIEW updated_trade_dates AS
# MAGIC SELECT DISTINCT b.TradeDate AS trade_date
# MAGIC FROM `${CATALOG}`.bronze.fx_panoramic b
# MAGIC LEFT JOIN (
# MAGIC     SELECT trade_date, MAX(last_modified) AS silver_last_modified
# MAGIC     FROM `${CATALOG}`.silver.fx_panoramic
# MAGIC     GROUP BY trade_date
# MAGIC ) s
# MAGIC ON b.TradeDate = s.trade_date
# MAGIC WHERE b.LastModified > s.silver_last_modified OR s.silver_last_modified IS NULL;
# MAGIC
# MAGIC -- Step 2: Delete Old Data for Updated Trade Dates
# MAGIC DELETE FROM `${CATALOG}`.silver.fx_panoramic
# MAGIC WHERE trade_date IN (SELECT trade_date FROM updated_trade_dates);
# MAGIC
# MAGIC -- Step 3: Insert New Data into the Silver Table
# MAGIC INSERT INTO `${CATALOG}`.silver.fx_panoramic
# MAGIC SELECT
# MAGIC b.TradeDate AS trade_date,
# MAGIC b.OriginalContractId AS original_contract_id,
# MAGIC b.OriginalTradeId AS original_trade_id,
# MAGIC b.TradeId AS trade_id,
# MAGIC b.CustomerLongName AS customer_long_name,
# MAGIC b.MurexMnemonic AS murex_mnemonic,
# MAGIC b.SalesDeskType AS sales_desk_type,
# MAGIC b.SalesRegion AS sales_region,
# MAGIC b.ProductType AS product_type,
# MAGIC b.CurrencyPair AS currency_pair,
# MAGIC b.CurrencyGroup AS currency_group,
# MAGIC b.Side AS side,
# MAGIC b.VolumeUsd AS volume_usd,
# MAGIC b.ClientValueUsd AS client_value_usd,
# MAGIC b.MurexMnemonicName AS murex_mnemonic_name,
# MAGIC b.UsdcadMid AS usdcad_mid,
# MAGIC b.ClientValueCad AS client_value_cad,
# MAGIC b.VolumeCad AS volume_cad,
# MAGIC b.IsClientFacing AS is_client_facing,
# MAGIC b.LastModified AS last_modified,
# MAGIC b.ExtractDate AS extract_date,
# MAGIC b.file_name_path AS file_name_path,
# MAGIC b.ingestion_timestamp AS brnz_ingestion_timestamp,
# MAGIC current_timestamp() AS slvr_ingestion_timestamp,
# MAGIC 'MUREX_GLBFX' AS source_system,
# MAGIC YEAR(b.TradeDate) AS trade_year, -- Extract year from TradeDate
# MAGIC MONTH(b.TradeDate) AS trade_month -- Extract month from TradeDate
# MAGIC FROM `${CATALOG}`.bronze.fx_panoramic b
# MAGIC WHERE b.TradeDate IN (SELECT trade_date FROM updated_trade_dates);