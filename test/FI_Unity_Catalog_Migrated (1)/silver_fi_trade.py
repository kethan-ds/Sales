# Databricks notebook source
# MAGIC %md
# MAGIC ### Retrieve Bronze Data

# COMMAND ----------

import os

from pyspark.sql.types import FloatType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import LongType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit
from pyspark.sql.functions import date_format, concat, month, year, when, lit, abs, col

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

BRONZE_FI_TRADES_TABLE = f"`{CATALOG_NAME}`.`bronze`.`fi_trades`"
SILVER_FI_DESK_INVENTORY_VIEW = f"`{CATALOG_NAME}`.`silver`.`fi_desk_inventory`"
SILVER_FI_TRADES_TABLE = f"`{CATALOG_NAME}`.`silver`.`fi_trades`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## All Dates Fields

# COMMAND ----------

BOOK                                   = "book"
C360_CASHBID                           = "c360CashBid"
C360_CASH_ASK                          = "c360CashAsk"
C360_CASH_SPREAD                       = "c360CashSpread"
C360_CASH_SPREAD_SOURCE                = "c360CashSpreadSource"
C360_ESTIMATED_SPREAD                  = "c360EstimatedSpread"
C360_MULTILEG_TYPE                     = "c360MultiLegType"
C360_SETTLEMENT_AMOUNT                 = "c360SettlementAmount"
C360_TRADE_EXECUTION_TYPE              = "c360TradeExecutionType"
C360_TRADE_TYPE                        = "c360TradeType"
C360_YIELD_ASK                         = "c360YieldAsk"
C360_YIELD_BID                         = "c360YieldBid"
C360_YIELD_SPREAD                      = "c360YieldSpread"
C360_YIELD_SPREAD_SOURCE               = "c360YieldSpreadSource"
CAD_US_FX_RATE                         = "cadUsFxRate"
CLIENT_VALUE_CAD                       = "clientValueCad"
CLIENT_VALUE_CAD_All_DATES             = "clientValueCadAllDates"
CLIENT_VALUE_MARKUP_VALUE_CAD          = "clientValueMarkupValueCad"
CLIENT_VALUE_USD                       = "clientValueUsd"
COUNTER_PARTY_CODE                     = "counterPartyCode"
DIRECTION                              = "direction"
DV01_CAD                               = "dv01Cad"
DV01_TRADE_CURRENCY                    = "dv01TradeCurrency"
DV01_USD                               = "dv01Usd"
FISCAL_TRADE_QUARTER                   = "fiscalTradeQuarter"
FISCAL_QUARTER                         = "fiscalQuarter"
FISCAL_YEAR                            = "fiscalYear"
IS_CLIENT_FACING                       = "isClientFacing"
LEGACY_PRODUCT_LEVEL_0                 = "legacyProductLevel0"
LEGACY_PRODUCT_LEVEL_1                 = "legacyProductLevel1"
LEGACY_PRODUCT_LEVEL_2                 = "legacyProductLevel2"
LEGACY_PRODUCT_LEVEL_3                 = "legacyProductLevel3"
MATURITY_DATE                          = "maturityDate"
PRE_COMPUTED_CLIENT_VALUE_CAD          = "preComputedClientValueCad"
PRE_COMPUTE_REQUIRED                   = "preComputedRequired"
PRICE                                  = "price"
PRODUCT_LEVEL_0                        = "productLevel0"
PRODUCT_LEVEL_1                        = "productLevel1"
PRODUCT_LEVEL_2                        = "productLevel2"
PRODUCT_LEVEL_3                        = "productLevel3"
PRODUCT_LEVEL_4                        = "productLevel4"
RAW_CLIENT_NAME                        = "rawClientName"
RISK_ON                                = "riskOn"
SALES_PERSON                           = "salesPerson"
SALES_PERSON_ACF2                      = "salesPersonACF2"
SALES_PERSON_SOURCE                    = "salesPersonSource"
SECURITY_DESCRIPTION                   = "securityDescription"
SECURITY_ID                            = "securityId"
SECURITY_ID_TYPE                       = "securityIdType"
SOURCE_SYSTEM                          = "sourceSystem"
SRC_CAD_FXRATE                         = "srcCadFxRate"
TRADER                                 = "trader"
TRADER_REGION                          = "traderRegion"
TRADE_CURRENCY                         = "tradeCurrency"
TRADE_DATE                             = "tradeDate"
TRADE_DESCRIPTION                      = "tradeDescription"
TRADE_ID                               = "tradeId"
VOLUME_CAD_MM                          = "volumeCadMM"
VOLUME_TRADE_CURRENCY                  = "volumeTradeCurrency"
VOLUME_TRADE_CURRENCY_MM               = "volumeTradeCurrencyMM"
VOLUME_USD_MM                          = "volumeUsdMM"
YEAR_MONTH                             = "yearMonth"

# COMMAND ----------

# MAGIC %md
# MAGIC ## C360 Fields

# COMMAND ----------

C360_TRADE_ID                              = 'tradeId'
C360_TRADE_SYSTEM                          = 'tradeSystem'
C360_TRADE_DATE                            = 'tradeDate'
C360_TRADE_DESCRIPTION                     = 'tradeDescription'
C360_TRADE_CURRENCY                        = 'tradeCurrency'
C360_DIRECTION                             = 'c360Direction'
C360_SECURITY_MATURITY_DATE                = 'securityMaturityDate'
C360_TRADE_PRICE                           = 'tradePrice'
C360_SECURITY_KEY                          = 'securityKey'
C360_SECURITY_DESCRIPTION                  = 'securityDescription'
C360_SECURITY_KEYTYPE                      = 'securityKeyType'
C360_INVENTORY                             = 'inventory'
C360_ISRISK_ON                             = 'c360IsRiskOn'
C360_TRADER_REGION                         = 'c360TraderRegion'
C360_CLIENTCODE                            = 'c360ClientCode'
C360_COUNTERPARTY_NAME                     = 'counterpartyName'
C360_TRADER_ID                             = 'traderId'
C360_SALES_PERSON_ID                       = 'salesPersonId'
C360_SALES_PERSON_NAME                     = 'salesPersonName'
C360_SALES_PERSON_SOURCE                   = 'salesPersonSource'
C360_DV01                                  = 'c360Dv01'
C360_CAD_USD_FXRATE                        = 'cadUsdFxRate'
C360_QUANTITY                              = 'c360Quantity'
C360_PRODUCT_CLASS_DISPLAY_NAME            = 'c360ProductClassDisplayName'
C360_PRODUCT_CLASS_TYPE_DISPLAY_NAME       = 'c360ProductClassTypeDisplayName'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory Names

# COMMAND ----------

INVENTORY_NAME = 'inventoryName'
TRADE_SYSTEM_NAME = 'tradeSystemName'
BUSINESS_NAME = 'businessName'
DESK_NAME = 'deskName'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read In Bronze Data and Select Only Relevant Columns

# COMMAND ----------

# These are the relevant columns we actually use
relevant_columns = [
    'securityPrincipalFactor',
    'deskAdjustmentWeight',
    'id',
    'c360Version',
    'tradeSystem',
    'tradeId',
    'tradeVersion',
    'tradeDate',
    'settleDate',
    'isClientFacing',
    'inventory',
    'c360ClientCode',
    'counterpartyName',
    'traderId',
    'salesPersonId',
    'settleCurrency',
    'settleAmountInSettleCurrency',
    'tradeCurrency',
    'settleAmountInTradeCurrency',
    'commissionValue',
    'tradeDescription',
    'tradeQuantity',
    'tradePrice',
    'marketDataKey',
    'tradeMaturityDate',
    'tradeSecurityKey',
    'parentTradeSystem',
    'securityMaturityDate',
    'parentTradeId',
    'securityKey',
    'securityDescription',
    'securityKeyType',
    'productLevel1',
    'productLevel2',
    'productLevel3',
    'productLevel4',
    'c360ProductClassDisplayName',
    'c360ProductClassTypeDisplayName',
    'c360MaturityTermInDays',
    'c360DaysToMaturity',
    'tradeProductRepoDayCount',
    'c360Description',
    'tradeCalculatorType',
    'c360TradeExecutionType',
    'c360IsRiskOn',
    'c360Direction',
    'c360TraderRegion',
    'c360TradeType',
    'c360MultiLegType',
    'c360Quantity',
    'c360CashSpread',
    'c360YieldSpread',
    'c360EstimatedSpread',
    'c360SettlementAmount',
    'c360CashAsk',
    'c360CashBid',
    'c360YieldAsk',
    'c360YieldBid',
    'c360CashSpreadSource',
    'c360YieldSpreadSource',
    'c360CvCalculator',
    'c360Dv01',
    'c360Dv01Source',
    'c360RiskMetricValue',
    'c360RiskMetricSource',
    'c360RiskMetricType',
    'cadUsdFxRate',
    'srcCadFxRate',
    'clientValueCad',
    'preComputedClientValueCad',
    'clientValueMarkupValueCad',
    'clientValueMarkupSource',
    'etlStatus',
 ]

# COMMAND ----------

bronze_df = spark.table(BRONZE_FI_TRADES_TABLE).select(relevant_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simple Column Renames

# COMMAND ----------

simple_rename_columns = {
    C360_TRADE_ID: TRADE_ID,
    C360_TRADE_SYSTEM: SOURCE_SYSTEM,
    C360_DIRECTION: DIRECTION,
    C360_SECURITY_MATURITY_DATE: MATURITY_DATE,
    C360_TRADE_PRICE: PRICE,
    C360_SECURITY_KEY: SECURITY_ID,
    C360_SECURITY_KEYTYPE: SECURITY_ID_TYPE,
    C360_INVENTORY: BOOK,
    C360_ISRISK_ON: RISK_ON,
    C360_TRADER_REGION: TRADER_REGION,
    C360_CLIENTCODE: COUNTER_PARTY_CODE,
    C360_COUNTERPARTY_NAME: RAW_CLIENT_NAME,
    C360_TRADER_ID: TRADER,
    C360_SALES_PERSON_ID: SALES_PERSON_ACF2,
    C360_SALES_PERSON_NAME: SALES_PERSON,
    C360_DV01: DV01_TRADE_CURRENCY,
    C360_CAD_USD_FXRATE: CAD_US_FX_RATE
}

# COMMAND ----------

bronze_df = bronze_df.withColumnsRenamed(simple_rename_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DateTime Column Enrichment

# COMMAND ----------

bronze_df = bronze_df.withColumn(YEAR_MONTH, date_format(C360_TRADE_DATE, 'yyyy-MM'))
# Need to calculate Quarter and FY columns temporarily to get fiscal trade quarter
bronze_df = bronze_df.withColumn(
    FISCAL_QUARTER,
    when(month(C360_TRADE_DATE).isin(11, 12, 1), 'Q1')
    .when(month(C360_TRADE_DATE).isin(2, 3, 4), 'Q2')
    .when(month(C360_TRADE_DATE).isin(5, 6, 7), 'Q3')
    .when(month(C360_TRADE_DATE).isin(8, 9, 10), 'Q4')
)\
.withColumn(
    FISCAL_YEAR,
    when(month(C360_TRADE_DATE).isin(11, 12, 1), year(C360_TRADE_DATE) + 1)
    .otherwise(year(C360_TRADE_DATE))
)\
.withColumn(FISCAL_TRADE_QUARTER, concat(lit('FY'), FISCAL_YEAR, FISCAL_QUARTER))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fiscal Column Enrichment

# COMMAND ----------

# Volume Columns
# MM columns usually named with , and changed to . for tableau upload but given . here for sql upload compliance
bronze_df = bronze_df.withColumn(
    VOLUME_TRADE_CURRENCY,
    abs(C360_QUANTITY))\
.withColumn(
    VOLUME_CAD_MM,
    col(VOLUME_TRADE_CURRENCY) * col(SRC_CAD_FXRATE) / 1e6
)\
.withColumn(
    VOLUME_USD_MM,
    col(VOLUME_CAD_MM) * col(CAD_US_FX_RATE)
)\
.withColumn(
    VOLUME_TRADE_CURRENCY_MM,
    col(VOLUME_TRADE_CURRENCY) / 1e6
)

# DV01 Columns
bronze_df = bronze_df.withColumn(
    DV01_CAD,
    abs(col(DV01_TRADE_CURRENCY) * col(SRC_CAD_FXRATE)))\
.withColumn(
    DV01_USD,
    abs(col(DV01_CAD) * col(CAD_US_FX_RATE))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Class Enrichment

# COMMAND ----------

# MAGIC %md
# MAGIC #### Retrieve Silver Business and Inventories data

# COMMAND ----------

# Legacy Product Level Columns Being assigned before dropping legacy columns
bronze_df = bronze_df.withColumns({
    LEGACY_PRODUCT_LEVEL_0: col(PRODUCT_LEVEL_1),
    LEGACY_PRODUCT_LEVEL_1: col(PRODUCT_LEVEL_2),
    LEGACY_PRODUCT_LEVEL_2: col(PRODUCT_LEVEL_3),
    LEGACY_PRODUCT_LEVEL_3: col(PRODUCT_LEVEL_4)
})
# Drop old product level columns so they don't interfere with new assignments
bronze_df = bronze_df.drop(
    PRODUCT_LEVEL_1,
    PRODUCT_LEVEL_2,
    PRODUCT_LEVEL_3,
    PRODUCT_LEVEL_4
    )


# COMMAND ----------

silver_bus_inv_df = spark.table(SILVER_FI_DESK_INVENTORY_VIEW)\
    .select(
        col(INVENTORY_NAME).alias(BOOK),
        col(TRADE_SYSTEM_NAME).alias(SOURCE_SYSTEM),
        col(BUSINESS_NAME).alias(PRODUCT_LEVEL_0),
        col(DESK_NAME).alias(PRODUCT_LEVEL_1)
    )

# COMMAND ----------

# Join in desk and business as Product Level 0 and 1
bronze_df = bronze_df.join(silver_bus_inv_df, [BOOK, SOURCE_SYSTEM], 'left')

# Rename c360 product class and type as Product Level 2 and 3 respectively
bronze_df = bronze_df.withColumnsRenamed({
    C360_PRODUCT_CLASS_DISPLAY_NAME: PRODUCT_LEVEL_2,
    C360_PRODUCT_CLASS_TYPE_DISPLAY_NAME: PRODUCT_LEVEL_3,
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Client Value Enrichment

# COMMAND ----------

# Assign temporary column for selecting precomputed or standard client value calculation
bronze_df = bronze_df.withColumn(
    PRE_COMPUTE_REQUIRED,
    (col(PRODUCT_LEVEL_1) == 'Canadian DCM') | (col(PRODUCT_LEVEL_2) == 'FI Derivative') | (col(PRODUCT_LEVEL_2) == 'US Agency')
)

# Waterfall logic assigns markups first then fills blanks with either precomputed or standard cv based on above filter
# First assign markups since they take priority
bronze_df = bronze_df.withColumn(
    CLIENT_VALUE_CAD_All_DATES,
    col(CLIENT_VALUE_MARKUP_VALUE_CAD)
)

# Then when markups are null assign cv by waterfall logic
bronze_df = bronze_df.withColumn(
    CLIENT_VALUE_CAD_All_DATES,
    when(
        col(CLIENT_VALUE_CAD_All_DATES).isNull() & col(PRE_COMPUTE_REQUIRED),
        col(PRE_COMPUTED_CLIENT_VALUE_CAD)
    ).when(
        col(CLIENT_VALUE_CAD_All_DATES).isNull() & ~col(PRE_COMPUTE_REQUIRED),
        col(CLIENT_VALUE_CAD_All_DATES)
    )
).withColumn(
    CLIENT_VALUE_USD,
    col(CLIENT_VALUE_CAD_All_DATES) * col(CAD_US_FX_RATE)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Specially Format Columns For Tableau Publish

# COMMAND ----------

# We format a number of columns either replacing true and false values or null values with a standard value for tableau display
bronze_df = bronze_df.withColumn(
    IS_CLIENT_FACING,
    when(col(IS_CLIENT_FACING), '1').otherwise('0')
).fillna(
    'Unmapped Product',
    [PRODUCT_LEVEL_0, PRODUCT_LEVEL_1, PRODUCT_LEVEL_2, PRODUCT_LEVEL_3, LEGACY_PRODUCT_LEVEL_0, LEGACY_PRODUCT_LEVEL_1, LEGACY_PRODUCT_LEVEL_2, LEGACY_PRODUCT_LEVEL_3]
).withColumn(
    RISK_ON,
    when(col(RISK_ON) == 'true', True)
    .when(col(RISK_ON) == 'false', False)
).withColumn(
    PRODUCT_LEVEL_0,
    when(col(PRODUCT_LEVEL_0) == 'Canadian Fixed Income', 'Canada Fixed Income')
    .otherwise(col(PRODUCT_LEVEL_0))
).filter(
    col(PRODUCT_LEVEL_1) != 'Canadian DCM'
)

bronze_df = bronze_df.withColumnRenamed(C360_TRADE_DATE, TRADE_DATE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter for desired columns

# COMMAND ----------

silver_1_df = bronze_df.select(TRADE_ID, SOURCE_SYSTEM, TRADE_DATE, TRADE_DESCRIPTION, TRADE_CURRENCY, IS_CLIENT_FACING, DIRECTION, YEAR_MONTH,
                             FISCAL_TRADE_QUARTER, C360_TRADE_EXECUTION_TYPE, C360_MULTILEG_TYPE, C360_TRADE_TYPE, MATURITY_DATE, PRICE, VOLUME_TRADE_CURRENCY,
                             VOLUME_CAD_MM, VOLUME_USD_MM, VOLUME_TRADE_CURRENCY_MM, C360_SETTLEMENT_AMOUNT, SECURITY_ID, SECURITY_DESCRIPTION, SECURITY_ID_TYPE,
                             BOOK, RISK_ON, TRADER_REGION, LEGACY_PRODUCT_LEVEL_0, LEGACY_PRODUCT_LEVEL_1, LEGACY_PRODUCT_LEVEL_2, LEGACY_PRODUCT_LEVEL_3, PRODUCT_LEVEL_0,
                             PRODUCT_LEVEL_1, PRODUCT_LEVEL_2, PRODUCT_LEVEL_3, COUNTER_PARTY_CODE, RAW_CLIENT_NAME, TRADER, SALES_PERSON_ACF2, DV01_TRADE_CURRENCY, DV01_CAD,
                             DV01_USD, CAD_US_FX_RATE, SRC_CAD_FXRATE, C360_CASH_SPREAD, C360_YIELD_SPREAD, C360_ESTIMATED_SPREAD, C360_CASH_ASK, C360_CASHBID, C360_YIELD_ASK,
                             C360_YIELD_BID, C360_CASH_SPREAD_SOURCE, C360_YIELD_SPREAD_SOURCE, CLIENT_VALUE_USD, CLIENT_VALUE_MARKUP_VALUE_CAD, PRE_COMPUTED_CLIENT_VALUE_CAD,
                             CLIENT_VALUE_CAD, CLIENT_VALUE_CAD_All_DATES)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upload To Silver

# COMMAND ----------

from delta.tables import DeltaTable

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_FI_TRADES_TABLE} (
        tradeId STRING,
        sourceSystem STRING,
        tradeDate TIMESTAMP,
        tradeDescription STRING,
        tradeCurrency STRING,
        isClientFacing STRING,
        direction STRING,
        yearMonth STRING,
        fiscalTradeQuarter STRING,
        c360TradeExecutionType STRING,
        c360MultiLegType STRING,
        c360TradeType STRING,
        maturityDate TIMESTAMP,
        price FLOAT,
        volumeTradeCurrency FLOAT,
        volumeCadMM FLOAT,
        volumeUsdMM FLOAT,
        volumeTradeCurrencyMM FLOAT,
        c360SettlementAmount FLOAT,
        securityID STRING,
        securityDescription STRING,
        securityIDType STRING,
        book STRING,
        riskOn STRING,
        traderRegion STRING,
        legacyProductLevel0 STRING,
        legacyProductLevel1 STRING,
        legacyProductLevel2 STRING,
        legacyProductLevel3 STRING,
        productLevel0 STRING,
        productLevel1 STRING,
        productLevel2 STRING,
        productLevel3 STRING,
        counterPartyCode STRING,
        rawClientName STRING,
        trader STRING,
        salesPersonACF2 STRING,
        dv01TradeCurrency FLOAT,
        dv01Cad FLOAT,
        dv01Usd FLOAT,
        cadUsFxRate FLOAT,
        srcCadFxRate FLOAT,
        c360CashSpread FLOAT,
        c360YieldSpread FLOAT,
        c360EstimatedSpread FLOAT,
        c360CashAsk FLOAT,
        c360CashBid FLOAT,
        c360YieldAsk FLOAT,
        c360YieldBid FLOAT,
        c360CashSpreadSource STRING,
        c360YieldSpreadSource STRING,
        clientValueCadAllDates FLOAT,
        clientValueUsd FLOAT,
        clientValueMarkupValueCad FLOAT,
        preComputedClientValueCad FLOAT,
        clientValueCad FLOAT
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# Get existing silver table
existing_silver_table = DeltaTable.forName(spark, SILVER_FI_TRADES_TABLE)

# Update when the trade exists, insert when it doesn't
existing_silver_table.alias("silver").merge(
    silver_1_df.alias("bronze"),
    "silver.tradeId = bronze.tradeId AND silver.sourceSystem = bronze.sourceSystem"
).whenMatchedUpdateAll()\
.whenNotMatchedInsertAll()\
.execute()

# COMMAND ----------



