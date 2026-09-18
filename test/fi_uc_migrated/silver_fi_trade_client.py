# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Merged View of Client And Trade Data

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

SILVER_FI_TRADES_TABLE = f"`{CATALOG_NAME}`.`silver`.`fi_trades`"
SILVER_FI_TRADES_CLIENTS_TABLE = f"`{CATALOG_NAME}`.`silver`.`fi_trades_clients`"
SILVER_CLIENTS_VIEW = f"`{CATALOG_NAME}`.`silver`.`clients`"
SILVER_FI_TRADES_DISPLAY_VIEW = f"`{CATALOG_NAME}`.`silver`.`fi_trades_display_view`"

source = SILVER_FI_TRADES_TABLE
target = SILVER_FI_TRADES_CLIENTS_TABLE
clients_view = SILVER_CLIENTS_VIEW
display_view = SILVER_FI_TRADES_DISPLAY_VIEW

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target}
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    AS SELECT
        *,
        CAST(NULL AS STRING) AS clientDisplayName,
        CAST(NULL AS STRING) AS clientDisplayNameType,
        CAST(NULL AS STRING) AS clientDisplayNameTopLevel,
        CAST(NULL AS STRING) AS clientDisplayNameTopType,
        CAST(NULL AS STRING) AS focusedAccount
    FROM {source}
    WHERE FALSE
""")

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {target} AS targ
    USING (SELECT
        t.*,
        COALESCE(c.displayName, 'Unmapped Client') AS clientDisplayName,
        c.title AS clientDisplayNameType,
        c.topLevelClientDisplayName AS clientDisplayNameTopLevel,
        c.topLevelClientType AS clientDisplayNameTopType,
        CASE
            WHEN c.isFocusedAccount = TRUE THEN 'Yes'
            WHEN c.isFocusedAccount = FALSE THEN 'No'
            ELSE 'Not Provided'
        END AS focusedAccount
    FROM
        {source} t
        LEFT JOIN {clients_view} c
        ON UPPER(t.sourceSystem) = UPPER(c.sourceSystem)
        AND UPPER(t.counterPartyCode) = UPPER(c.shortCode)) as sour
    ON targ.tradeId = sour.tradeId
    AND targ.sourceSystem = sour.sourceSystem
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create view for any client wanting user readable column names

# COMMAND ----------

spark.sql(f"""
    CREATE VIEW IF NOT EXISTS {display_view} AS
      SELECT
        tradeId as `Trade ID`,
        sourceSystem as `Source System`,
        tradeDate as `Trade Date`,
        tradeDescription as `Trade Description`,
        tradeCurrency as `Trade Currency`,
        isClientFacing as `Is Client Facing`,
        direction as `Direction`,
        yearMonth as `Year Month`,
        fiscalTradeQuarter as `Fiscal Trade Quarter`,
        c360TradeExecutionType as `Client360 Trade Execution Type`,
        c360MultiLegType as `Client360 MultiLeg Type`,
        c360TradeType as `Client360 Trade Type`,
        maturityDate as `Maturity Date`,
        price as `Price`,
        volumeTradeCurrency as `Volume Trade Currency`,
        volumeCadMM as `Volume CAD MM`,
        volumeUsdMM as `Volume USD MM`,
        volumeTradeCurrencyMM as `Volume Trade Currency MM`,
        c360SettlementAmount as `Client360 Settlement Amount`,
        securityID as `Security ID`,
        securityDescription as `Security Description`,
        securityIDType as `Security ID Type`,
        book as `Book`,
        riskOn as `Risk On`,
        traderRegion as `Trade Region`,
        legacyProductLevel0 as `Legacy Product Level 0`,
        legacyProductLevel1 as `Legacy Product Level 1`,
        legacyProductLevel2 as `Legacy Product Level 2`,
        legacyProductLevel3 as `Legacy Product Level 3`,
        productLevel0 as `Product Level 0`,
        productLevel1 as `Product Level 1`,
        productLevel2 as `Product Level 2`,
        productLevel3 as `Product Level 3`,
        counterPartyCode as `CounterParty Code`,
        rawClientName as `Raw Client Name`,
        trader as Trader,
        salesPersonACF2 as `Sales Person ACF2`,
        dv01TradeCurrency as `DV01 Trade Currency`,
        dv01Cad as `DV01 CAD`,
        dv01Usd as `DV01 USD`,
        cadUsFxRate as `CAD USD FX Rate`,
        srcCadFxRate as `Source CAD FX Rate`,
        c360CashSpread as `Client360 Cash Spread`,
        c360YieldSpread as `Client360 Yield Spread`,
        c360EstimatedSpread as `Client360 Estimate Spread`,
        c360CashAsk as `Client360 Cash Ask`,
        c360CashBid as `Client360 Cash Bid`,
        c360YieldAsk as `Client360 Yield Ask`,
        c360YieldBid as `Client360 Yield Bid`,
        c360CashSpreadSource as `Client360 Cash Spread Source`,
        c360YieldSpreadSource as `Client360 Yield Spread Source`,
        clientValueCadAllDates as `Client Value CAD All Dates`,
        clientValueUsd as `Client Value USD`,
        clientValueMarkupValueCad as `Client Value Markup Value CAD`,
        preComputedClientValueCad as `PreComputed Client Value CAD`,
        clientValueCad as `Client Value CAD`,
        clientDisplayName as `Client Display Name`,
        clientDisplayNameType as `Client Display Name Type`,
        clientDisplayNameTopLevel as `Client Display Name Top Level`,
        clientDisplayNameTopType as `Client Display Name Top Type`,
        focusedAccount as `Focused Account`
    FROM {target}
""")

