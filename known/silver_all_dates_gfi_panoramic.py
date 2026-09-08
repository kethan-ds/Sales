# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

import os

from pyspark.sql.types import *
from pyspark.sql import functions as F
from functools import reduce
import io
import requests


# COMMAND ----------

dbutils.widgets.text("ENV", "dev")
dbutils.widgets.text('CLIENT_ID', '')
dbutils.widgets.text('CLIENT_SECRET', '')
dbutils.widgets.text('REFERENCE_DATA_URL', '')
dbutils.widgets.text('CATALOG', '')

# COMMAND ----------

ENV = dbutils.widgets.get("ENV").lower()
CLIENT_ID = dbutils.widgets.get('CLIENT_ID')
CLIENT_SECRET = dbutils.widgets.get('CLIENT_SECRET')
REFERENCE_DATA_URL = dbutils.widgets.get('REFERENCE_DATA_URL')
AUTH_URL = os.getenv('TDSCI_RIVENDELL_AUTH_URL', '')

CATALOG_NAME = dbutils.widgets.get('CATALOG')
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

#Create all_dates_silver table
all_dates_silver_schema = StructType([
    StructField("trade_id", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("trade_date", DateType(), True),
    StructField("trade_description", StringType(), True),
    StructField("trade_currency", StringType(), True),
    StructField("is_client_facing", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("year_month", StringType(), True),
    StructField("fiscal_trade_quarter", StringType(), True),
    StructField("c360_trade_execution_type", StringType(), True),
    #StructField("client_location", StringType(), True),
    StructField("c360_multi_leg_type", StringType(), True),
    StructField("c360_trade_type", StringType(), True),
    StructField("maturity_date", DateType(), True),
    StructField("days_to_maturity", IntegerType(), True),
    StructField("years_to_maturity", StringType(), True),
    StructField("maturity_bucket", StringType(), True),
    StructField("price", DecimalType(18,5), True),
    StructField("volume_trade_currency", DecimalType(18,5), True),
    StructField("volume_cad_mm", DecimalType(18,10), True),
    StructField("volume_usd_mm", DecimalType(18,10), True),
    StructField("volume_trade_currency_mm", DecimalType(18,10), True),
    StructField("c360_settlement_amount", DecimalType(18,5), True),
    StructField("security_id", StringType(), True),
    StructField("security_description", StringType(), True),
    StructField("security_id_type", StringType(), True),
    StructField("book", StringType(), True),
    #StructField("inventory_group_name", StringType(), True),## Maybe Remove
    StructField("risk_on", StringType(), True),
    StructField("trader_region", StringType(), True),
    StructField("legacy_product_level_0", StringType(), True),
    StructField("legacy_product_level_1", StringType(), True),
    StructField("legacy_product_level_2", StringType(), True),
    StructField("legacy_product_level_3", StringType(), True),
    StructField("product_level_0", StringType(), True),
    StructField("product_level_1", StringType(), True),
    StructField("product_level_2", StringType(), True),
    StructField("product_level_3", StringType(), True),
    StructField("counterparty_code", StringType(), True),
    StructField("raw_client_name", StringType(), True),
    StructField("client_display_name", StringType(), True),
    StructField("client_display_name_type", StringType(), True),
    StructField("focused_account", StringType(), True),
    StructField("client_display_name_top_level", StringType(), True),
    StructField("client_display_name_top_level_type", StringType(), True),
    StructField("trader", StringType(), True),
    StructField("salesperson_acf2", StringType(), True),
    StructField("salesperson", StringType(), True),
    StructField("salesperson_source", StringType(), True),
    StructField("dv01_trade_currency", DecimalType(18,6), True),
    StructField("dv01_cad", DecimalType(18,6), True),
    StructField("dv01_usd", DecimalType(18,6), True),
    StructField("cad_usd_fx_rate", DecimalType(18,12), True),
    StructField("src_cad_fx_rate", DecimalType(18,12), True),
    StructField("c360_cash_spread", DecimalType(18,8), True),
    StructField("c360_yield_spread", DecimalType(18,8), True),
    StructField("c360_estimated_spread", DecimalType(18,8), True),
    StructField("c360_cash_ask", DecimalType(18,8), True),
    StructField("c360_cash_bid", DecimalType(18,8), True),
    StructField("c360_yield_ask", DecimalType(18,8), True),
    StructField("c360_yield_bid", DecimalType(18,8), True),
    StructField("c360_cash_spread_source", StringType(), True),
    StructField("c360_yield_spread_source", StringType(), True),
    StructField("client_value_cad", DecimalType(18,7), True),
    StructField("client_value_usd", DecimalType(18,7), True),
    StructField("client_value_markup", DecimalType(18,7), True),
    StructField("precomputed_client_value_cad", DecimalType(18,6), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("file_name_path", StringType(), True),
])


# COMMAND ----------

# Create Table (managed table - no LOCATION clause under UC)
SILVER_ALL_DATES_TABLE = f"`{CATALOG_NAME}`.`silver`.`all_dates_gfi_panoramic`"

_schema_ddl = ",\n".join(
    [f"`{f.name}` {f.dataType.simpleString()}" for f in all_dates_silver_schema.fields]
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_ALL_DATES_TABLE}
(
{_schema_ddl}
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

@enhanced_errors()
def apply_schema_data_types(df, target_schema):
    existing_columns = set(df.columns)

    schema_dict = {field.name: field.dataType for field in target_schema.fields}

    for column_name, data_type in schema_dict.items():
        if column_name in existing_columns:
            df = df.withColumn(column_name, F.col(column_name).cast(data_type))

    return df

# COMMAND ----------

#Trade table references (fully-qualified UC three-level names)
# NOTE: "panoramic" and "td_cowen_wra" do not appear in create_uc_tables_adf.py's
# table mapping list - confirm these are the correct UC table names before running.
SILVER_PANORAMIC_PATH = f"`{CATALOG_NAME}`.`silver`.`panoramic`"
SILVER_X_DEALER_PATH = f"`{CATALOG_NAME}`.`silver`.`x_dealer`"
SILVER_CAD_PRIME_MONTHLY_PATH = f"`{CATALOG_NAME}`.`silver`.`cad_prime_monthly`"
SILVER_CAD_PRIME_DAILY_PATH = f"`{CATALOG_NAME}`.`silver`.`cad_prime_pnl_summary`"
SILVER_CAD_PRIME_CV_SB_DAILY_PATH = f"`{CATALOG_NAME}`.`silver`.`cad_prime_securities_lending`"
SILVER_COWEN_PATH = f"`{CATALOG_NAME}`.`silver`.`td_cowen_equities`"
SILVER_US_PRIME_PATH = f"`{CATALOG_NAME}`.`silver`.`us_prime`"
SILVER_FUND_FINANCE_CFS_PATH = f"`{CATALOG_NAME}`.`silver`.`fund_financing_cfs`"
SILVER_FUND_FINANCE_ACT_PATH = f"`{CATALOG_NAME}`.`silver`.`fund_financing_act`"
SILVER_WRA = f"`{CATALOG_NAME}`.`silver`.`td_cowen_wra`"

BRONZE_CLIENT_MAPPINGS_PATH = f"`{CATALOG_NAME}`.`bronze`.`clients`"
bronze_paam_clients_path = f"`{CATALOG_NAME}`.`bronze`.`paam_clients`"
SILVER_FX_RATES_PATH = f"`{CATALOG_NAME}`.`silver`.`fx_rates`"

# COMMAND ----------

### Panoramic (FI, FX, Equities (GED Options FY26 onwards))

# COMMAND ----------

pano = spark.table(SILVER_PANORAMIC_PATH)

# COMMAND ----------

existing_cols = set(pano.columns)
for field in all_dates_silver_schema.fields:
    if field.name not in existing_cols:
        pano = pano.withColumn(field.name, F.lit(None).cast(field.dataType))

# 2. Build a stable, ordered column list: schema columns first, then any pano-only extras
schema_cols = [f.name for f in all_dates_silver_schema.fields]
extra_cols = [c for c in pano.columns if c not in schema_cols]
ordered_cols = schema_cols + extra_cols

# 3. Apply the same order to pano
pano = pano.select(ordered_cols)

# COMMAND ----------

pano = apply_schema_data_types(pano, all_dates_silver_schema)

# COMMAND ----------

### X_Dealer (Without CAD PRIME)

# COMMAND ----------

x_dealer = spark.table(SILVER_X_DEALER_PATH)

# COMMAND ----------

x_dealer = (x_dealer
            .withColumn("is_client_facing", F.lit("1"))
            .withColumn("volume_trade_currency", F.regexp_replace(F.col("volume_trade_currency"), ",", "").cast("decimal(18,5)"))
            .withColumn("maturity_date", F.col("maturity_date").cast("date"))
)

# COMMAND ----------

x_dealer = apply_schema_data_types(x_dealer, all_dates_silver_schema)

# COMMAND ----------

# GFI PANO CHANGE: Filter HY Loans trades and TDSAT from x_dealer because gfi pano overrides it
x_dealer = x_dealer.filter(
    (x_dealer['source_system'] != 'HY_LOANS')
    & (x_dealer['source_system'] != 'TDSAT')
)

# COMMAND ----------

# Filter out GED Options trades in FY26 or later
x_dealer = x_dealer.filter(
    ~(
        (F.col("product_level_0") == "GED")
        & (F.col("product_level_1") == "Options")
        & (F.col("trade_date") > F.lit("2025-10-31").cast("date"))
    ).eqNullSafe(True)
)

# COMMAND ----------

#Ignore trades with null or empty counterparty code. Desk only wants to see CV with for real counterparty codes
x_dealer = x_dealer.filter((F.col("counterparty_code").isNotNull()) & (F.trim(F.col("counterparty_code")) != ""))


# COMMAND ----------

### CAD PRIME Monthly (Up to July 2025)

# COMMAND ----------

cad_prime_monthly = (cad_prime_monthly
                     .withColumn("is_client_facing", F.lit("1"))
                     .withColumn("volume_trade_currency", F.regexp_replace(F.col("volume_trade_currency"), ",", "").cast("decimal(18,5)"))
                     .withColumn("maturity_date", F.col("maturity_date").cast("date"))
)

# COMMAND ----------

cad_prime_monthly = apply_schema_data_types(cad_prime_monthly, all_dates_silver_schema)

# COMMAND ----------

cad_prime_monthly = cad_prime_monthly.filter(F.col("trade_date") <= F.lit("2025-07-31"))

# COMMAND ----------

### CAD PRIME Daily (August 2025 Onwards)

# COMMAND ----------

cad_prime_daily = spark.sql(f"""
    select b.Firm_Name, s.*
    from `{CATALOG_NAME}`.`bronze`.`cad_prime_pnl_summary` b, `{CATALOG_NAME}`.`silver`.`cad_prime_pnl_summary` s
    where lower(trim(b.Firm_Name)) = s.client_display_name
    and s.trade_date = b.Trade_Date
""").withColumn('client_display_name', F.trim(F.column('Firm_Name'))).drop('Firm_Name')


# COMMAND ----------

cad_prime_daily = (cad_prime_daily
                   .withColumnRenamed("client_value", "client_value_cad")
                   .withColumnRenamed("trade_system", "source_system")
                   .withColumnRenamed("product_class_type", "product_level_2")
                   .withColumnRenamed("update_time", "ingestion_timestamp")
)

# COMMAND ----------

cad_prime_daily = (cad_prime_daily
                   .withColumn("is_client_facing", F.lit("1"))
                   .withColumn("product_level_0", F.lit("Prime"))
                   .withColumn("product_level_1", F.lit("Canadian Prime Broker"))
)

# COMMAND ----------

#Keep only trades on August 2025 and after
cad_prime_daily = cad_prime_daily.filter(F.col("trade_date") >= F.lit("2025-08-01"))

# COMMAND ----------

#Ignore trades with null or empty counterparty code. Desk only wants to see CV with for real counterparty codes.
cad_prime_daily = cad_prime_daily.filter((F.col("counterparty_code").isNotNull()) & (F.trim(F.col("counterparty_code")) != ""))

# COMMAND ----------

### CAD PRIME CV Securities Borrowed Daily

# COMMAND ----------

cad_prime_cv_sb_daily = spark.sql(f"""
    select s.*
    from `{CATALOG_NAME}`.`silver`.`cad_prime_securities_lending` s
    -- select s.*
    -- from `{CATALOG_NAME}`.`silver`.`cad_prime_securities_lending` s
    -- inner join (
    --     select distinct to_date(regexp_extract(Source_File, '(\\d{{2}}-\\d{{2}}-\\d{{4}})', 1), 'MM-dd-yyyy') as bronze_trade_date
    --     from `{CATALOG_NAME}`.`bronze`.`cad_prime_securities_lending`
    --     where Source_File is not null
    --       and regexp_extract(Source_File, '(\\d{{2}}-\\d{{2}}-\\d{{4}})', 1) != ''
    -- ) b on s.trade_date = b.bronze_trade_date
""")

# COMMAND ----------

cad_prime_cv_sb_daily = (
    cad_prime_cv_sb_daily
        .withColumnRenamed("client_value", "client_value_cad")
        .withColumnRenamed("update_time", "ingestion_timestamp")
        .withColumnRenamed("source_file", "file_name_path")
)

# COMMAND ----------

cad_prime_cv_sb_daily = (
    cad_prime_cv_sb_daily
        .withColumn("is_client_facing", F.lit("1"))
)

# COMMAND ----------

cad_prime_cv_sb_daily = apply_schema_data_types(cad_prime_cv_sb_daily, all_dates_silver_schema)

# COMMAND ----------

#Ignore trades with null or empty counterparty code. Desk only wants to see CV with for real counterparty codes.
cad_prime_cv_sb_daily = cad_prime_cv_sb_daily.filter((F.col("counterparty_code").isNotNull()) & (F.trim(F.col("counterparty_code")) != ""))

# COMMAND ----------

### Cowen

# COMMAND ----------

cowen = spark.table(SILVER_COWEN_PATH)

# COMMAND ----------

#Rename Cowen
cowen = (cowen
         .withColumnRenamed("slvr_ingestion_timestamp", "ingestion_timestamp")
)

# COMMAND ----------

# Cast Correct Data Types
cowen = (cowen
         .withColumn("trade_currency", F.lit("USD"))
         .withColumn("volume_usd_mm", (F.col("quantity").cast("decimal(18,6)")/1000000))
         .withColumn("is_client_facing", F.lit("1"))
)

# COMMAND ----------

cowen = apply_schema_data_types(cowen, all_dates_silver_schema)

# COMMAND ----------

### US Prime

# COMMAND ----------

us_prime = spark.table(SILVER_US_PRIME_PATH)

# COMMAND ----------

#Rename Cowen
us_prime = (us_prime
            .withColumnRenamed("slvr_ingestion_timestamp", "ingestion_timestamp")
)

# COMMAND ----------

# Cast Correct Data Types
us_prime = (us_prime
            .withColumn("trade_currency", F.lit("USD"))
            .withColumn("is_client_facing", F.lit("1"))
)

# COMMAND ----------

us_prime = apply_schema_data_types(us_prime, all_dates_silver_schema)

# COMMAND ----------

### Fund Financing (CFS)

# COMMAND ----------

fund_finance_cfs = spark.table(SILVER_FUND_FINANCE_CFS_PATH)

# COMMAND ----------

fund_finance_cfs = apply_schema_data_types(fund_finance_cfs, all_dates_silver_schema)

# COMMAND ----------

### Fund Financing (ACT)

# COMMAND ----------

fund_finance_act = spark.table(SILVER_FUND_FINANCE_ACT_PATH)

# COMMAND ----------

fund_finance_act = apply_schema_data_types(fund_finance_act, all_dates_silver_schema)

# COMMAND ----------

### TD Cowen WRA

# COMMAND ----------

wra = spark.table(SILVER_WRA)

# COMMAND ----------

wra = apply_schema_data_types(wra, all_dates_silver_schema)

# COMMAND ----------

# Combine Trades datasets

# COMMAND ----------

def align_to_schema(df, schema):
    schema_types = {f.name: f.dataType for f in schema.fields}
    for name, dtype in schema_types.items():
        if name in df.columns:
            df = df.withColumn(name, F.col(name).cast(dtype))
    return df

# COMMAND ----------

@enhanced_errors()
def concat_dataframes(dataframes):
    aligned = [align_to_schema(df, all_dates_silver_schema) for df in dataframes]
    return reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), aligned)


# COMMAND ----------

trades_list = [pano, x_dealer, cowen, cad_prime_monthly, cad_prime_daily, cad_prime_cv_sb_daily, us_prime, fund_finance_cfs, fund_finance_act, wra]

# COMMAND ----------

ad = concat_dataframes(trades_list)

# COMMAND ----------

# FX Conversion

# COMMAND ----------

fx_rates = spark.table(SILVER_FX_RATES_PATH)

# COMMAND ----------

src_cad_fx = fx_rates.filter(F.col("versusCurrency") == "CAD").select("fxDate", "tradeCurrency", "versusCurrency", "fxRate")

# COMMAND ----------

# SRC CAD FX Rate
all_dates = (ad.alias("trades")
             .join(src_cad_fx.alias("fx"),
                   (F.col("trades.trade_date") == F.col("fx.fxDate"))
                   & (F.col("trades.trade_currency") == F.col("fx.tradeCurrency")),
                   "left")
             .select("trades.*", F.coalesce(F.col("trades.src_cad_fx_rate"), F.col("fx.fxRate")).alias("src_cad_fx_rate_new"))
             .drop(F.col("trades.src_cad_fx_rate"))
             .withColumnRenamed("src_cad_fx_rate_new", "src_cad_fx_rate")
)

# COMMAND ----------

all_dates = all_dates.withColumn("src_cad_fx_rate", F.when(F.col("trade_currency") == 'CAD', F.lit(1)).otherwise(F.col("src_cad_fx_rate")))

# COMMAND ----------

cad_usd_fx = fx_rates.filter((F.col("tradeCurrency") == "CAD") & (F.col("versusCurrency") == "USD")).select("fxDate", "tradeCurrency", "versusCurrency", "fxRate")

# COMMAND ----------

#CAD USD FX RATE
all_dates = (all_dates.alias("trades")
             .join(cad_usd_fx.alias("fx"),
                   (F.col("trades.trade_date") == F.col("fx.fxDate")),
                   "left")
             .select("trades.*", F.coalesce(F.col("trades.cad_usd_fx_rate"), F.col("fx.fxRate")).alias("cad_usd_fx_rate_new"))
             .drop(F.col("trades.cad_usd_fx_rate"))
             .withColumnRenamed("cad_usd_fx_rate_new", "cad_usd_fx_rate")
)

# COMMAND ----------

### Do Currency Exchange for CV, Volume and DV01

# COMMAND ----------

@enhanced_errors()
def apply_fx_conversions(df):
    df = (df
          .withColumn('volume_trade_currency_mm', F.col('volume_trade_currency')/1000000)
          .withColumn("volume_cad_mm",
                      F.when((F.col("volume_cad_mm").isNull()) & (F.col("src_cad_fx_rate").isNotNull()) & (F.col("volume_trade_currency_mm").isNotNull()),
                             (F.col("volume_trade_currency_mm") * F.col("src_cad_fx_rate"))).otherwise(F.col("volume_cad_mm")))
          .withColumn("volume_cad_mm",
                      F.when((F.col("volume_cad_mm").isNull()) & (F.col("cad_usd_fx_rate").isNotNull()) & (F.col("volume_usd_mm").isNotNull()),
                             (F.col("volume_usd_mm") * (1/F.col("cad_usd_fx_rate")))).otherwise(F.col("volume_cad_mm")))
          .withColumn("volume_usd_mm", F.when((F.col("volume_usd_mm").isNull()) & (F.col("volume_cad_mm").isNotNull()) & (F.col("cad_usd_fx_rate").isNotNull()),
                                             F.col("volume_cad_mm") * F.col("cad_usd_fx_rate"))
                                      .otherwise(F.col("volume_usd_mm")))
          .withColumn("client_value_cad", F.when((F.col("client_value_cad").isNull()) & (F.col("cad_usd_fx_rate").isNotNull()),
                                                 F.col("client_value_usd") * (1/F.col("cad_usd_fx_rate")))
                                          .otherwise(F.col("client_value_cad")))
          .withColumn("client_value_usd", F.when((F.col("client_value_usd").isNull()) & (F.col("cad_usd_fx_rate").isNotNull()),
                                                 F.col("client_value_cad") * F.col("cad_usd_fx_rate"))
                                          .otherwise(F.col("client_value_usd")))
          .withColumn("dv01_cad",
                      F.when((F.col("src_cad_fx_rate").isNotNull()) & (F.col("dv01_trade_currency").isNotNull()),
                             F.col("dv01_trade_currency") * F.col("src_cad_fx_rate")).otherwise(F.lit(None)))
          .withColumn("dv01_usd", F.when((F.col("dv01_cad").isNotNull()) & (F.col("cad_usd_fx_rate").isNotNull()),
                                         F.col("dv01_cad") * F.col("cad_usd_fx_rate"))
                                  .otherwise(F.lit(None)))
    )
    return df

# COMMAND ----------

all_dates = apply_fx_conversions(all_dates)

# COMMAND ----------

# Client Mappings

# COMMAND ----------

#Ref Data
ref_data_clients = spark.table(BRONZE_CLIENT_MAPPINGS_PATH)

# COMMAND ----------

ref_data_clients = ref_data_clients.select(
    F.col("sourceSystem").alias("c_source_system"),
    F.col("shortCode").alias("c_counterparty_code"),
    F.col("client.displayName").alias("c_client_display_name"),
    F.col("client.type.title").alias("c_client_display_name_type"),
    F.col("client.topLevelClient.displayName").alias("c_client_display_name_top_level"),
    F.col("client.topLevelClient.type.title").alias("c_client_display_name_top_level_type"),
    F.col("client.focusedAccount").alias("c_focused_account")
)


# COMMAND ----------

#PAAM Data
paam_clients = spark.table(bronze_paam_clients_path)

# COMMAND ----------

paam_clients = paam_clients.select(
    F.col("source_system").alias("p_source_system"),
    F.col("counterparty_code").alias("p_counterparty_code"),
    F.col("client_display_name").alias("p_client_display_name"),
    F.col("client_display_name_type").alias("p_client_display_name_type"),
    F.col("client_display_name_top_level").alias("p_client_display_name_top_level"),
    F.col("client_display_name_top_level_type").alias("p_client_display_name_top_level_type"),
)

# COMMAND ----------

#Keep the raw_client_name if it is not null, else populate raw_client_name with client_display_name
# if client_display_name not null otherwise populate raw_client_name with counterparty_code
all_dates = all_dates.withColumn('raw_client_name', F.coalesce(F.col("raw_client_name"), F.col("client_display_name"), F.col("counterparty_code")))

# COMMAND ----------

#Need to ignore all trades coming from GFI/FX Pano
pano_source_systems = [
    row.source_system
    for row in pano.select("source_system")
        .na.drop()
        .distinct()
        .collect()
]
all_dates_refresh_clients = all_dates.filter(~F.col("source_system").isin(pano_source_systems))
all_dates_no_client_refresh = all_dates.filter(F.col("source_system").isin(pano_source_systems))

all_dates_refresh_clients = (all_dates_refresh_clients
                             .join(ref_data_clients, (F.col("source_system") == F.col("c_source_system"))
                                   & (F.col("counterparty_code") == F.col("c_counterparty_code")), "left")
                             .join(paam_clients, (F.col("source_system") == F.col("p_source_system"))
                                   & (F.col("counterparty_code") == F.col("p_counterparty_code")), "left")
)

use_ref_data = F.col('c_client_display_name').isNotNull()
use_paam_data = F.col('p_client_display_name').isNotNull()

all_dates_refresh_clients = (all_dates_refresh_clients
                             .withColumn('client_display_name', F.coalesce(F.col('c_client_display_name'), F.col('p_client_display_name'), F.col('raw_client_name')))
                             .withColumn('client_display_name_type',
                                         F.when(use_ref_data, F.col('c_client_display_name_type'))
                                          .when(use_paam_data, F.col('p_client_display_name_type')).otherwise(F.lit(None)))
                             .withColumn('client_display_name_top_level',
                                         F.when(use_ref_data, F.col('c_client_display_name_top_level'))
                                          .when(use_paam_data, F.col('p_client_display_name_top_level')).otherwise(F.lit(None)))
                             .withColumn('client_display_name_top_level_type',
                                         F.when(use_ref_data, F.col('c_client_display_name_top_level_type'))
                                          .when(use_paam_data, F.col('p_client_display_name_top_level_type')).otherwise(F.lit(None)))
                             .withColumn('focused_account', F.when(use_ref_data, F.col("c_focused_account")).otherwise(F.lit(None)).cast("string"))
)

all_dates = concat_dataframes([all_dates_refresh_clients, all_dates_no_client_refresh])

# COMMAND ----------

# Salesperson Mappings

# COMMAND ----------

@enhanced_errors()
def _get_access_token():
    resp = requests.post(
        url=AUTH_URL,
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        data={
            'grant_type': 'client_credentials',
            'client_id': f'{CLIENT_ID}',
            'client_secret': f'{CLIENT_SECRET}',
            'scope': 'roles email audience',
        },
        verify=False
    )


    if resp.ok:
        token = resp.json()
        return token['access_token']

# COMMAND ----------

@enhanced_errors()
def _post_ref_data(endpoint):
    url = f"{REFERENCE_DATA_URL}{endpoint}"
    token = _get_access_token()

    headers = { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Accept': 'application/octet-stream' }

    PAYLOAD = {
        "draw": 1,
        "columns": [
            {
                "data": "id",
                "name": "id",
                "searchable": True,
                "orderable": True,
                "search": {
                    "value": "",
                    "regex": False
                }
            }

        ],
        "order": [
            {
                "column": 0,
                "dir": "asc"
            }
        ],
        "start": 0,
        "length": 10,
        "search": {
            "value": "",
            "regex": False
        }
    }
    print(headers)

    return requests.post(url=url, headers=headers, stream=True, json=PAYLOAD, verify=False)

# COMMAND ----------

@enhanced_errors()
def _get_ref_data(endpoint):
    token = _get_access_token()
    url = f"{REFERENCE_DATA_URL}{endpoint}"
    headers = {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response = requests.get(
        url=url,
        headers=headers,
        verify=False
    )

    return response

# COMMAND ----------

@enhanced_errors()
def _get_trading_id_lookups():
    endpoint = "/trading-id-lookups/export-to-csv"
    response = _post_ref_data(
        endpoint=endpoint,
    )
    response_string = response.content.decode()
    rdd = spark.sparkContext.parallelize(response_string.splitlines())
    trade_id_lookups = spark.read.csv(rdd, header=True, inferSchema=True)

    return trade_id_lookups

# COMMAND ----------

@enhanced_errors()
def _get_internal_org_mappings():
    endpoint = "/internal-organizations/export-to-csv"
    response = _post_ref_data(
        endpoint=endpoint,
    )

    #org_mappings = pd.read_csv(io.StringIO(response.content.decode()))
    response_string = response.content.decode()
    rdd = spark.sparkContext.parallelize(response_string.splitlines())
    org_mappings = spark.read.csv(rdd, header=True, inferSchema=True)

    return org_mappings


# COMMAND ----------

@enhanced_errors()
def _get_internal_sales_support_attribute():
    #url = f"{REFERENCE_DATA_URL}/internal-organizations/export-to-csv"
    response = _get_ref_data('/internal-sales-support-attributes')

    json = response.json()['InternalSalesSupportAttributes']


    sales_support_attributes = spark.createDataFrame(json)

    return sales_support_attributes

# COMMAND ----------

@enhanced_errors()
def _standardize_internal_org_mappings(df):
    df = df.withColumn("salesperson", F.upper(F.concat_ws(" ", F.col("firstName"), F.col("lastName"))))
    df = df.drop(*[
        'id', 'version', 'startDate', 'active', 'createdBy', 'createdDateTime',
        'departmentId', 'departmentName', 'preferredName', 'firstName',
        'lastName', 'email', 'description', 'endDate', 'jobTitle', 'city',
        'country', 'supervisorName', 'supervisorAcf2', 'mdsUsername',
        'globalLeader', 'globalBusiness', 'business', 'desk', 'role',
        'foSupervisor', 'orgStructure6', 'orgStructure7'
    ])
    df = df.withColumn("acf2Id", F.upper(F.col("acf2Id")))

    return df

# COMMAND ----------

@enhanced_errors()
def _replace_column_with_acf2Id(df, trading_id_lookups, column_to_replace):
    df = (df.alias("trades")
          .join(trading_id_lookups.alias("trading_ids"),
                (F.col("trades.source_system") == F.col("trading_ids.sourceSystem")) &
                (F.col(f"trades.{column_to_replace}") == F.col("trading_ids.tradingId")), "left")
          .select("trades.*", F.coalesce(F.col("trading_ids.acf2Id"), F.col(f"trades.{column_to_replace}")).alias(f"{column_to_replace}_new"))
          .drop(F.col(f"trades.{column_to_replace}"))
          .withColumnRenamed(f"{column_to_replace}_new", column_to_replace)
    )
    return df

# COMMAND ----------

@enhanced_errors()
def _apply_salesperson_from_trade(df):
    df = df.withColumn("salesperson_source", F.lit(None))

    trading_id_lookups = _get_trading_id_lookups()
    trading_id_lookups = trading_id_lookups.select(F.col("sourceSystem"), F.col("tradingId"), F.col("acf2Id"))
    #Remove Duplicate sourceSystem and tradingId rows (Keep First)
    trading_id_lookups = trading_id_lookups.dropDuplicates(["sourceSystem", "tradingId"])

    df = _replace_column_with_acf2Id(df, trading_id_lookups, "salesperson_acf2")
    df = _replace_column_with_acf2Id(df, trading_id_lookups, "trader")

    internal_org_mappings = _get_internal_org_mappings()
    org_mappings = _standardize_internal_org_mappings(internal_org_mappings)

    org_mappings_distinct_acf2Id = [row.acf2Id for row in org_mappings.select("acf2Id").distinct().collect()]

    df = df.withColumn("salesperson_source",
                       F.when(F.col("salesperson_acf2").isin(org_mappings_distinct_acf2Id),
                              F.lit("Ticketed By")).otherwise(F.col("salesperson_source")))

    df = df.withColumn("salesperson_source",
                       F.when((F.col("salesperson_acf2").isin(org_mappings_distinct_acf2Id)) & (F.col("trader").isin(org_mappings_distinct_acf2Id)),
                              F.lit("Trader On Ticket")).otherwise(F.col("salesperson_source")))

    df = df.withColumn("salesperson_acf2", F.when(F.col("salesperson_source") == "Trader On Ticket", F.col("trader")).otherwise(F.col("salesperson_acf2")))


    df = (df.alias("trades").join(
        org_mappings.alias("org_mappings"),
        F.col("trades.salesperson_acf2") == F.col("org_mappings.acf2Id"), "left")
        .select("trades.*", F.coalesce(F.col("trades.salesperson"), F.col("org_mappings.salesperson")).alias("salesperson_new"))
        .drop(F.col("trades.salesperson"))
        .withColumnRenamed("salesperson_new", "salesperson")
    )


    internal_ss_attr_mappings = _get_internal_sales_support_attribute()


    return df

# COMMAND ----------

all_dates = _apply_salesperson_from_trade(all_dates)

# COMMAND ----------

#Final Clean up and Standardizations

# COMMAND ----------

@enhanced_errors()
def _standardize_boolean_columns(df, col_name, yes_value, no_value, none_value):

    true_like_values = ['1', 'true']
    false_like_values = ['0', 'false', 'nan']

    df = df.withColumn(
        col_name,
        F.when(F.trim(F.lower(F.col(col_name))).isin(true_like_values), F.lit(yes_value))
        .when(F.trim(F.lower(F.col(col_name))).isin(false_like_values), F.lit(no_value))
        .when(F.col(col_name).isNull(), F.lit(none_value)).otherwise(F.col(col_name))
    )

    return df

# COMMAND ----------

@enhanced_errors()
def _replace_empty_str_with_value(df, col_name, null_value):
    df = df.withColumn(col_name, F.when((F.col(col_name).isNull()) | (F.trim(F.col(col_name))==""), null_value).otherwise(F.col(col_name)))
    return df

# COMMAND ----------

@enhanced_errors()
def _get_maturity_bucket(df):
    df = df.withColumn(
        'maturity_bucket',
        F.when(F.col('days_to_maturity') > 3650, F.lit('4: >10Y'))
        .when(F.col('days_to_maturity') <= 3650, F.lit('3: 5-10Y'))
        .when(F.col('days_to_maturity') <= 1825, F.lit('2: 2-5Y'))
        .when(F.col('days_to_maturity') <= 730, F.lit('1: 0-2Y'))
        .otherwise('N/A')
    )
    return df

# COMMAND ----------

@enhanced_errors()
def final_standardizations(df):
    df = df.withColumn('year_month', F.date_format("trade_date", "yyyy MM"))

    df = df.withColumn("fiscal_trade_quarter",
                       F.concat(
                           F.lit("FY"),
                           F.when(F.month("trade_date") >= 11, F.year("trade_date")+1).otherwise(F.year("trade_date")), F.lit("Q"),
                           F.when(F.month("trade_date").isin(11,12,1), 1)
                            .when(F.month("trade_date").isin(2,3,4), 2)
                            .when(F.month("trade_date").isin(5,6,7), 3).otherwise(4)
                       )
    )
    df = df.withColumn("days_to_maturity", F.datediff("maturity_date", "trade_date").cast("integer"))
    df = df.withColumn("years_to_maturity", F.when(~F.col("days_to_maturity").isNull(),
                                                   F.format_string("%.5f", F.col("days_to_maturity")/365.25)).otherwise(F.lit(None)))

    #fiscal_trade_quarter
    df = _standardize_boolean_columns(df, 'is_client_facing', '1', '0', None)
    df = df.withColumn('focused_account', F.when(F.col('focused_account').isNull(), F.lit("Not Provided"))
                       .when(F.col('focused_account')==True, F.lit('Yes')).otherwise(F.lit('No'))
    )

    for product_level in ['product_level_0', 'product_level_1', 'product_level_2', 'product_level_3']:
        df = _replace_empty_str_with_value(df, product_level, "Unmapped Product")

    df = _replace_empty_str_with_value(df, "salesperson", "NOT PROVIDED")
    df = _replace_empty_str_with_value(df, "client_display_name", "Unmapped Client")

    for col in ['salesperson_source', 'security_description', 'security_id']:
        df = _replace_empty_str_with_value(df, col, "Not Available")

    df = _get_maturity_bucket(df)

    df = _standardize_boolean_columns(df, 'risk_on', 'True', 'False', None)

    return df

# COMMAND ----------

all_dates = final_standardizations(all_dates)

# COMMAND ----------

# Final Data Model

# COMMAND ----------

all_dates = apply_schema_data_types(all_dates, all_dates_silver_schema)

# COMMAND ----------

#Final Schema definition
schema_columns = [field.name for field in all_dates_silver_schema.fields]

all_dates = all_dates.select([F.col(column) for column in schema_columns])

# COMMAND ----------

#Save to Silver Table

# COMMAND ----------

# TODO: Ask Aaron if we can do a merge instead of an overwrite for these products
all_dates.write.format("delta").mode("overwrite").saveAsTable(SILVER_ALL_DATES_TABLE)