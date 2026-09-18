# Databricks notebook source
# Databricks Notebook Source
import os

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# MAGIC %run "./dateutils.py"

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

from pyspark.sql.types import FloatType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import LongType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import DateType
from pyspark.sql.functions import lit
from pyspark.sql.functions import input_file_name
from delta.tables import DeltaTable
import os

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Schema

# COMMAND ----------

# This is the read-in schema for all of the raw data. All irrelevant columns are given string type by default and will be filtered out before upload
fi_trade_bronze_schema = StructType(
    [
        StructField('securityPrincipalFactor', FloatType(), True),
        StructField('deskAdjustmentWeight', FloatType(), True),
        StructField('id', LongType(), True),
        StructField('c360Version', IntegerType(), True),
        StructField('tradeSystem', StringType(), True),
        StructField('tradeId', StringType(), True),
        StructField('tradeVersion', IntegerType(), True),
        StructField('tradeDate', DateType(), True),
        StructField('settleDate', DateType(), True),
        StructField('isClientFacing', BooleanType(), True),
        StructField('inventory', StringType(), True),
        StructField('c360ClientCode', StringType(), True),
        StructField('counterpartyCode', StringType(), True),
        StructField('counterpartyShortCode', StringType(), True),
        StructField('counterpartyName', StringType(), True),
        StructField('traderId', StringType(), True),
        StructField('salesPersonId', StringType(), True),
        StructField('settleCurrency', StringType(), True),
        StructField('settleAmountInSettleCurrency', StringType(), True),
        StructField('tradeCurrency', StringType(), True),
        StructField('settleAmountInTradeCurrency', FloatType(), True),
        StructField('commissionValue', FloatType(), True),
        StructField('tradeDescription', StringType(), True),
        StructField('tradeProductType', StringType(), True),
        StructField('tradeProductSubType', StringType(), True),
        StructField('tradeProductExtendedType', StringType(), True),
        StructField('tradeQuantity', IntegerType(), True),
        StructField('tradePrice', StringType(), True),
        StructField('marketDataKey', StringType(), True),
        StructField('tradeMaturityDate', DateType(), True),
        StructField('tradeSecurityKey', StringType(), True),
        StructField('parentTradeSystem', StringType(), True),
        StructField('securityMaturityDate', DateType(), True),
        StructField('parentTradeId', StringType(), True),
        StructField('securityKey', StringType(), True),
        StructField('securityDescription', StringType(), True),
        StructField('securityKeyType', StringType(), True),
        StructField('productLevel1', StringType(), True),
        StructField('productLevel2', StringType(), True),
        StructField('productLevel3', StringType(), True),
        StructField('productLevel4', StringType(), True),
        StructField('c360ProductClassId', StringType(), True),
        StructField('c360ProductClassVersion', StringType(), True),
        StructField('c360ProductClassName', StringType(), True),
        StructField('c360ProductClassDisplayName', StringType(), True),
        StructField('c360ProductClassTypeId', StringType(), True),
        StructField('c360ProductClassTypeVersion', StringType(), True),
        StructField('c360ProductClassTypeName', StringType(), True),
        StructField('c360ProductClassTypeDisplayName', StringType(), True),
        StructField('c360MaturityTermInDays', IntegerType(), True),
        StructField('c360DaysToMaturity', IntegerType(), True),
        StructField('tradeProductRepoDayCount', IntegerType(), True),
        StructField('c360Description', StringType(), True),
        StructField('tradeCalculatorType', StringType(), True),
        StructField('clientFacingRule', StringType(), True),
        StructField('tradeExecutionTypeRule', StringType(), True),
        StructField('productEnrichmentRule', StringType(), True),
        StructField('tdProductClassName', StringType(), True),
        StructField('tdProductClassTypeName', StringType(), True),
        StructField('c360TradeExecutionType', StringType(), True),
        StructField('c360IsRiskOn', StringType(), True),
        StructField('c360Direction', StringType(), True),
        StructField('c360TraderRegion', StringType(), True),
        StructField('c360TradeType', StringType(), True),
        StructField('c360MultiLegType', StringType(), True),
        StructField('c360MultiLegGroupKey', StringType(), True),
        StructField('c360CvProductId', StringType(), True),
        StructField('c360Quantity', IntegerType(), True),
        StructField('c360CashSpread', FloatType(), True),
        StructField('c360YieldSpread', FloatType(), True),
        StructField('c360EstimatedSpread', FloatType(), True),
        StructField('c360SettlementAmount', FloatType(), True),
        StructField('c360CashAsk', FloatType(), True),
        StructField('c360CashBid', FloatType(), True),
        StructField('c360YieldAsk', FloatType(), True),
        StructField('c360YieldBid', FloatType(), True),
        StructField('c360CashSpreadSource', StringType(), True),
        StructField('c360YieldSpreadSource', StringType(), True),
        StructField('c360EstimatedSpreadSource', StringType(), True),
        StructField('c360EstimatedSpreadTags', StringType(), True),
        StructField('c360CvCalculator', StringType(), True),
        StructField('c360CvWeight', StringType(), True),
        StructField('c360Dv01', FloatType(), True),
        StructField('c360Dv01Source', StringType(), True),
        StructField('c360RiskMetricValue', FloatType(), True),
        StructField('c360RiskMetricSource', StringType(), True),
        StructField('c360RiskMetricType', StringType(), True),
        StructField('cadUsdFxRate', FloatType(), True),
        StructField('srcCadFxRate', FloatType(), True),
        StructField('clientValueCad', FloatType(), True),
        StructField('preComputedClientValueCad', FloatType(), True),
        StructField('clientValueMarkupValueCad', FloatType(), True),
        StructField('clientValueMarkupSource', StringType(), True),
        StructField('etlStatus', StringType(), True),
        StructField('etlErrorCodes', StringType(), True),
        StructField('etlErrorDetails', StringType(), True),
        StructField('isActive', StringType(), True),
        StructField('tradeCreatedBy', StringType(), True),
        StructField('tradeCreatedDateTime', StringType(), True),
        StructField('tradeUpdatedBy', StringType(), True),
        StructField('tradeUpdatedDateTime', StringType(), True),
        StructField('tradeSecurityDetailsId', StringType(), True),
        StructField('tradeSecurityDetailsVersion', StringType(), True),
        StructField('tradeSecurityDetailsCreatedBy', StringType(), True),
        StructField('tradeSecurityDetailsCreatedDateTime', StringType(), True),
        StructField('tradeSecurityDetailsUpdatedBy', StringType(), True),
        StructField('tradeSecurityDetailsUpdatedDateTime', StringType(), True),
        StructField('tradeDetailsId', StringType(), True),
        StructField('tradeDetailsVersion', StringType(), True),
        StructField('tradeDetailsCreatedBy', StringType(), True),
        StructField('tradeDetailsCreatedDateTime', StringType(), True),
        StructField('tradeDetailsUpdatedBy', StringType(), True),
        StructField('tradeDetailsUpdatedDateTime', StringType(), True),
        StructField('tradeClientValueMarkupId', StringType(), True),
        StructField('tradeClientValueMarkupVersion', StringType(), True),
        StructField('tradeClientValueMarkupCreatedBy', StringType(), True),
        StructField('tradeClientValueMarkupCreatedDateTime', StringType(), True),
        StructField('tradeClientValueMarkupUpdatedBy', StringType(), True),
        StructField('tradeClientValueMarkupUpdatedDateTime', StringType(), True),
        StructField('tradeClientValueId', StringType(), True),
        StructField('tradeClientValueVersion', StringType(), True),
        StructField('tradeClientValueCreatedBy', StringType(), True),
        StructField('tradeClientValueCreatedDateTime', StringType(), True),
        StructField('tradeClientValueUpdatedBy', StringType(), True),
        StructField('tradeClientValueUpdatedDateTime', StringType(), True),
        StructField('tradeProductDetailsId', StringType(), True),
        StructField('tradeProductDetailsVersion', StringType(), True),
        StructField('tradeProductDetailsCreatedBy', StringType(), True),
        StructField('tradeProductDetailsCreatedDateTime', StringType(), True),
        StructField('tradeProductDetailsUpdatedBy', StringType(), True),
        StructField('tradeProductDetailsUpdatedDateTime', StringType(), True),
        StructField('etlStatusId', StringType(), True),
        StructField('etlStatusVersion', StringType(), True),
        StructField('etlStatusCreatedBy', StringType(), True),
        StructField('etlStatusCreatedDateTime', StringType(), True),
        StructField('etlStatusUpdatedBy', StringType(), True),
        StructField('etlStatusUpdatedDateTime', StringType(), True),
        StructField('auditDetailsId', StringType(), True),
        StructField('auditDetailsVersion', StringType(), True),
        StructField('auditDetailsCreatedBy', StringType(), True),
        StructField('auditDetailsCreatedDateTime', StringType(), True),
        StructField('auditDetailsUpdatedBy', StringType(), True),
        StructField('auditDetailsUpdatedDateTime', StringType(), True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Raw and Bronze Data Paths

# COMMAND ----------

# Trade System and Trade Date
dbutils.widgets.text('TRADE_DATE', '')
TRADE_DATE = string_to_date(dbutils.widgets.get('TRADE_DATE')).strftime('%Y-%m-%d')

dbutils.widgets.text('TRADE_SYSTEM', '')
TRADE_SYSTEM = dbutils.widgets.get('TRADE_SYSTEM')

trade_dt = string_to_date(TRADE_DATE)
raw_trade_dir = (
    f"{VOLUME_BASE_PATH}/trades/fixed_income/client_value_service/"
    f"{TRADE_SYSTEM}/{trade_dt.year}/{trade_dt.month}"
)

# The legacy Raw notebook writes single-underscore names, while this legacy
# Bronze notebook expected a double-underscore variant. Accept both so the UC
# migration does not depend on that pre-existing filename inconsistency.
candidate_filenames = [
    f"trades_client_value_service_{TRADE_SYSTEM}_{TRADE_DATE}.csv",
    f"trades__client_value_service__{TRADE_SYSTEM}__{TRADE_DATE}.csv",
]

raw_files = {f.name: f.path for f in dbutils.fs.ls(raw_trade_dir) if not f.isDir()}
client_value_service_trades_path = next(
    (raw_files[name] for name in candidate_filenames if name in raw_files),
    None,
)
if client_value_service_trades_path is None:
    raise FileNotFoundError(
        f"No FI raw trade file found in {raw_trade_dir}. "
        f"Expected one of: {candidate_filenames}"
    )

BRONZE_FI_TRADES_TABLE = f"`{CATALOG_NAME}`.`bronze`.`fi_trades`"

# COMMAND ----------

# MAGIC %md
# MAGIC # Load File

# COMMAND ----------

new_bronze_df = spark.read.csv(
    path=client_value_service_trades_path,
    schema=fi_trade_bronze_schema,
    header=True,
    # nullValue=None
)



# COMMAND ----------

# MAGIC %md
# MAGIC # Create If Not Exists

# COMMAND ----------

# Create or replace the Bronze table with specified location
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_FI_TRADES_TABLE} (
        securityPrincipalFactor FLOAT,
        deskAdjustmentWeight FLOAT,
        id LONG,
        c360Version INTEGER,
        tradeSystem STRING,
        tradeId STRING,
        tradeVersion INTEGER,
        tradeDate TIMESTAMP,
        settleDate TIMESTAMP,
        isClientFacing BOOLEAN,
        inventory STRING,
        c360ClientCode STRING,
        counterpartyCode STRING,
        counterpartyShortCode STRING,
        counterpartyName STRING,
        traderId STRING,
        salesPersonId STRING,
        settleCurrency STRING,
        settleAmountInSettleCurrency STRING,
        tradeCurrency STRING,
        settleAmountInTradeCurrency FLOAT,
        commissionValue FLOAT,
        tradeDescription STRING,
        tradeProductType STRING,
        tradeProductSubType STRING,
        tradeProductExtendedType STRING,
        tradeQuantity INTEGER,
        tradePrice STRING,
        marketDataKey STRING,
        tradeMaturityDate TIMESTAMP,
        tradeSecurityKey STRING,
        parentTradeSystem STRING,
        securityMaturityDate TIMESTAMP,
        parentTradeId STRING,
        securityKey STRING,
        securityDescription STRING,
        securityKeyType STRING,
        productLevel1 STRING,
        productLevel2 STRING,
        productLevel3 STRING,
        productLevel4 STRING,
        c360ProductClassId STRING,
        c360ProductClassVersion STRING,
        c360ProductClassName STRING,
        c360ProductClassDisplayName STRING,
        c360ProductClassTypeId STRING,
        c360ProductClassTypeVersion STRING,
        c360ProductClassTypeName STRING,
        c360ProductClassTypeDisplayName STRING,
        c360MaturityTermInDays INTEGER,
        c360DaysToMaturity INTEGER,
        tradeProductRepoDayCount INTEGER,
        c360Description STRING,
        tradeCalculatorType STRING,
        clientFacingRule STRING,
        tradeExecutionTypeRule STRING,
        productEnrichmentRule STRING,
        tdProductClassName STRING,
        tdProductClassTypeName STRING,
        c360TradeExecutionType STRING,
        c360IsRiskOn STRING,
        c360Direction STRING,
        c360TraderRegion STRING,
        c360TradeType STRING,
        c360MultiLegType STRING,
        c360MultiLegGroupKey STRING,
        c360CvProductId STRING,
        c360Quantity INTEGER,
        c360CashSpread FLOAT,
        c360YieldSpread FLOAT,
        c360EstimatedSpread FLOAT,
        c360SettlementAmount FLOAT,
        c360CashAsk FLOAT,
        c360CashBid FLOAT,
        c360YieldAsk FLOAT,
        c360YieldBid FLOAT,
        c360CashSpreadSource STRING,
        c360YieldSpreadSource STRING,
        c360EstimatedSpreadSource STRING,
        c360EstimatedSpreadTags STRING,
        c360CvCalculator STRING,
        c360CvWeight STRING,
        c360Dv01 FLOAT,
        c360Dv01Source STRING,
        c360RiskMetricValue FLOAT,
        c360RiskMetricSource STRING,
        c360RiskMetricType STRING,
        cadUsdFxRate FLOAT,
        srcCadFxRate FLOAT,
        clientValueCad FLOAT,
        preComputedClientValueCad FLOAT,
        clientValueMarkupValueCad FLOAT,
        clientValueMarkupSource STRING,
        etlStatus STRING,
        etlErrorCodes STRING,
        etlErrorDetails STRING,
        isActive STRING,
        tradeCreatedBy STRING,
        tradeCreatedDateTime STRING,
        tradeUpdatedBy STRING,
        tradeUpdatedDateTime STRING,
        tradeSecurityDetailsId STRING,
        tradeSecurityDetailsVersion STRING,
        tradeSecurityDetailsCreatedBy STRING,
        tradeSecurityDetailsCreatedDateTime STRING,
        tradeSecurityDetailsUpdatedBy STRING,
        tradeSecurityDetailsUpdatedDateTime STRING,
        tradeDetailsId STRING,
        tradeDetailsVersion STRING,
        tradeDetailsCreatedBy STRING,
        tradeDetailsCreatedDateTime STRING,
        tradeDetailsUpdatedBy STRING,
        tradeDetailsUpdatedDateTime STRING,
        tradeClientValueMarkupId STRING,
        tradeClientValueMarkupVersion STRING,
        tradeClientValueMarkupCreatedBy STRING,
        tradeClientValueMarkupCreatedDateTime STRING,
        tradeClientValueMarkupUpdatedBy STRING,
        tradeClientValueMarkupUpdatedDateTime STRING,
        tradeClientValueId STRING,
        tradeClientValueVersion STRING,
        tradeClientValueCreatedBy STRING,
        tradeClientValueCreatedDateTime STRING,
        tradeClientValueUpdatedBy STRING,
        tradeClientValueUpdatedDateTime STRING,
        tradeProductDetailsId STRING,
        tradeProductDetailsVersion STRING,
        tradeProductDetailsCreatedBy STRING,
        tradeProductDetailsCreatedDateTime STRING,
        tradeProductDetailsUpdatedBy STRING,
        tradeProductDetailsUpdatedDateTime STRING,
        etlStatusId STRING,
        etlStatusVersion STRING,
        etlStatusCreatedBy STRING,
        etlStatusCreatedDateTime STRING,
        etlStatusUpdatedBy STRING,
        etlStatusUpdatedDateTime STRING,
        auditDetailsId STRING,
        auditDetailsVersion STRING,
        auditDetailsCreatedBy STRING,
        auditDetailsCreatedDateTime STRING,
        auditDetailsUpdatedBy STRING,
        auditDetailsUpdatedDateTime STRING
    ) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Bronze

# COMMAND ----------

with capture_errors(section="fi_trades_bronze_update_with_schema_evolution"):
    # Get existing brone table
    existing_bronze_table = DeltaTable.forName(spark, BRONZE_FI_TRADES_TABLE)

    # Get current Schema automerge config variable so it can be reset properly after merge with schema evol
    original_auto_merge_setting = spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")

    # Update when the trade exists, insert when it doesn't
    try:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        existing_bronze_table.alias("bronze").merge(
            new_bronze_df.alias("bronze_new"),
            "bronze.tradeId = bronze_new.tradeId AND bronze.tradeSystem = bronze_new.tradeSystem"
        ).whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    finally:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", original_auto_merge_setting)

