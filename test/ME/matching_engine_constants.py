# Databricks notebook source
import json

from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DecimalType, ArrayType,
    FloatType, TimestampType, DateType
)

STOP_WORDS = ['asset management', 'capital management', 'investment management', 'group', 'investments', 'investment',
              'capital', 'financial', 'alliance', 'ssga', 'corporation', 'corp', 'incorporated', 'inc', 'limited', 'ltd',
              'company', 'co', 'group', 'holdings', 'partners', 'partnership', 'associates', 'organization', 'llc', 'llp',
              'plc', 'ag', 'sa', 'gmbh', 'nv', 'bv', 'srl', 'spa', 'pte', 'pvt', 'ltda', 'sdn', 'bhd', 'llp', 'lp', 'incorp',
              'intl', 'international', 'global', 'worldwide', 'enterprises'] + StopWordsRemover().getStopWords()

STOP_WORDS_AS_REGEX = '|'.join([s.lower() for s in STOP_WORDS])

# COMMAND ----------

def try_get(name: str):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return ""

# COMMAND ----------

prism_web_server_base_url = try_get('prism_web_server_base_url')
prism_auth_server = try_get('prism_auth_server')

auth_and_date_formats = try_get('auth_and_date_formats')

if auth_and_date_formats:
    auth_and_date_formats = json.loads(auth_and_date_formats)
else:
    raise ValueError('auth_and_date_formats must be supplied as an object')

python_datetime_format = auth_and_date_formats.get('python_datetime_format', "")
spark_datetime_format = auth_and_date_formats.get('spark_datetime_format', "")
rivendell_auth_scopes = auth_and_date_formats.get('rivendell_auth_scopes', "")

client_matching_web_server_base_url = try_get('client_matching_web_server_base_url')
cip_client_id = try_get('cip_client_id')
cip_client_secret = try_get('cip_client_secret')
prism_client_id = try_get('prism_client_id')
prism_client_secret = try_get('prism_client_secret')
auth_server = try_get('auth_server')
trigger_name = try_get('trigger_name')
trigger_start_time = try_get('trigger_start_time')
trigger_type = try_get('trigger_type')
trigger_schedule = try_get('trigger_schedule')

# COMMAND ----------

PRISM_LAST_MODIFIED_TABLE = f"`{CATALOG_NAME}`.`raw`.`prism_last_modified`"
PRISM_TABLE = f"`{CATALOG_NAME}`.`raw`.`prism_data`"
SUBORDINATE_TABLE = f"`{CATALOG_NAME}`.`raw`.`subordinate_data`"
CLIENT_CODE_MATCHES_TABLE = f"`{CATALOG_NAME}`.`raw`.`client_code_matches`"
ALL_DATES_TABLE = f"`{CATALOG_NAME}`.`silver`.`all_dates`"
PRISM_PROCESSING_STATE_TABLE = f"`{CATALOG_NAME}`.`raw`.`prism_processing_state`"
TRUEMATCH_ACCOUNT_DIFFS_TABLE = f"`{CATALOG_NAME}`.`raw`.`truematch_account_diffs`"
PRISM_ACCOUNT_FEED_VIEW = f"`{CATALOG_NAME}`.`raw`.`prism_account_feed`"
CLIENT_CODES_AGGREGATES_TABLE = f"`{CATALOG_NAME}`.`raw`.`client_codes_aggregates`"
PRISM_WEB_SERVER_BASE_URL = prism_web_server_base_url
PRISM_AUTH_SERVER = prism_auth_server
PYTHON_DATETIME_FORMAT = python_datetime_format
SPARK_DATETIME_FORMAT = spark_datetime_format
CLIENT_MATCHING_WEB_SERVER_BASE_URL = client_matching_web_server_base_url
CIP_CLIENT_ID = cip_client_id
CIP_CLIENT_SECRET = cip_client_secret
PRISM_CLIENT_ID = prism_client_id
PRISM_CLIENT_SECRET = prism_client_secret
AUTH_SERVER = auth_server
RIVENDELL_AUTH_SCOPES = rivendell_auth_scopes
TRIGGER_NAME = trigger_name
TRIGGER_START_TIME = trigger_start_time
TRIGGER_TYPE = trigger_type
TRIGGER_SCHEDULE = trigger_schedule

CLIENT_MAPPING_SOURCE = 'C360'
SYSTEM_ID = 1
NON_MATCHING_PRISM_ID = '#NON_MATCH_ID'
UNMATCHED = 'unmatched'
RATIO_WEIGHT = 'RATIO_WEIGHT'
JW_PENALTY_EXP = 'JW_PENALTY_EXP'
JW_REWARD = 'JW_REWARD'
DELTA_MERGE_DELAY = 'DELTA_MERGE_DELAY'
DELTA_MERGE_RETRIES = 'DELTA_MERGE_RETRIES'
DELTA_MERGE_MAX_DELAY = 'DELTA_MERGE_MAX_DELAY'
DELTA_MERGE_BACKOFF = 'DELTA_MERGE_BACKOFF'
HTTP_ERROR_DELAY = 'HTTP_ERROR_DELAY'
HTTP_ERROR_RETRIES = 'HTTP_ERROR_RETRIES'
HTTP_ERROR_MAX_DELAY = 'HTTP_ERROR_MAX_DELAY'
HTTP_ERROR_BACKOFF = 'HTTP_ERROR_BACKOFF'
MATCHING_LOWER_BOUND = 'MATCHING_LOWER_BOUND'
PRISM_ACCOUNT_TYPE_IN_FILTER = "('investor')"
PRISM_ACCOUNT_SUBTYPE_IN_FILTER = "('account', 'prospect')"

PRISM_FIELDS = [
    'account_id',
    'account_name',
    'account_type',
    'account_status',
    'account_subtype',
    'account_parent_id',
    'account_address_1',
    'account_address_2',
    'account_address_3',
    'account_city',
    'account_state',
    'account_zipcode',
    'account_country',
    'account_region',
    'account_website',
    'account_tier',
    'account_short_code',
    'alias_name',
    'account_parent_name',
    'created_by',
    'cusip_id',
    'ownership_status',
    'is_parent',
    'investorType',
    'filing_date',
    'ownership_struct',
    'banking_account_tier',
    'sector_name',
    'classification_2',
    'merged_accounts',
    'merged_with'
]


PRISM_SCHEMA = StructType([
    StructField('account_id', StringType(), nullable=False),
    StructField('account_name', StringType(), nullable=False),
    StructField('account_type', StringType(), nullable=True),
    StructField('account_status', StringType(), nullable=True),
    StructField('account_subtype', StringType(), nullable=True),
    StructField('account_parent_id', StringType(), nullable=True),
    StructField('account_address_1', StringType(), nullable=True),
    StructField('account_address_2', StringType(), nullable=True),
    StructField('account_address_3', StringType(), nullable=True),
    StructField('account_city', StringType(), nullable=True),
    StructField('account_state', StringType(), nullable=True),
    StructField('account_zipcode', StringType(), nullable=True),
    StructField('account_country', StringType(), nullable=True),
    StructField('account_region', StringType(), nullable=True),
    StructField('account_website', StringType(), nullable=True),
    StructField('account_tier', StringType(), nullable=True),
    StructField('account_short_code', StringType(), nullable=True),
    StructField('alias_name', StringType(), nullable=True),
    StructField('account_parent_name', StringType(), nullable=True),
    StructField('created_by', StringType(), nullable=True),
    StructField('cusip_id', StringType(), nullable=True),
    StructField('ownership_status', StringType(), nullable=True),
    StructField('is_parent', StringType(), nullable=True),
    StructField('investorType', StringType(), nullable=True),
    StructField('filing_date', StringType(), nullable=True),
    StructField('ownership_struct', StringType(), nullable=True),
    StructField('banking_account_tier', StringType(), nullable=True),
    StructField('sector_name', StringType(), nullable=True),
    StructField('classification_2', StringType(), nullable=True),
    StructField("merged_accounts",
        ArrayType(
            StructType([
                StructField("merged_account_id", StringType(), True),
                StructField("merged_time", StringType(), True),
            ])
        ),
        True
    ),
    StructField('merged_with', StringType(), nullable=True),
])

CLIENT_CODE_MATCHES_FIELDS = [
    'id',
    'ops_user_id',
    'account_id',
    'parent_name',
    'account_name',
    'account_type',
    'account_subtype',
    'account_status',
    'client_display_name',
    'client_code',
    'client_mapping_source',
    'originating_source',
    'client_value_cad',
    'client_value_usd',
    'comment',
    'matching_percentage',
    'matching_category',
    'match_type',
    'trade_date',
    'creation_time',
    'update_time'
]

CLIENT_CODE_MATCHES_SCHEMA = StructType([
    StructField('id', StringType(), nullable=False),
    StructField('ops_user_id', LongType(), nullable=False),
    StructField('account_id', StringType(), nullable=False),
    StructField('parent_name', StringType(), nullable=True),
    StructField('account_name', StringType(), nullable=False),
    StructField('account_type', StringType(), nullable=True),
    StructField('account_subtype', StringType(), nullable=True),
    StructField('account_status', StringType(), nullable=False),
    StructField('client_display_name', StringType(), nullable=True),
    StructField('client_code', StringType(), nullable=False),
    StructField('client_mapping_source', StringType(), nullable=False),
    StructField('originating_source', StringType(), nullable=False),
    StructField('client_value_cad', DecimalType(18, 2), nullable=True),
    StructField('client_value_usd', DecimalType(18, 2), nullable=True),
    StructField('comment', StringType(), nullable=True),
    StructField('matching_percentage', FloatType(), nullable=False),
    StructField('matching_category', StringType(), nullable=False),
    StructField('match_type', StringType(), nullable=False),
    StructField('trade_date', DateType(), nullable=True),
    StructField('creation_time', TimestampType(), nullable=True),
    StructField('update_time', TimestampType(), nullable=True),
])

