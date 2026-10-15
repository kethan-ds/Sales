# Databricks notebook source
from typing import Any, Dict, List, Tuple, Optional
from typing import Union
from datetime import datetime, timezone
from decimal import Decimal
import json
import os

import httpx
from httpx import Client
from retry import retry
import pyspark.sql.functions as F
from pyspark.sql import (
    DataFrame,
    Row,
    SparkSession,
    Column
)

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
RETRYABLE_EXCEPTIONS = (
    httpx.TimeoutException,     # All timeout errors (base class)
    httpx.ConnectTimeout,       # Connection timeout
    httpx.ReadTimeout,          # Read timeout
    httpx.WriteTimeout,         # Write timeout
    httpx.PoolTimeout,          # Connection pool timeout
    httpx.ConnectError,         # Failed to establish connection
    httpx.RemoteProtocolError,  # Server protocol error
    httpx.NetworkError,         # Base class for network errors
    RetryableHTTPError
)

# COMMAND ----------

def to_decimal(value):
    if value is None:
        return None

    return Decimal(str(value))  # important: use str() to avoid float precision issues


def to_timestamp(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    return datetime.strptime(value, DATE_FORMAT)


def to_date(value):
    if value is None:
        return None
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        return value

    return datetime.strptime(value, "%Y-%m-%d").date()

# COMMAND ----------

ExpressionOrColumn = Union[Column, str]

def to_client_match(record: Row) -> Tuple[Optional[Any], ...]:
    return (
        record['id'].upper(),
        record['opsUserId'],
        record['embeddedPrismMapping']['accountId'],
        record['embeddedPrismMapping']['parentName'],
        record['embeddedPrismMapping']['accountName'],
        record['embeddedPrismMapping']['accountType'],
        record['embeddedPrismMapping']['accountSubType'],
        record['embeddedPrismMapping']['accountStatus'],
        record['embeddedSubordinateMapping']['clientDisplayName'],
        record['embeddedSubordinateMapping']['clientCode'],
        record['embeddedSubordinateMapping']['clientMappingSource'],
        record['embeddedSubordinateMapping']['originatingSource'],
        to_decimal(record['embeddedSubordinateMapping']['clientValueCad']),
        to_decimal(record['embeddedSubordinateMapping']['clientValueUsd']),
        record['comment'],
        record['matchPercentage'],
        record['matchCategory'],
        record['matchType'],
        to_date(record['tradeDate']),
        to_timestamp(record['creationTime']),
        to_timestamp(record['updateTime'])
    )

def to_notification(record) -> Dict[str, Any]:
    return {
        'id': record['id'],
        'notificationType': record['notificationType'],
        'creationTime': datetime.strptime(record['creationTime'].split(" ")[0], '%Y-%m-%d').date,
        'publisher': record['publisher'],
        'checksum': record['checksum'],
        'target': record['target'],
        'priority': record['priority'],
        'message': json.loads(record['message'])
    }

# COMMAND ----------

def to_trade_date_update(record: Row) -> Dict[str, Any]:
    return {
        'id': record['id'].upper(),
        'opsUserId': SYSTEM_ID,
        'comment': 'Changing trade_dates',
        'tradeDate': record['trade_date'].isoformat()
    }

def to_parent_name_update(record: Row) -> Dict[str, Any]:
    return {
        'id': record['id'].upper(),
        'opsUserId': SYSTEM_ID,
        'comment': 'Updating parent name',
        "embeddedPrismMapping": {
            'parentName': record['parent_name']
        }
    }

# COMMAND ----------

def to_prism_rec(r) -> Tuple[str, ...]:
    return (
        r['account_id'],
        r['account_name'],
        r.get('account_type', '').lower(),
        r['account_status'].lower(),
        r.get('account_subtype', '').lower(),
        r.get('account_parent_id', ''),
        r.get('account_address_1', ''),
        r.get('account_address_2', ''),
        r.get('account_address_3', ''),
        r.get('account_city', ''),
        r.get('account_state', ''),
        r.get('account_zipcode', ''),
        r.get('account_country', ''),
        r.get('account_region', ''),
        r.get('account_website', ''),
        r.get('account_tier', ''),
        r.get('account_short_code', ''),
        r.get('alias_name', ''),
        r.get('account_parent_name', ''),
        r.get('created_by', ''),
        r.get('cusip_id', ''),
        r.get('ownership_status', ''),
        r.get('is_parent', ''),
        r.get('investorType', ''),
        r.get('filing_date', ''),
        r.get('ownership_struct', ''),
        r.get('banking_account_tier', ''),
        r.get('sector_name', ''),
        r.get('classification_2', ''),
        r.get('merged_accounts', ''),
        r.get('merged_with', '')
    )

def to_prism_sql(fields: List[str], table_alias: str, ignore_fields:
                 List[str]) -> Dict[str, ExpressionOrColumn]:
    maps: List[Dict[str, str]] = [{field: table_alias + '.' + field} for field in fields if field not
                                 in ignore_fields]

    record = {}

    for m in maps:
        record.update(m)

    return record

# COMMAND ----------

def to_prism_sql(fields: List[str], table_alias: str, ignore_fields:
                 List[str]) -> Dict[str, ExpressionOrColumn]:
    maps: List[Dict[str, str]] = [{field: table_alias + '.' + field} for field in fields if field not
                                 in ignore_fields]

    record = {}

    for m in maps:
        record.update(m)

    return record

def to_server_record(record: Row) -> Dict[str, Any]:
    cv_cad = (
        float(record['client_value_cad'])
        if record['client_value_cad'] is not None else 0.0
    )
    cv_usd = (
        float(record['client_value_usd'])
        if record['client_value_usd'] is not None else 0.0
    )
    matching_percentage = (
        float(record['matching_percentage'])
        if record['matching_percentage'] is not None else 0.0
    )
    trade_date = record['trade_date'].isoformat() if record['trade_date'] else None

    return {
        'id': record['id'].upper(),
        'opsUserId': SYSTEM_ID,
        'comment': 'potential match',
        'matchPercentage': matching_percentage,
        'matchCategory': record['matching_category'],
        'matchType': record['match_type'],
        'tradeDate': trade_date,
        'embeddedSubordinateMapping': {
            'clientMappingSource': record['client_mapping_source'],
            'clientDisplayName': record['client_display_name'],
            'clientCode': record['client_code'],
            'originatingSource': record['originating_source'],
            'clientValueCad': cv_cad,
            'clientValueUsd': cv_usd
        },
        "embeddedPrismMapping": {
            'accountId': record['account_id'],
            'parentName': record['parent_name'],
            'accountName': record['account_name'],
            'accountType': record['account_type'],
            'accountSubType': record['account_subtype'],
            'accountStatus': record['account_status']
        }
    }

# COMMAND ----------

def to_cv_patch_record(record: Row) -> Dict[str, Any]:
    return {
        'id': record['id'].upper(),
        'comment': 'Updated CV USD value',
        "embeddedSubordinateMapping": {
            'clientValueUsd': float(record['updated_cv_usd'])
        }
    }

# COMMAND ----------

def to_account_merge_record(record: Row) -> Dict[str, Any]:
    comment = f'Automated merge: Winner={record["winner_account_id"]}, Loser={record["loser_account_id"]}'

    return {
        'id': record['id'].upper(),
        'opsUserId': SYSTEM_ID,
        'comment': comment,
        "matchPercentage": record['matching_percentage'],
        "matchCategory": record['matching_category'],
        "embeddedPrismMapping": {
            'accountId': record['winner_account_id'],
            'accountName': record['winner_account_name'],
            'accountType': record['winner_account_type'],
            'accountSubType': record['winner_account_subtype'],
            'accountStatus': record['winner_account_status'],
            'parentName': record['winner_parent_name'],
        },
        'lastModifiedAudit': {
            'lastUpdatedBy': SYSTEM_ID,
            'isLastActionAuditVisible': True,
            'lastComment': comment,
            'lastModifiedAt': datetime.now(timezone.utc).strftime(DATE_FORMAT)
        }
    }

def to_match_patch_record(record: Row) -> Dict[str, Any]:
    if record['matching_percentage']:
        return {
            'id': record['id'].upper(),
            'opsUserId': SYSTEM_ID,
            'comment': 'Updating match percentage and other changes',
            "matchPercentage": record['matching_percentage'],
            "matchCategory": record['matching_category'],
            "embeddedPrismMapping": {
                'accountId': record['prism_account_id'],
                'accountName': record['prism_account_name'],
                'accountType': record['prism_account_type'],
                'accountSubType': record['prism_account_subtype'],
                'accountStatus': record['prism_account_status']
            }
        }

    return {
        'id': record['id'].upper(),
        'opsUserId': SYSTEM_ID,
        'comment': 'Updating match for detected changes...',
        "embeddedPrismMapping": {
            'accountId': record['prism_account_id'],
            'accountName': record['prism_account_name'],
            'accountType': record['prism_account_type'],
            'accountSubType': record['prism_account_subtype'],
            'accountStatus': record['prism_account_status']
        }
    }

# COMMAND ----------

@retry(exceptions=RETRYABLE_EXCEPTIONS,
       delay=int(os.getenv(HTTP_ERROR_DELAY, "5")),
       tries=int(os.getenv(HTTP_ERROR_RETRIES, "10")),
       max_delay=int(os.getenv(HTTP_ERROR_MAX_DELAY, "60")),
       backoff=int(os.getenv(HTTP_ERROR_BACKOFF, "2")),
       jitter=(1, 10))
def _get_prism_batch(httpx_client: Client, url: str, query_payload: Dict[str, Any]) -> Dict[str, Any]:
    token = PRISM_TOKEN_RETRIEVER(httpx_client, PRISM_CLIENT_ID, PRISM_CLIENT_SECRET, PRISM_AUTH_SERVER, "")

    response = httpx_client.post(url=url, json=query_payload, headers={'Authorization': f'Bearer {token}'})

    if is_retryable_http_error(response):
        raise RetryableHTTPError(f'Status code for retryable error: {response.status_code}, error: {response.reason_phrase}')

    response.raise_for_status()

    return response.json()

# COMMAND ----------

def _get_initial_query_payload(df: DataFrame) -> Dict[str, Any]:
    modified_from_as_date = df.first().last_modified # type: ignore
    modified_from = modified_from_as_date.strftime(PYTHON_DATETIME_FORMAT)

    logger.info(f'Modified from: {modified_from}')

    query_payload = {
        'modified_from': modified_from,
        'modified_to': datetime.now().strftime(PYTHON_DATETIME_FORMAT),
        'int_start': 0
    }

    if modified_from_as_date.year == 1970:
        query_payload = {
            'int_start': 0
        }

    return query_payload

# COMMAND ----------

def _get_prism_records(httpx_client: Client, url: str, df: DataFrame) -> List[Row]:
    prism_data: List[Row] = []

    query_payload = _get_initial_query_payload(df)

    logger.info(f'Fetching PRISM data (first batch)...')

    payload_with_metadata = _get_prism_batch(httpx_client=httpx_client, url=url,
                                            query_payload=query_payload)

    prism_data.extend(payload_with_metadata['accounts'])

    total_accounts = payload_with_metadata['total_count']

    increment = 1_000
    query_payload['int_start'] = increment

    logger.info(f'Total PRISM records: {total_accounts}, next increment: {increment}')

    while total_accounts > increment:
        logger.info(f'Fetching increment {increment} out of {total_accounts}')

        prism_response = _get_prism_batch(httpx_client=httpx_client, url=url,
                                         query_payload=query_payload)

        if 200 != prism_response.get('status'):
            logger.info(f'Refreshing token because of status {prism_response.get("status")} we received...')

            logger.info(f'Token refreshed')

            prism_response = _get_prism_batch(httpx_client=httpx_client, url=url,
                                             query_payload=query_payload)

        prism_data.extend(prism_response['accounts'])

        increment += 1_000
        query_payload['int_start'] = increment

    prism_data = list({item['account_id']: item for item in prism_data}.values())

    return prism_data

# COMMAND ----------

def call_prism_actual(httpx_client: Client, url: str,
                      spark_session: SparkSession,
                      location: str) -> List[Tuple[str, ...]]:
    try:
        df = spark_session.read.table(location)
        if df.isEmpty():
            raise ValueError('PRISM watermark table is empty')
    except Exception:
        first_modified_df = spark_session.createDataFrame([['1970-01-01 00:00:00']], ['last_modified'])
        first_modified_df = first_modified_df.withColumn('last_modified',
                                                        F.to_timestamp('last_modified', SPARK_DATETIME_FORMAT))
        first_modified_df.write.format('delta').mode('overwrite').saveAsTable(location)

        df = first_modified_df

    logger.info(f'Total records found in {location}: {df.count()}')
    logger.info(f'Fetching PRISM token...')

    prism_data = _get_prism_records(httpx_client, url, df)

    logger.info(f'Fetching complete...')

    logger.info(f'Total records retrieved: {len(prism_data)}')

    return [to_prism_rec(r) for r in prism_data]

