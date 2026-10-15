# Databricks notebook source
import os
import json
from typing import (
    Dict,
    Any,
    Callable,
    List,
    Tuple,
    Optional
)
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
from pyspark.sql import Row, DataFrame
from httpx import Client, Request
from retry import retry

# COMMAND ----------

def token_retriever(cache_key: str) -> Callable[[Client, str, str, str, str], str]:
    return (
        retry(exceptions=RETRYABLE_EXCEPTIONS, delay=int(os.getenv(HTTP_ERROR_DELAY, "5")),
              tries=int(os.getenv(HTTP_ERROR_RETRIES, "10")), max_delay=int(os.getenv(HTTP_ERROR_MAX_DELAY, "60")),
              backoff=int(os.getenv(HTTP_ERROR_BACKOFF, "2")), jitter=(1, 10))
        (timed_token_cache(cache_key)(get_token))
    )

def get_token(httpx_client: Client, client_id: str, client_secret: str, auth_url: str, scope: str) -> str:
    response = (
        httpx_client.post(auth_url,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data={
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret,
                'scope': scope
            }
        )
    )

    if is_retryable_http_error(response):
        raise RetryableHTTPError(f'Status code for retryable error: {response.status_code}, error: {response.reason_phrase}')

    response.raise_for_status()

    return response.json()['access_token']

# COMMAND ----------

RETRYABLE_EXCEPTIONS = (
    httpx.TimeoutException,      # All timeout errors (base class)
    httpx.ConnectTimeout,        # Connection timeout
    httpx.ReadTimeout,           # Read timeout
    httpx.WriteTimeout,          # Write timeout
    httpx.PoolTimeout,           # Connection pool timeout
    httpx.ConnectError,          # Failed to establish connection
    httpx.RemoteProtocolError,   # Server protocol error
    httpx.NetworkError,          # Base class for network errors
    RetryableHTTPError
)

RIVENDELL_TOKEN_RETRIEVER = token_retriever('rivendell_token_cache')
PRISM_TOKEN_RETRIEVER = token_retriever('prism_token_cache')

# COMMAND ----------

@retry(exceptions=RETRYABLE_EXCEPTIONS,
       delay=int(os.getenv(HTTP_ERROR_DELAY, "5")),
       tries=int(os.getenv(HTTP_ERROR_RETRIES, "10")),
       max_delay=int(os.getenv(HTTP_ERROR_MAX_DELAY, "60")),
       backoff=int(os.getenv(HTTP_ERROR_BACKOFF, "2")),
       jitter=(1, 10))
def get_records(httpx_client: Client, url: str, headers: Dict[str, str]):
    next_link = url

    while next_link:
        response = httpx_client.get(next_link, headers=headers)

        response.raise_for_status()

        payload = response.json()

        yield payload

        next_link = None

        if payload['_links'].get('next'):
            next_link = payload['_links']['next']['href']

# COMMAND ----------

@retry(exceptions=RETRYABLE_EXCEPTIONS,
       delay=int(os.getenv(HTTP_ERROR_DELAY, "5")),
       tries=int(os.getenv(HTTP_ERROR_RETRIES, "10")),
       max_delay=int(os.getenv(HTTP_ERROR_MAX_DELAY, "60")),
       backoff=int(os.getenv(HTTP_ERROR_BACKOFF, "2")),
       jitter=(1, 10))
def _perform_operation(httpx_client: Client, url: str, records: Any, http_method: str):
    headers = {
        'Authorization': f'Bearer {RIVENDELL_TOKEN_RETRIEVER(httpx_client, CIP_CLIENT_ID, CIP_CLIENT_SECRET, AUTH_SERVER, RIVENDELL_AUTH_SCOPES)}',
        'Content-Type': 'application/json'
    }

    response = httpx_client.request(
        http_method.upper(),
        url,
        headers=headers,
        json=records
    )

    if is_retryable_http_error(response):
        raise RetryableHTTPError(f'Status code for retryable error: {response.status_code}, error: {response.reason_phrase}')

    return response

# COMMAND ----------

@retry(exceptions=RETRYABLE_EXCEPTIONS,
       delay=int(os.getenv(HTTP_ERROR_DELAY, "5")),
       tries=int(os.getenv(HTTP_ERROR_RETRIES, "10")),
       max_delay=int(os.getenv(HTTP_ERROR_MAX_DELAY, "60")),
       backoff=int(os.getenv(HTTP_ERROR_BACKOFF, "2")),
       jitter=(1, 10))
def get_server_record(httpx_client: Client,url: str, embedded_type: str, extractor: Callable[[Row], Any]):
    headers = {
        'Authorization': f'Bearer {RIVENDELL_TOKEN_RETRIEVER(httpx_client, CIP_CLIENT_ID, CIP_CLIENT_SECRET, AUTH_SERVER, RIVENDELL_AUTH_SCOPES)}'
    }
    results = []

    for record in get_records(httpx_client, url, headers):
        processed_rec = [extractor(rec) for rec in record['_embedded'][embedded_type]]

        results.extend(processed_rec)

    return results

def bulk_retrieve(url: str, records_to_pull: List[str], httpx_client: Client,
                  extractor: Callable[[Row], Any]):
    with ThreadPoolExecutor() as executor:
        futures = []

        for id_batch in partition(records_to_pull, size=2_000):
            futures.append(executor.submit(_perform_operation, httpx_client, url, id_batch, "POST"))

        records = []

        for fut in as_completed(futures):
            fut.result().raise_for_status()

            records.extend([extractor(r) for r in fut.result().json()])

        return records

# COMMAND ----------

def send_to_rest_service_if_applicable(httpx_client: Client, df: DataFrame) -> None:
    if df.count() == 0:
        return

    client_url = f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-matches-bulk/bulk-upload'

    with ThreadPoolExecutor(max_workers=5) as executor:
        jobs = []

        for records in partition(df.collect(), size=2_000):
            server_records = [to_server_record(rec) for rec in records]

            jobs.append(executor.submit(_perform_operation, httpx_client, client_url, server_records, "POST"))

        for fut in as_completed(jobs):
            fut.result().raise_for_status()

def delete_server_by_id(httpx_client: Client, id_list: List[str], url: str) -> None:
    if not id_list:
        return

    with ThreadPoolExecutor(max_workers=5) as executor:
        jobs = []

        for records in partition(id_list, size=2_000):
            jobs.append(executor.submit(_perform_operation, httpx_client, url, records, 'DELETE'))

        for fut in as_completed(jobs):
            response = fut.result()

            if is_retryable_http_error(response):
                response.raise_for_status()

# COMMAND ----------

def save_diffs(httpx_client: Client, df: DataFrame, converter: Callable[[dict], str]) -> None:
    if df.count() == 0:
        return

    client_url = f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-match-diff'

    with ThreadPoolExecutor(max_workers=5) as executor:
        jobs = []

        for diff in df.collect():
            jobs.append(executor.submit(_perform_operation,
                                        httpx_client,
                                        client_url,
                                        converter(diff.asDict(recursive=True)),
                                        "POST"))

        for fut in as_completed(jobs):
            fut.result().raise_for_status()

# COMMAND ----------

def patch_server_by_id(httpx_client: Client, records: List[Dict[str, Any]]) -> None:
    if not records:
        return

    with ThreadPoolExecutor(max_workers=5) as executor:
        jobs = []

        client_url = f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-matches-bulk/bulk-patch'

        for payload_list in partition(records, size=2_000):
            jobs.append(executor.submit(_perform_operation, httpx_client, client_url, payload_list, 'PATCH'))

        for fut in as_completed(jobs):
            response = fut.result()

            if is_retryable_http_error(response):
                response.raise_for_status()

# COMMAND ----------

def get_server_ids(query_params: Optional[str] = None) -> DataFrame:
    # get all ids
    bulk_get_url = (
        f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-matches?projection=clientMatchIdProjection&size=5000'
    )

    if query_params:
        bulk_get_url = bulk_get_url + "&" + query_params

    extractor = lambda rec: (rec['id'].upper(),)
    server_ids = get_server_record(HTTPX_CLIENT, bulk_get_url, 'clientCodeMatches', extractor)

    if not server_ids:
        return spark.createDataFrame([], 'id: string')

    df = spark.createDataFrame(server_ids, ['id'])

    return df.withColumn('id', F.ucase('id'))

