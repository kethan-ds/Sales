# Databricks notebook source

# COMMAND ----------

from datetime import date
from datetime import datetime
from dateutil.parser import parse

import os
import json
import requests
import yaml

# COMMAND ----------

spark.conf.set('spark.sql.session.timeZone', 'UTC')

# COMMAND ----------

# MAGIC %md
# MAGIC # Get configurations

# COMMAND ----------

# params
dbutils.widgets.text('SOURCE_REST_SERVICE_API_PATH', '/inventories/export')
dbutils.widgets.text('SOURCE_REST_SERVICE_URL', 'https://businesses-asp.dev.client360.td.com/c360-business-service')
dbutils.widgets.text('VOLUME_RELATIVE_PATH', 'fixed_income/inventories')
dbutils.widgets.text('ADB_AUTH_CLIENT_ID', 'delta_lake_loader_client_id')
dbutils.widgets.text('ADB_AUTH_CLIENT_SECRET', 'delta_lake_loader_client_secret')
dbutils.widgets.text('CATALOG', '')

# COMMAND ----------

SOURCE_REST_SERVICE_API_PATH = dbutils.widgets.get('SOURCE_REST_SERVICE_API_PATH')
SOURCE_REST_SERVICE_URL = dbutils.widgets.get('SOURCE_REST_SERVICE_URL')
VOLUME_RELATIVE_PATH = dbutils.widgets.get('VOLUME_RELATIVE_PATH').strip('/')

ADB_SECRET_CLIENT_ID_NAME = dbutils.widgets.get('ADB_AUTH_CLIENT_ID')
ADB_SECRET_CLIENT_SECRET_NAME = dbutils.widgets.get('ADB_AUTH_CLIENT_SECRET')

# COMMAND ----------

AUTH_URL = os.getenv('TDSCI_RIVENDELL_AUTH_URL', 'https://auth-tor-dev.tds.td.com/auth/realms/Veritas/protocol/openid-connect/token')
AUTH_SCOPES = os.getenv('AUTH_SCOPES', 'roles email audience')

SOURCE_REST_SERVICE_SSL_VERIFY = os.getenv('SOURCE_REST_SERVICE_SSL_VERIFY', 'false').lower() == "true"
SOURCE_REST_SERVICE_IS_STREAM = os.getenv('SOURCE_REST_SERVICE_IS_STREAM', 'false').lower() == "true"

SOURCE_REST_API_HEADERS = json.loads(os.getenv('SOURCE_REST_API_HEADERS', '{}'))
SOURCE_REST_API_QUERY_PARAMETERS = {}

# COMMAND ----------

CATALOG_NAME = dbutils.widgets.get('CATALOG')
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup destination Volume path

# COMMAND ----------

target_dir = f"{VOLUME_BASE_PATH}/{VOLUME_RELATIVE_PATH}"
dbutils.fs.mkdirs(target_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC # Function to retrieve access token from auth url using ADB Scope and secrets

# COMMAND ----------

# get auth token
def get_oauth2_access_token():
    auth_response = requests.post(
        url=AUTH_URL,
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        data={
            'grant_type': 'client_credentials',
            'client_id': ADB_SECRET_CLIENT_ID_NAME,
            'client_secret': ADB_SECRET_CLIENT_SECRET_NAME,
            'scope': AUTH_SCOPES,
        },
        verify=False
    )
    auth_response.raise_for_status()

    return auth_response.json()['access_token']

# COMMAND ----------

# MAGIC %md
# MAGIC # Function to retrieve the data from the REST API

# COMMAND ----------

def get_raw_data(url: str, parameters: dict, stream: bool = False, verify_ssl: bool = False, headers: dict = {}):
    access_token = get_oauth2_access_token()
    headers["Authorization"] = f"Bearer {access_token}"

    response = requests.get(
        url=url,
        params=parameters,
        headers=headers,
        verify=verify_ssl,
        stream=stream
    )

    response.raise_for_status()
    return response

# COMMAND ----------

# MAGIC %md
# MAGIC # Run data download

# COMMAND ----------

response = get_raw_data(
    url=f"{SOURCE_REST_SERVICE_URL}{SOURCE_REST_SERVICE_API_PATH}",
    parameters=SOURCE_REST_API_QUERY_PARAMETERS,
    stream=SOURCE_REST_SERVICE_IS_STREAM,
    verify_ssl=SOURCE_REST_SERVICE_SSL_VERIFY,
    headers=SOURCE_REST_API_HEADERS
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Upload results to the UC Volume path

# COMMAND ----------

file_name = response.headers['Content-Disposition'].split('filename=')[1].split(';')[0]
output_path = f"{target_dir}/{file_name}"

with open(output_path, "wb") as f:
    f.write(response.content)
