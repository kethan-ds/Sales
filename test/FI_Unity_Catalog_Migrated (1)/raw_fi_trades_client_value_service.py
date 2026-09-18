# Databricks notebook source
import os

# UC catalog must be known before volume_util resolves the Volume path.
dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# MAGIC %run "./access_token_util.py"

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook used to load trades from TDSCI Client Value Service and land in the TDSCI Unity Catalog raw Volume
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook used to load trades from TDSCI Client Value Service and land in the TDSCI Unity Catalog raw Volume

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

from datetime import date
from datetime import datetime
from dateutil.parser import parse

import os
import json
import requests
import datetime

# COMMAND ----------

spark.conf.set('spark.sql.session.timeZone', 'UTC')

# COMMAND ----------

# MAGIC %md
# MAGIC # Get configurations

# COMMAND ----------

# job task parameters widgets
dbutils.widgets.text("TRADE_DATE", "")
dbutils.widgets.text("TRADE_SYSTEM", "")
dbutils.widgets.text("ADB_AUTH_CLIENT_ID", "")
dbutils.widgets.text("ADB_AUTH_CLIENT_SECRET", "")

# COMMAND ----------

ADB_SECRET_CLIENT_ID_NAME = dbutils.widgets.get('ADB_AUTH_CLIENT_ID')
ADB_SECRET_CLIENT_SECRET_NAME = dbutils.widgets.get('ADB_AUTH_CLIENT_SECRET')
input_trade_date = dbutils.widgets.get('TRADE_DATE')
TRADE_SYSTEM = dbutils.widgets.get('TRADE_SYSTEM')
TDSCI_CLIENT_VALUE_SERVICE_URL = os.getenv('TDSCI_CLIENT_VALUE_SERVICE_URL')

# COMMAND ----------

# if no date provided, default to previous weekday
trade_date = None
if (not input_trade_date or input_trade_date.isspace()) and datetime.date.today().weekday() != 0:
    trade_date = datetime.date.today() - datetime.timedelta(days=1)
elif (not input_trade_date or input_trade_date.isspace()):
    trade_date = datetime.date.today() - datetime.timedelta(days=3)
else:
    trade_date = parse(input_trade_date).date()

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Unity Catalog Volume destination

# COMMAND ----------

year = trade_date.year
month = trade_date.month
day = trade_date.day

data_set_type = "trades"
data_set_source = "client_value_service"

# VOLUME_BASE_PATH represents the legacy data/raw root.  Keep the existing
# year/month folder layout so the Bronze notebook can consume the same files.
target_dir = (
    f"{VOLUME_BASE_PATH}/trades/fixed_income/{data_set_source}/"
    f"{TRADE_SYSTEM}/{year}/{month}"
)
dbutils.fs.mkdirs(target_dir)

output_file_name = (
    f"{data_set_type}_{data_set_source}_{TRADE_SYSTEM}_{trade_date}.csv"
)
output_path = f"{target_dir}/{output_file_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC # Function to retrieve the data from the REST API

# COMMAND ----------

def get_raw_data(method: str, url: str, parameters: dict, payload: dict, stream: bool = False, verify_ssl: bool = False, headers: dict = {}):
    access_token = get_oauth2_access_token(ADB_SECRET_CLIENT_ID_NAME, ADB_SECRET_CLIENT_SECRET_NAME)
    headers["Authorization"] = f"Bearer {access_token}"

    response = requests.request(
        method=method,
        url=url,
        params=parameters,
        data=payload,
        headers=headers,
        verify=verify_ssl,
        stream=stream
    )
    response.raise_for_status()
    return response

# COMMAND ----------

# MAGIC %md
# MAGIC # Run data download to Unity Catalog Volume

# COMMAND ----------

endpoint = "/trades/export"
params = f"tradeDate={trade_date}&tradeSystem={TRADE_SYSTEM}"

response = get_raw_data(
    method="GET",
    url=f"{TDSCI_CLIENT_VALUE_SERVICE_URL}{endpoint}",
    parameters=params,
    payload={},
    stream=True,
    verify_ssl=False,
    headers={}
)

# COMMAND ----------

# /Volumes is FUSE-mounted, so binary response content can be written
# directly without an ADLS SDK/service-principal storage client.
with open(output_path, "wb") as f:
    f.write(response.content)

print(f"Successfully written FI raw file: {output_path}")
