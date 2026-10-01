# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Import Necessary Libraries

# COMMAND ----------

import pandas as pd
import numpy as np
from scipy import stats
import requests
import io
import os
import getpass
import datetime
from datetime import date, timedelta
from dateutil.parser import parse
import glob
import time
import logging
import json

# COMMAND ----------

import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

# MAGIC %md
# MAGIC # Helper Functions

# COMMAND ----------

def get_access_token(
    client_id:str
    , client_secret:str
    , target_url
) -> str:
    """
    Retrieves an access token for the provided id and secret.
    :param client_id: The client id to get the access token for
    :param client_secret: The client secret to get the access token for
    :return: str access token
    """

    resp = requests.post(
            url=target_url,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data={
                'grant_type': 'client_credentials',
                'client_id': f'{client_id}',
                'client_secret': f'{client_secret}',
                'scope': 'roles email audience',
            },
            verify=False
        )

    if resp.ok:
        token = resp.json()
        return token['access_token']

    resp.raise_for_status()

# COMMAND ----------

def fetch_tp1_trades(
        date: datetime.date,
        system: str,
        client_facing: bool = True
):
    """
    Fetches data from CV API from a given Trade Date and Source System using an Access Token.
    :param date: The Trade Date for which to pull CV data for
    :param system: The Source System for which to pull CV data for
    :return: df of CV data from Trade Date for a Source System
    """

    # Fetching this info from defaulted values in Widgets
    client_value_url = os.getenv('TDSCI_CLIENT_VALUE_SERVICE_URL', 'https://client-value-asp.dev.client360.td.com/client-value')
    auth_url = os.getenv('TDSCI_RIVENDELL_AUTH_URL', 'https://auth-tor-dev.tds.td.com/auth/realms/Veritas/protocol/openid-connect/token')
    client_id = dbutils.widgets.get("client_id")
    client_secret = dbutils.widgets.get("client_secret")

    # Generate authenticated Access Token
    token = get_access_token(client_id, client_secret, auth_url)

    # Setting header and url for get request
    headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}
    url = (
                client_value_url + '/trades/export?'
                + 'active=True'
                + f'&clientFacing={client_facing}'
                + f'&tradeSystem={system}'
                + f'&tradeDate={date.isoformat()}'
        )

    # Trying fetch from CV API
    print(f"Fetching T+1 trades from {date} for system {system} from url {client_value_url}")
    try:
        response = requests.get(
                        url=url,
                        headers=headers,
                        stream=True,
                        verify=False
                    )

        data = io.BytesIO(response.content)
        data.seek(0)
        df = pd.read_csv(data, low_memory=False)

        print('\nSuccess!\n')

    except requests.exceptions.Timeout:
        raise Exception("Request timed out! Max tries reached!")
    except requests.exceptions.RequestException as e:
        print("Request failed:", e)

    # Explicit deserialization to prevent things like trade id = 435612.0
    df['tradeId'] = df['tradeId'].astype('string').str.split('.').str[0].astype('string')
    df['parentTradeId'] = df['parentTradeId'].astype('string').str.split('.').str[0].astype('string')

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # **Main**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting Global Variables from Widgets: Source System and Trade Date

# COMMAND ----------

# No-op here: this notebook does not touch Unity Catalog resources itself.
# Declared only so it accepts a CATALOG parameter without erroring, should
# whatever eventually orchestrates this notebook pass one down.
dbutils.widgets.text("CATALOG", "")

# Setting Source System
source_system = dbutils.widgets.get("system")
assert source_system is not ''

# Setting Trade Date
input_trade_date = dbutils.widgets.get("trade_date")

# COMMAND ----------

trade_date = None
if (not input_trade_date or input_trade_date.isspace()) and datetime.date.today().weekday() != 0:
    trade_date = datetime.date.today() - datetime.timedelta(days=1)
elif (not input_trade_date or input_trade_date.isspace()) :
    trade_date = datetime.date.today() - datetime.timedelta(days=3)
else:
    trade_date = parse(input_trade_date).date()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetching data from CV API

# COMMAND ----------

# Fetching Trades from CV API using a given Trade Date and Source System
df_tplusone = fetch_tp1_trades(trade_date, source_system)

print(f"Retrieved {df_tplusone.shape[0]} trades from {source_system} source system in Client Value Service.")


# COMMAND ----------

df_tplusone

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output Datamodel

# COMMAND ----------

output_data_model = ['tradeSystem', 'tradeId', 'inventory', 'counterpartyName', 'tradeCalculatorType',
                     'tradeExecutionTypeRule', 'c360SettlementAmount', 'c360CashSpread', 'srcCadFxRate', 'deskAdjustmentWeight',
                     'c360Quantity', 'c360EstimatedSpread', 'tradeProductRepoDayCount', 'c360MaturityTermInDays', 'securityPrincipalFactor',
                     'tradeSecurityKey', 'securityMaturityDate', 'securityKey', 'securityDescription', 'securityKeyType', 'tradeSecurityDetailsId',
                     'tradeSecurityDetailsVersion']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation 1 - Product Classification

# COMMAND ----------

unmapped = df_tplusone[(df_tplusone["c360ProductClassName"].isna()) | (df_tplusone["c360ProductClassTypeName"].isna())]

# COMMAND ----------

print(f"{source_system} - {trade_date.strftime('%m/%d/%Y')}: {unmapped.shape[0]} trade do not have either c360ProductClassName or c360ProductClassTypeName populated")

# COMMAND ----------

print(f"Unmapped trades have following error codes")
print(f"\t\t{unmapped['etlErrorCodes'].value_counts(dropna=True)}")

# COMMAND ----------

df_dd_val1 = unmapped[output_data_model]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation 2 - CV Enrichment

# COMMAND ----------

mapped = df_tplusone[~(df_tplusone["c360ProductClassName"].isna()) & ~(df_tplusone["c360ProductClassTypeName"].isna())]

mapped_no_cv = mapped[mapped['clientValueCad'].isna()]

# COMMAND ----------

print(f"{source_system} - {trade_date.strftime('%m/%d/%Y')}: {mapped_no_cv.shape[0]} trades have product class and product class type name populated but missing cv")

# COMMAND ----------

print(f"Mapped trades with no CV have following error codes")
print(f"\t\t{mapped_no_cv['etlErrorCodes'].value_counts(dropna=True)}")

# COMMAND ----------

df_dd_val2 = mapped_no_cv[output_data_model]

# COMMAND ----------

# MAGIC %md
# MAGIC # Logging

# COMMAND ----------

class Log4jHandler(logging.Handler):
    def __init__(self, log4j):
        logging.Handler.__init__(self)
        self.customLogs = log4j.LogManager.getLogger("TPlusOneValidationLogger")
        self.customLogs.setLevel(log4j.Level.ALL)

    def log_to_log4j(self, message, level):
        if level == logging.INFO:
            self.customLogs.info(message)
        elif level == logging.DEBUG:
            self.customLogs.debug(message)
        elif level == logging.WARNING:
            self.customLogs.warn(message)
        elif level == logging.ERROR:
            self.customLogs.error(message)
        else:
            self.customLogs.fatal(message)

    def emit(self, record):
        msg = self.format(record)
        if msg:
            self.log_to_log4j(msg, record.levelno)

# COMMAND ----------

# log_level = logging.INFO if not debug else logging.DEBUG
log_level = logging.INFO
logging.basicConfig(level=log_level)

log4j = spark.sparkContext._jvm.org.apache.log4j
log4jHandler = Log4jHandler(log4j=log4j)

# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.ERROR)

# add the handler to the root logger
log4jHandler = Log4jHandler(log4j=log4j)
log4jHandler.setLevel(logging.INFO)
logging.getLogger('').addHandler(log4jHandler)
logging.getLogger('').addHandler(console)
logging.getLogger('').setLevel(log_level)

logging.getLogger('root').addHandler(log4jHandler)
# logging.getLogger('root').addHandler(console)
logging.getLogger('root').setLevel(log_level)

logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
logging.getLogger("httpx._client").setLevel(log_level)
logging.getLogger('asyncio').setLevel(log_level)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logging for Validation 1

# COMMAND ----------

logging.info(f"{source_system} - {trade_date.strftime('%m/%d/%Y')}")

if df_dd_val1.shape[0] > 0:
    logging.info('(Failure!) Validation 1 - Product Classification')

    logging.info('Percentage of Trades without Product Classification : {}'.format((df_dd_val1.shape[0] / df_tplusone.shape[0]) * 100))

    for index, trade in df_dd_val1.iterrows():
        logging.info("trade failed product_classification: " + trade.to_json())

    display(df_dd_val1)
else:
    logging.info('(Success!) Validation 1 - Product Classification')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Logging for Validation 2

# COMMAND ----------

logging.info(f"{source_system} - {trade_date.strftime('%m/%d/%Y')}")

if df_dd_val2.shape[0] > 0:
    logging.info('(Failure!) Validation 2 - CV Enrichment')

    logging.info('Percentage of Trades without Client Value Classification : {}'.format((df_dd_val2.shape[0] / df_tplusone.shape[0]) * 100))

    for index, trade in df_dd_val2.iterrows():
        logging.info("trade failed cv_enrichment: " + trade.to_json())

    display(df_dd_val2)
else:
    logging.info('(Success!) Validation 2 - CV Enrichment')