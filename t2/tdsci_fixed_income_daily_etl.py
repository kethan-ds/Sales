# Databricks notebook source
# DBTITLE 1,Imports
from client360_t_plus_one.client360_t_plus_one import Client360TPlusOne
from client360_t_plus_one.clients.auth.security_config import Client360SecurityConfig, SecurityConfig, DremioSecurityConfig
from client360_t_plus_one.preprocessing.core.client360_loader import TradeSystem
from client360_t_plus_one.preprocessing.t_plus_one_preprocessing import Client360TradeSystemProcessor
from client360_t_plus_one.preprocessing.clients.auth.access_token_util import AccessTokenUtil
from client360_t_plus_one.clients import client360_clients_helper
from client360_t_plus_one.util.upload_util import UploadUtil
from urllib3.exceptions import InsecureRequestWarning
from anyio import ClosedResourceError
from pandas import DataFrame
from httpx import AsyncClient
from httpx import HTTPStatusError
from uuid import uuid4
from dateutil.parser import parse

import os
import io
import datetime
import requests
import warnings
import contextlib
import httpx
import time
import asyncio
import nest_asyncio
import threading
import logging
import numpy as np
import pandas as pd

# COMMAND ----------


# set the below environment variable and update properties in next cell when running this notebook locally
LocalExecution = os.getenv('C360_T_PLUS_ONE_LOCAL_EXECUTION', 0)
client_value_url = os.getenv('TDSCI_CLIENT_VALUE_SERVICE_URL', 'https://client-value-asp.dev.client360.td.com/client-value')

if LocalExecution:
    env="prod"
    system="TOMS"
    statuses=["VERIFIED"]
    client_id="tdsci-fixed-income-etl"
    client_secret=""
    dremio_username="PTDSCI241C360TC"
    dremio_personal_access_token=""
    input_trade_date = '2023-05-30'
    preproc_partition_count = 100
    postproc_partition_count = 100
    VERIFY_SSL = False
    CLIENT_VALUE_MAX_CONNECTIONS = 10
    CLIENT_VALUE_MAX_KEEPALIVE_CONNECTIONS = 5
    rerun = True
    debug = False
    chunk_size = 200
else:
    dbutils.widgets.text("client_value_max_connections", "5")
    dbutils.widgets.text("client_value_max_keepalive_connections", "2")
    dbutils.widgets.text("verify_ssl", 'False')
    dbutils.widgets.text("environment", "dev")
    dbutils.widgets.text("rerun", "False")
    dbutils.widgets.text("debug", "False")
    dbutils.widgets.text("system", "")
    dbutils.widgets.text("trade_statuses", "")
    dbutils.widgets.text("client_id", "")
    dbutils.widgets.text("client_secret", "")
    dbutils.widgets.text("dremio_username", "")
    dbutils.widgets.text("dremio_personal_access_token", "")
    dbutils.widgets.text("trade_date", "")
    dbutils.widgets.text("preproc_partition_count", "100")
    dbutils.widgets.text("postproc_partition_count", "100")
    dbutils.widgets.text("chunk_size", "200")

    VERIFY_SSL = dbutils.widgets.get('verify_ssl') == "True"
    CLIENT_VALUE_MAX_CONNECTIONS = int(dbutils.widgets.get('client_value_max_connections'))
    CLIENT_VALUE_MAX_KEEPALIVE_CONNECTIONS = int(dbutils.widgets.get('client_value_max_keepalive_connections'))
    rerun = dbutils.widgets.get('rerun') == "True"
    debug = dbutils.widgets.get('debug') == "True"

    env = dbutils.widgets.get('environment')
    system = dbutils.widgets.get('system')
    statuses = dbutils.widgets.get('trade_statuses').split(",")
    client_id = dbutils.widgets.get('client_id')
    client_secret = dbutils.widgets.get('client_secret')
    dremio_username = dbutils.widgets.get('dremio_username')
    dremio_personal_access_token = dbutils.widgets.get('dremio_personal_access_token')
    input_trade_date = dbutils.widgets.get('trade_date')
    preproc_partition_count = int(dbutils.widgets.get('preproc_partition_count'))
    postproc_partition_count = int(dbutils.widgets.get('postproc_partition_count'))
    chunk_size = int(dbutils.widgets.get('chunk_size'))

# COMMAND ----------

# DBTITLE 1,ETL Setup
trade_date = None
if (not input_trade_date or input_trade_date.isspace()) and datetime.date.today().weekday() != 0:
    trade_date = datetime.date.today() - datetime.timedelta(days=1)
elif (not input_trade_date or input_trade_date.isspace()):
    trade_date = datetime.date.today() - datetime.timedelta(days=3)
else:
    trade_date = parse(input_trade_date).date()

at_date = datetime.datetime.now()

# COMMAND ----------

# DBTITLE 1,Log4J Handler
class Log4jHandler(logging.Handler):
    def __init__(self, log4j):
        logging.Handler.__init__(self)
        self.customLogs = log4j.LogManager.getLogger("FixedIncomeETl")
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

# DBTITLE 1,Logging Config
log_level = logging.INFO if not debug else logging.DEBUG
logging.basicConfig(level=log_level)

if not LocalExecution:
    # log4j handler
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

    logging.getLogger('Client360TradeSystemProcessor').addHandler(log4jHandler)
    # logging.getLogger('Client360TradeSystemProcessor').addHandler(console)
    logging.getLogger('Client360TradeSystemProcessor').setLevel(log_level)

    logging.getLogger('Client360VeritasTradeLoader').addHandler(log4jHandler)
    # logging.getLogger('Client360VeritasTradeLoader').addHandler(console)
    logging.getLogger('Client360VeritasTradeLoader').setLevel(log_level)


    logging.getLogger('Client360TradeSystemProcessor').addHandler(log4jHandler)
    # logging.getLogger('Client360TradeSystemProcessor').addHandler(console)
    logging.getLogger('Client360TradeSystemProcessor').setLevel(log_level)

    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
    logging.getLogger("httpx._client").setLevel(log_level)
    logging.getLogger('asyncio').setLevel(log_level)

# COMMAND ----------

old_merge_environment_settings = requests.Session.merge_environment_settings

@contextlib.contextmanager
def no_ssl_verification():
    opened_adapters = set()
    def merge_environment_settings(self, url, proxies, stream, verify, cert):
        # Verification happens only once per connection so we need to close
        # all the opened adapters once we're done. Otherwise, the effects of
        # verify=False persist beyond the end of this context manager.
        opened_adapters.add(self.get_adapter(url))
        settings = old_merge_environment_settings(self, url, proxies, stream, verify, cert)
        settings['verify'] = False
        settings['timeout'] = 1000000000 if not LocalExecution else 100000

        return settings
    requests.Session.merge_environment_settings = merge_environment_settings
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', InsecureRequestWarning)
            yield
    finally:
        requests.Session.merge_environment_settings = old_merge_environment_settings
        for adapter in opened_adapters:
            try:
                adapter.close()
            except:
                pass

# COMMAND ----------

AccessTokenUtil.verify_ssl = VERIFY_SSL

# COMMAND ----------

# DBTITLE 1,Load and Standardize Trades
trades = None
securities = None
trade_loader = Client360TradeSystemProcessor(
    system=TradeSystem[system],
    client360_env=env,
    client360_client_id=client_id,
    client360_client_secret=client_secret,
    veritas_env=env,
    veritas_client_id=client_id,
    veritas_client_secret=client_secret,
    num_partitions=preproc_partition_count,
    verify_ssl=VERIFY_SSL
)

with no_ssl_verification():
    preprocessed_result = trade_loader.run(
        trade_date=trade_date,
        at_date=at_date,
        status=statuses
    )
    trades = preprocessed_result.trades
    securities = preprocessed_result.securities

# COMMAND ----------

client360_clients_helper.VERIFY_SSL = VERIFY_SSL

# COMMAND ----------

# DBTITLE 1,Enrich with T+1 Rules
t_plus_one_trades = None

tplusone = Client360TPlusOne(
    trade_date=trade_date,
    trades=trades,
    securities=securities,
    client360_security_config=Client360SecurityConfig(
        env=env,
        client_id=client_id,
        client_secret=client_secret
    ),
    veritas_security_config=SecurityConfig(
        env=env,
        client_id=client_id,
        client_secret=client_secret
    ),
    dremio_security_config=DremioSecurityConfig(
        env=env,
        dremio_username=dremio_username,
        dremio_personal_access_token=dremio_personal_access_token
    )
)

with no_ssl_verification():
    t_plus_one_trades = tplusone.run_pipeline(
        num_partitions=postproc_partition_count
    )

# COMMAND ----------

# DBTITLE 1,Get trades based on URL and convert csv response to dataframe
def get_existing_trades(url: str, headers: dict) -> DataFrame:
    existing_trades_response = requests.get(
        url=url,
        headers=headers,
        stream=True,
        verify=VERIFY_SSL
    )

    data = io.BytesIO(existing_trades_response.content)
    data.seek(0)
    return pd.read_csv(data)

# COMMAND ----------

# DBTITLE 1,Get Trades based on IDs and System
def get_existing_trades_for(current_trades: DataFrame) -> DataFrame:
    logging.info("Checking for existing trades that match current trades")
    token = AccessTokenUtil.get_access_token(
        env=env,
        client_id=client_id,
        client_secret=client_secret
    )
    headers = {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
    }

    payload = {
        "tradeSystem": system,
        "tradeIds": current_trades['tradeId'].unique().tolist()
    }

    url = f"{client_value_url}/trades/export"

    existing_trades_response = requests.post(
        url=url,
        headers=headers,
        json=payload,
        stream=True,
        verify=VERIFY_SSL
    )

    existing_trades_response.raise_for_status()

    data = io.BytesIO(existing_trades_response.content)
    data.seek(0)
    df = pd.read_csv(data)

    return df

# COMMAND ----------

COLS_TO_CLEAN = {
    'id': 'Int64',
    'c360Version': 'Int32',
    'quantity': 'Int64',
    'c360CvProductId': 'Int64',
    'c360CvProductVersion': 'Int32',
    'c360ProductClassId': 'Int64',
    'c360ProductClassVersion': 'Int32',
    'c360ProductClassTypeId': 'Int64',
    'c360ProductClassTypeVersion': 'Int32',
    'auditDetailsId': 'Int64',
    'auditDetailsVersion': 'Int32',
    'etlStatusId': 'Int64',
    'etlStatusVersion': 'Int32',
    'tradeProductDetailsId': 'Int64',
    'tradeProductDetailsVersion': 'Int32',
    'tradeClientValueId': 'Int64',
    'tradeClientValueVersion': 'Int32',
    'tradeClientValueMarkupId': 'Int64',
    'tradeClientValueMarkupVersion': 'Int32',
    'tradeDetailsId': 'Int64',
    'tradeDetailsVersion': 'Int32',
    'tradeSecurityDetailsId': 'Int64',
    'tradeSecurityDetailsVersion': 'Int32',
    'tradeProductRepoDayCount': 'Int32',
    'tradeProductRepoDayCountVersion': 'Int32'
}


def convert_column(trades: DataFrame, col: str, data_type) -> DataFrame:
    if col in trades:
        if col.endswith('Version'):
            trades[col] = np.floor(pd.to_numeric(trades[col], errors='coerce')).fillna(0).astype(data_type)
        else:
            trades[col] = np.floor(pd.to_numeric(trades[col], errors='coerce')).astype(data_type)
    return trades


def cleanup_trades(trades: DataFrame) -> DataFrame:
    for col, data_type in COLS_TO_CLEAN.items():
        trades = convert_column(trades, col, data_type)

    if 'quantity' in trades:
        trades['tradeQuantity'] = trades['quantity']
        trades['c360Quantity'] = trades['quantity']

    trades['c360MaturityTermInDays'] = np.floor(pd.to_numeric(trades['c360MaturityTermInDays'], errors='coerce')).fillna(0).astype(int)
    return trades

# COMMAND ----------

# DBTITLE 1,Merge new trades with existing trades
def set_existing_id_and_version(trades_to_upload: DataFrame, existing_trades: DataFrame) -> DataFrame:
    trades_to_upload = trades_to_upload.merge(
        existing_trades[[
            'id',
            'tradeId',
            'c360Version',
            'tradeCreatedBy',
            'tradeCreatedDateTime',
            'tradeUpdatedBy',
            'tradeUpdatedDateTime',
            'tradeSecurityDetailsId',
            'tradeSecurityDetailsVersion',
            'tradeSecurityDetailsCreatedBy',
            'tradeSecurityDetailsCreatedDateTime',
            'tradeSecurityDetailsUpdatedBy',
            'tradeSecurityDetailsUpdatedDateTime',
            'tradeDetailsId',
            'tradeDetailsVersion',
            'tradeDetailsCreatedBy',
            'tradeDetailsCreatedDateTime',
            'tradeDetailsUpdatedBy',
            'tradeDetailsUpdatedDateTime',
            'tradeClientValueMarkupId',
            'tradeClientValueMarkupVersion',
            'tradeClientValueMarkupCreatedBy',
            'tradeClientValueMarkupCreatedDateTime',
            'tradeClientValueMarkupUpdatedBy',
            'tradeClientValueMarkupUpdatedDateTime',
            'tradeClientValueId',
            'tradeClientValueVersion',
            'tradeClientValueCreatedBy',
            'tradeClientValueCreatedDateTime',
            'tradeClientValueUpdatedBy',
            'tradeClientValueUpdatedDateTime',
            'tradeProductDetailsId',
            'tradeProductDetailsVersion',
            'tradeProductDetailsCreatedBy',
            'tradeProductDetailsCreatedDateTime',
            'tradeProductDetailsUpdatedBy',
            'tradeProductDetailsUpdatedDateTime',
            'etlStatusId',
            'etlStatusVersion',
            'etlStatusCreatedBy',
            'etlStatusCreatedDateTime',
            'etlStatusUpdatedBy',
            'etlStatusUpdatedDateTime',
            'auditDetailsId',
            'auditDetailsVersion',
            'auditDetailsCreatedBy',
            'auditDetailsCreatedDateTime',
            'auditDetailsUpdatedBy',
            'auditDetailsUpdatedDateTime'
        ]],
        how="left",
        left_on=['tradeId'],
        right_on=['tradeId']
    )
    return trades_to_upload

# COMMAND ----------

def check_for_duplicated_indices(dfs: list[DataFrame]) -> None:
    logging.info("Checking for any duplicate indexes...")

    for idx, df in enumerate(dfs):
        if not df.index.is_unique:
            logging.error(f"DataFrame {idx} in list has duplicated indices: {idx}:\n{df[df.index.duplicated(keep=False)]}")

# COMMAND ----------

# DBTITLE 1,Disable Invalid Trades
def disable_existing_invalid_trades(trades_to_upload: DataFrame, existing_trades: DataFrame) -> DataFrame:
    trades_to_upload['isActive'] = True
    missing_existing_trades_filter = ~(existing_trades['tradeId'].isin(trades_to_upload['tradeId']))

    existing_trades.loc[
        missing_existing_trades_filter,
        'isActive'
    ] = False

    try:
        trades_to_upload = trades_to_upload.reset_index(drop=True)
        trades_to_merge = existing_trades.loc[missing_existing_trades_filter]
        trades_to_merge = trades_to_merge.reset_index(drop=True)

        if not trades_to_merge.empty:
            return pd.concat([trades_to_upload, trades_to_merge], axis=0, ignore_index=True)
        else:
            return trades_to_upload
    except Exception as ex:
        logging.error(f"Error occured when disabling existing invalid trades: {ex}", exc_info=True)
        logging.info(f"Trades to upload: {trades_to_upload}")
        logging.info(f"Existing trades: {existing_trades.loc[missing_existing_trades_filter]}")

        check_for_duplicated_indices([trades_to_upload, existing_trades.loc[missing_existing_trades_filter]])

        raise ex


# COMMAND ----------

# DBTITLE 1,Use Historic DV01 where Available
DV01_COLS = ['c360Dv01', 'c360Dv01Source']

def replace_default_dv01_with_historical_dv01(trades: DataFrame, existing_trades: DataFrame) -> DataFrame:
    trades_with_missing_dv01_filter = (
        (trades['tradeSystem'] == 'CALYPSO') &
        (trades['isClientFacing'] == True) &
        (trades['c360Dv01Source'].astype(str).str.contains('DV01 Calculator = _deriv_dv01_calculator', na=True))
    )

    if trades_with_missing_dv01_filter.any():
        existing_trades_with_good_dv01_filter = (
            (existing_trades['tradeSystem'] == 'CALYPSO') &
            (existing_trades['isClientFacing'] == True) &
            ~(existing_trades['c360Dv01Source'].astype(str).str.contains('DV01 Calculator = _deriv_dv01_calculator', na=False))
        )

        if existing_trades_with_good_dv01_filter.any():
            trades.loc[trades_with_missing_dv01_filter, DV01_COLS] = trades.loc[trades_with_missing_dv01_filter].drop(
                columns=DV01_COLS
            ).merge(
                existing_trades.loc[existing_trades_with_good_dv01_filter],
                left_on='tradeId',
                right_on='tradeId',
                how='left'
            )[DV01_COLS].values

    return trades

# COMMAND ----------

# DBTITLE 1,Merge existing user based markups
CLIENT_VALUE_MARKUP_COLS = [
    'clientValueMarkupValueCad',
    'clientValueMarkupSource'
]

def update_with_existing_cv_markups(trades: DataFrame, existing_trades: DataFrame) -> DataFrame:
    existing_trades_with_markups = existing_trades.loc[
        (existing_trades['clientValueMarkupSource'].notnull()) &
        (existing_trades['clientValueMarkupSource'].isin(['USER_MARKUP']))
    ]
    trades_to_apply_markup_to = trades_to_upload['tradeClientValueMarkupId'].isin(existing_trades_with_markups['tradeClientValueMarkupId'])
    if not existing_trades_with_markups.empty:
        trades.loc[trades_to_apply_markup_to, CLIENT_VALUE_MARKUP_COLS] = trades.loc[trades_to_apply_markup_to].drop(
            columns=CLIENT_VALUE_MARKUP_COLS
        ).merge(
            existing_trades_with_markups,
            left_on='tradeClientValueMarkupId',
            right_on='tradeClientValueMarkupId',
            how='left'
        )[CLIENT_VALUE_MARKUP_COLS].values

    return trades

# COMMAND ----------

# DBTITLE 1,Auth Class For Client Value
class MyCustomAuth(httpx.Auth):
    def __init__(self, env: str, client_id: str, client_secret: str):
        self._env = env
        self._client_id = client_id
        self._client_secret = client_secret
        self._sync_lock = threading.RLock()
        self._async_lock = asyncio.Lock()

    def sync_get_token(self):
        with self._sync_lock:
            with no_ssl_verification():
                token = AccessTokenUtil.get_access_token(
                    env=self._env,
                    client_id=self._client_id,
                    client_secret=self._client_secret
                )
                return token

    def sync_auth_flow(self, request):
        token = self.sync_get_token()
        request.headers["Authorization"] = f"Bearer {token}"
        yield request

    async def async_get_token(self):
        async with self._async_lock:
            with no_ssl_verification():
                token = AccessTokenUtil.get_access_token(
                    env=self._env,
                    client_id=self._client_id,
                    client_secret=self._client_secret
                )
                return token

    async def async_auth_flow(self, request):
        token = await self.async_get_token()
        request.headers["Authorization"] = f"Bearer {token}"
        yield request

# COMMAND ----------

# DBTITLE 1,Enrich trades with existing trade fields
def enrich_trades_with_existing_trades(current_trades: DataFrame, existing_trades: DataFrame) -> DataFrame:
    enrich_trades_with_existing_trades = set_existing_id_and_version(current_trades, existing_trades)
    enrich_trades_with_existing_trades = disable_existing_invalid_trades(enrich_trades_with_existing_trades, existing_trades)

    if 'tradeClientValueMarkupId' in existing_trades and 'tradeClientValueMarkupId' in current_trades:
        enrich_trades_with_existing_trades = update_with_existing_cv_markups(enrich_trades_with_existing_trades, existing_trades)

    enrich_trades_with_existing_trades = replace_default_dv01_with_historical_dv01(enrich_trades_with_existing_trades, existing_trades)

    COLS_TO_DROP = [
        'c360DaysToMaturity'
    ]

    enrich_trades_with_existing_trades = enrich_trades_with_existing_trades.drop(columns=COLS_TO_DROP, errors='ignore')

    return enrich_trades_with_existing_trades

# COMMAND ----------

# DBTITLE 1,function for creating tasks to upload trades
async def create_upload_tasks(
    env: str,
    client_id: str,
    client_secret: str,
    trades: DataFrame,
    trade_date: datetime.date
):
    logging.info("Uploading {} trades to the client value api".format(trades.shape[0]))
    limits = httpx.Limits(
        max_keepalive_connections=CLIENT_VALUE_MAX_KEEPALIVE_CONNECTIONS,
        max_connections=CLIENT_VALUE_MAX_CONNECTIONS
    )
    auth = MyCustomAuth(
        env=env,
        client_id=client_id,
        client_secret=client_secret
    )
    tasks = []
    errors = False

    async with httpx.AsyncClient(limits=limits, auth=auth, verify=VERIFY_SSL) as client:
        for inventory, df_group in trades.groupby(['inventory']):
            inventory_chunks_df = [df_group[i:i+chunk_size] for i in range(0, len(df_group), chunk_size)]

            logging.info("Uploading {0} chunks for inventory {1}".format(len(inventory_chunks_df), inventory))

            inventory_name = str(inventory).replace('/', '.')

            for inventory_chunk_df in inventory_chunks_df:
                logging.info("Stating upload for inventory {0} chunk".format(inventory_chunk_df['inventory'].unique().tolist()))

                tasks.append(asyncio.ensure_future(UploadUtil.upload_to_client_value(
                    client=client,
                    trade_date=trade_date,
                    inventory_name=inventory_name,
                    trades=inventory_chunk_df,
                    client_value_url=client_value_url,
                    get_existing_trades_for=get_existing_trades_for,
                    enrich_trades_with_existing_trades=enrich_trades_with_existing_trades,
                    cleanup_trades=cleanup_trades,
                    trade_system=system
                )))

        responses = await asyncio.gather(*tasks)

        for response in responses:
            if response.status_code != 204:
                logging.error("Failed to upload inventory: {0} for trade date: {1}, status code: {2}, response: {3}".format(
                    response.request.headers['inventory'],
                    response.request.headers['trade_date'],
                    response.status_code,
                    response.text
                ))
                errors = True
            else:
                logging.info("Successfully Uploaded inventory: {0}, trade date: {1}".format(
                    response.request.headers['inventory'],
                    response.request.headers['trade_date']
                ))

    if errors:
        raise Exception("Failed to load all inventories successfully")

# COMMAND ----------

# DBTITLE 1,Check if existing trades are available
trades_to_upload = t_plus_one_trades.copy()
if system != 'ANVIL':
    trades_to_upload['tradeId'] = np.floor(pd.to_numeric(trades_to_upload['tradeId'], errors='coerce')).astype(int).astype('str')

if rerun:
    logging.info("Retrieving existing trade ids")
    token = AccessTokenUtil.get_access_token(
        env=env,
        client_id=client_id,
        client_secret=client_secret
    )
    headers = {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
    }

    url = f"{client_value_url}/trades/export?" \
          f"active=true" \
          f"&tradeSystem={system}" \
          f"&tradeDate={trade_date.isoformat()}"

    existing_trades = get_existing_trades(url, headers)

    if not existing_trades.empty:
        if system != 'ANVIL':
            existing_trades['tradeId'] = np.floor(pd.to_numeric(existing_trades['tradeId'], errors='coerce')).astype(int).astype('str')
        trades_to_upload = enrich_trades_with_existing_trades(trades_to_upload, existing_trades)

# COMMAND ----------

# DBTITLE 1,Upload all Client360 Trades
trades_to_upload = cleanup_trades(trades_to_upload)

nest_asyncio.apply()

start_time = time.time()
asyncio.run(create_upload_tasks(
    trades=trades_to_upload,
    trade_date=trade_date,
    env=env,
    client_id=client_id,
    client_secret=client_secret
))
logging.info("--- %s seconds ---" % (time.time() - start_time))

