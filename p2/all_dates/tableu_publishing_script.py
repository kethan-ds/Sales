# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

import pandas as pd
import configparser
import logging
import os, shutil, zipfile, time
from tableau_api_lib import TableauServerConnection
from tableauhyperapi import HyperProcess, Telemetry
import pantab
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

dbutils.widgets.text("ENV", "dev")
dbutils.widgets.text("NPID", "")
dbutils.widgets.text("NPID_PASSWORD", "")
dbutils.widgets.text("TABLEAU_PROJECT_ID", "")
dbutils.widgets.text("CATALOG", "")

# COMMAND ----------

ENV = dbutils.widgets.get("ENV").lower()
NPID = dbutils.widgets.get("NPID")
NPID_PASSWORD = dbutils.widgets.get("NPID_PASSWORD")
TABLEAU_PROJECT_ID = dbutils.widgets.get("TABLEAU_PROJECT_ID")

CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

HYPER_FILE_PATH_DEFAULT = "data_hyper_file.hyper"
ZIP_FILE_PATH_DEFAULT = "downloaded_x_dealer_zip_extract"

# COMMAND ----------

@enhanced_errors()
def _publish_to_tableau(
    df:pd.DataFrame
    , tableau_env:str
    , datasource_name:str
    , retry_threshold:int
) -> None:
    # Create Hyper File from new dataframe
    #logger.debug("Creating Hyper File..")
    try:
        with HyperProcess(
            Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
            , "myapp"
            , parameters={"log_config": ""}
        ) as hyper:
            pantab.frame_to_hyper(df, HYPER_FILE_PATH_DEFAULT, table="Extract", hyper_process=hyper)
    except Exception: # This roundabout way is helpful when you want to log things to a file
        #logger.exception("Problem creating hyper file. Error msg:")
        # remove_artifacts(datasource_name=datasource_name)
        raise

    #Starting Publish step now..

    tableau_url = {
        "dev" : "https://tabcluat.td.com",
        "pat" : "https://tabcluat.td.com",
        "prod" : "https://tableaupro.td.com"
    }

    # Publish Hyper File
    env = {
        "server": tableau_url[ENV.lower()],
        "api_version": "3.21", # This makes me uneasy. Explore later.
        "username": "CORP.TDSECURITIES.com\\" + NPID,
        "password": NPID_PASSWORD,
        "site_name": "TDS",
        "site_url": "TDS"
    }

    tableau_config = {
        "my_env": env
    }

    conn = TableauServerConnection(config_json=tableau_config, env="my_env")
    conn.sign_in()
    retry_counter = 0
    keep_retrying:bool = True
    while keep_retrying:
        try:
            response = conn.publish_data_source(
                datasource_file_path=HYPER_FILE_PATH_DEFAULT
                , datasource_name=datasource_name
                , project_id=TABLEAU_PROJECT_ID
            )
            #Completed Publish
            keep_retrying = False
        except requests.exceptions.ConnectionError:
            # Retry
            if retry_counter > retry_threshold:
                #Too many retries
                keep_retrying = False
            else:
                retry_counter += 1
                time.sleep(1)
        except Exception:
            #Failed Publishing
            keep_retrying = False
    conn.sign_out()

# COMMAND ----------

GOLD_ALL_DATES_SRM_AGG_TABLE = f"`{CATALOG_NAME}`.`gold`.`all_dates_srm_agg`"

# COMMAND ----------

all_dates_srm_agg = spark.table(GOLD_ALL_DATES_SRM_AGG_TABLE)

# COMMAND ----------

@enhanced_errors()
def convert_decimal_to_float_from_table(df):
    # check the decimal columns values
    decimal_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, DecimalType)]

    # using below for loop you can convert decimal columns to float
    for col_name in decimal_columns:
        df = df.withColumn(col_name, F.col(col_name).cast(FloatType()))

    return df

# COMMAND ----------

all_dates_srm_agg = convert_decimal_to_float_from_table(all_dates_srm_agg)

# COMMAND ----------

pandas_all_dates = all_dates_srm_agg.toPandas()

# COMMAND ----------

tableau_env = {
    "dev" : "uat",
    "pat" : "uat",
    "prod" : "prod"
}


# COMMAND ----------

_publish_to_tableau(df=pandas_all_dates,
                    tableau_env=tableau_env[ENV],
                    datasource_name="SRM Aggregate ADB",
                    retry_threshold=3,
                    )