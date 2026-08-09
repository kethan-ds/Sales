# Databricks notebook source
# DBTITLE 1,Initialise Parameters
dbutils.widgets.text("CATALOG", "")
dbutils.widgets.text("KDB_CLIENT_ID", "")
dbutils.widgets.text("KDB_CLIENT_SECRET", "")
dbutils.widgets.text("W_FX_PANORAMIC_URL", "")
dbutils.widgets.text("START_DATE", "")
dbutils.widgets.text("END_DATE", "")
dbutils.widgets.text("DATE_INTERVAL_STEP_RANGE", "")

# COMMAND ----------

# DBTITLE 1,Get Configurations
CATALOG_NAME = dbutils.widgets.get('CATALOG')
KDB_CLIENT_ID_NAME = dbutils.widgets.get('KDB_CLIENT_ID')
KDB_CLIENT_SECRET_NAME = dbutils.widgets.get('KDB_CLIENT_SECRET')
FX_PANORAMIC_SERVICE_URL = dbutils.widgets.get('W_FX_PANORAMIC_URL')

START_DATE = dbutils.widgets.get('START_DATE')
END_DATE = dbutils.widgets.get('END_DATE')
DATE_INTERVAL_STEP_RANGE = dbutils.widgets.get('DATE_INTERVAL_STEP_RANGE')

# COMMAND ----------

# DBTITLE 1,Run Raw Notebook
try:
    dbutils.notebook.run("./raw_fx_panoramic_trades.py", 18000, {
        "CATALOG": CATALOG_NAME,
        "KDB_CLIENT_ID": KDB_CLIENT_ID_NAME,
        "KDB_CLIENT_SECRET": KDB_CLIENT_SECRET_NAME,
        "W_FX_PANORAMIC_URL": FX_PANORAMIC_SERVICE_URL,
        "START_DATE": START_DATE,
        "END_DATE": END_DATE,
        "DATE_INTERVAL_STEP_RANGE": DATE_INTERVAL_STEP_RANGE
    })
except Exception as e:
    raise Exception(f"Error occurred while running raw notebook: {e}")

# COMMAND ----------

# DBTITLE 1,Run Bronze Notebook
try:
    dbutils.notebook.run("./bronze_fx_panoramic_trades.py", 600, {
        "CATALOG": CATALOG_NAME,
    })
except Exception as e:
    raise Exception(f"Error occurred while running bronze notebook: {e}")

# COMMAND ----------

# DBTITLE 1,Run Silver Notebook
try:
    dbutils.notebook.run("./silver_fx_panoramic_trades.py", 600, {
        "CATALOG": CATALOG_NAME,
    })
except Exception as e:
    raise Exception(f"Error occurred while running silver notebook: {e}")