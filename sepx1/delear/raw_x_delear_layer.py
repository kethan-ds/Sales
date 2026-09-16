# Databricks notebook source
import os

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
# volume_util.py reads CATALOG_NAME via os.getenv, so the widget value must
# be pushed into the environment BEFORE it runs.
os.environ["CATALOG_NAME"] = dbutils.widgets.get("CATALOG")

# COMMAND ----------

import pyarrow
from pyarrow import flight
import pandas as pd
from datetime import datetime, date
from delta.tables import DeltaTable
from io import StringIO
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

dbutils.widgets.text("environment", "")
dbutils.widgets.text("dremio_username", "")
dbutils.widgets.text("dremio_personal_access_token", "")

# COMMAND ----------

CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

env = dbutils.widgets.get("environment").lower()
dremio_username = dbutils.widgets.get("dremio_username")
dremio_personal_access_token = dbutils.widgets.get("dremio_personal_access_token")

X_DEALER_DELTA_TABLE = f"`{CATALOG_NAME}`.`bronze`.`x_dealer_delta`"

# COMMAND ----------

hostname = {
    "dev": "valrsinfo-atgk1.dev.azure.td.com",
    "pat": "infoplatform-dremio-pat.corp.tdsecurities.com",
    "prod": "infoplatform-dremio.corp.tdsecurities.com",
}

# COMMAND ----------

port = 32010
client = flight.FlightClient("grpc+tcp://" + hostname[env] + ":" + str(port))

# COMMAND ----------

bearer_token = client.authenticate_basic_token(
    dremio_username,
    dremio_personal_access_token
)

# COMMAND ----------

options = flight.FlightCallOptions(headers=[bearer_token])

# COMMAND ----------

product_paths = {
    "GED_NOTES": "GED.GED_NOTES",
    "GED_SWAPS": "GED.GED_SWAPS",
    "GED_OPTIONS": "GED.GED_OPTIONS",
    "high_yield": "HY.high_yield",
    "Inst_Eq": "IE.Inst_Eq",
    "Gov_Finance": "GOV_Finance.Gov_Finance",
    "US_MUNIS_GENERAL": "MUNIS.US_MUNIS_GENERAL",
    "METAL": "COMM.METAL",
    "ENERGY": "COMM.ENERGY",
    "US_PRIME_GENERAL": "PRIME.US_PRIME_GENERAL",
    "TDSAT": "TDSAT.TDSAT",
    "CAD_PRIME": "PRIME.CAD_PRIME",
}

# COMMAND ----------

delta_list = []
for key, value in product_paths.items():
    try:
        sql = (
            f'SELECT DISTINCT LEFT(dir1, LENGTH(dir1)-9) as product, '
            f'CAST(SUBSTRING(dir0,1,4) AS INT) AS "year", '
            f'SUBSTRING(dir0,5,2) AS "month", '
            f'CAST(RIGHT(dir1,8) AS INT) AS uploadDate '
            f'FROM "INFO-HDFS".cdo."C360_Data".{value} '
            f'ORDER BY "year", "month", uploadDate'
        )

        info = client.get_flight_info(
            flight.FlightDescriptor.for_command(sql),
            options
        )
        reader = client.do_get(info.endpoints[0].ticket, options)

        table = reader.read_all()
        result = table.to_pandas()
        delta_list.append(result)

    except Exception as e:
        if env == "dev" or env == "pat":
            continue
        else:
            raise e

dremio_delta = pd.concat(delta_list)

# COMMAND ----------

dremio_delta[["isDoneADLS", "isDoneBronze", "isDoneSilver"]] = False

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {X_DEALER_DELTA_TABLE} (
        product string,
        year int,
        month int,
        uploadDate int,
        isDoneADLS BOOLEAN DEFAULT false,
        isDoneBronze BOOLEAN DEFAULT false,
        isDoneSilver BOOLEAN DEFAULT false
    )
    USING DELTA
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# COMMAND ----------

# Insert NEW Product, year, month uploads into delta table
df = spark.createDataFrame(dremio_delta)
df.createOrReplaceTempView("temp_dremio_delta")

spark.sql(f"""
    MERGE INTO {X_DEALER_DELTA_TABLE} AS bronze
    USING temp_dremio_delta AS dremio
    ON bronze.product = dremio.product
    AND bronze.year = dremio.year
    AND bronze.month = dremio.month
    AND bronze.uploadDate = dremio.uploadDate
    WHEN NOT MATCHED THEN
    INSERT *
""")

spark.catalog.dropTempView("temp_dremio_delta")

# COMMAND ----------

delta_table = spark.sql(f"""
    SELECT * FROM {X_DEALER_DELTA_TABLE}
    WHERE isDoneADLS = FALSE
    ORDER BY uploadDate
""")
adls_delta = delta_table.toPandas()

# COMMAND ----------

with capture_errors("x_dealer_adls_upload"):
    # Upload to the Volume (FUSE path)
    for index, row in adls_delta.iterrows():
        path = product_paths[row["product"]]
        product = row["product"]
        year = row["year"]
        month = row["month"]
        uploadDate = row["uploadDate"]

        target_dir = f"{VOLUME_BASE_PATH}/trades/{product.lower()}/{year}/{month:02d}"
        dbutils.fs.mkdirs(target_dir)
        target_path = f"{target_dir}/m_trades_{product.lower()}_{uploadDate}.csv"

        sql = (
            f'SELECT * FROM "INFO-HDFS".cdo."C360_Data".{path} '
            f"WHERE dir0 = {year}{month:02d} "
            f"AND dir1 = '{product}_{uploadDate}'"
        )

        info = client.get_flight_info(
            flight.FlightDescriptor.for_command(sql),
            options
        )
        reader = client.do_get(info.endpoints[0].ticket, options)

        table = reader.read_all()
        dremio_data = table.to_pandas()

        # drop dir0 and dir1 fields
        dremio_data = dremio_data.drop(columns=["dir0", "dir1"])

        # Add ingestion date field
        dremio_data["ingestion_timestamp"] = datetime.now()

        csv_buffer = StringIO()
        dremio_data.to_csv(csv_buffer, index=False)
        csv_text = csv_buffer.getvalue()

        with open(target_path, "w") as f:
            f.write(csv_text)

        # Update delta table to indicate it's been loaded to the Volume
        spark.sql(f"""
            UPDATE {X_DEALER_DELTA_TABLE}
            SET isDoneADLS = True
            WHERE product = '{product}'
              AND year = '{year}'
              AND month = '{month}'
              AND uploadDate = '{uploadDate}'
        """)