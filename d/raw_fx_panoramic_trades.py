# Databricks notebook source
import os

# COMMAND ----------

import json
import gzip
import requests
import datetime
import logging
from datetime import date
from datetime import datetime, timedelta
from dateutil.parser import parse
from io import BytesIO

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./dateutils.py"

# COMMAND ----------

# MAGIC %run "./network_utils.py"

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
# volume_util.py reads CATALOG_NAME via os.getenv, so the widget value must
# be pushed into the environment BEFORE it runs.
os.environ["CATALOG_NAME"] = dbutils.widgets.get("CATALOG")

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# MAGIC %run "./access_token_util.py"

# COMMAND ----------

# DBTITLE 1,Get configurations
# job task parameters widgets
dbutils.widgets.text("KDB_CLIENT_ID", "")
dbutils.widgets.text("KDB_CLIENT_SECRET", "")
dbutils.widgets.text("W_FX_PANORAMIC_URL", "")
dbutils.widgets.text("START_DATE", "")
dbutils.widgets.text("END_DATE", "")
dbutils.widgets.text("DATE_INTERVAL_STEP_RANGE", "")

# COMMAND ----------

KDB_CLIENT_ID_NAME = dbutils.widgets.get('KDB_CLIENT_ID')
KDB_CLIENT_SECRET_NAME = dbutils.widgets.get('KDB_CLIENT_SECRET')
FX_PANORAMIC_SERVICE_URL = dbutils.widgets.get('W_FX_PANORAMIC_URL')

# COMMAND ----------

# DBTITLE 1,Define Variables
# Setting up variables
data_set_type = "trades"
trade_system = "fx_panoramic"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# COMMAND ----------

# DBTITLE 1,Volume (FUSE) output path helpers
@enhanced_errors()
def volume_output_path(trade_start_date_plain, trade_end_date_plain, year, month, day):
    """
    Builds the Volume (FUSE) path for the compressed JSON output file and
    ensures its parent directory exists.

    Args:
        trade_start_date_plain (str): The start date of the trade in plain format (YYYYMMDD).
        trade_end_date_plain (str): The end date of the trade in plain format (YYYYMMDD).
        year (str): The year component of the file path.
        month (str): The month component of the file path.
        day (str): The day component of the file path.

    Returns:
        str: The full Volume path the file should be written to.
    """
    try:
        logging.info("Generating Volume output path.")
        target_dir = f"{VOLUME_BASE_PATH}/trades/{trade_system}/{year}/{month}/{day}"
        dbutils.fs.mkdirs(target_dir)
        file_name = (
            f"d_{data_set_type}_{trade_system}_"
            f"{trade_start_date_plain}_{trade_end_date_plain}.json.gz"
        )
        target_path = f"{target_dir}/{file_name}"
        logging.info("Volume output path generated successfully.")
        return target_path
    except Exception as e:
        logging.error(f"Error generating Volume output path: {e}")
        raise

# COMMAND ----------

def volume_path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

# COMMAND ----------

# DBTITLE 1,Date Range
def generate_date_ranges(start_date, end_date, step_days=15):
    """
    Generates a list of date ranges between a start date and an end date,
    with each range spanning a specified number of days.
    """
    logging.info("Generating date ranges.")
    date_ranges = []
    current = start_date

    while current < end_date:
        range_end = min(current + timedelta(days=step_days - 1), end_date)
        date_ranges.append((current, range_end))
        logging.info(f"Added range: {current} to {range_end}")
        current = range_end + timedelta(days=1)

    logging.info("Date ranges generated successfully.")
    return date_ranges

# COMMAND ----------

# DBTITLE 1,Fetch, compress and store data
def compress_data(data):
    """Compress the given data using gzip."""
    logging.info("Starting data compression.")
    compressed_data = BytesIO()
    with gzip.GzipFile(fileobj=compressed_data, mode="wb") as gz:
        gz.write(data)
    compressed_data.seek(0)  # Reset the pointer to the beginning of the stream
    logging.info("Data compression completed successfully.")
    return compressed_data.read()

# COMMAND ----------

@enhanced_errors()
def fetch_data(start_date, end_date, modified_date=None):
    """Fetch data from the API with optional modified_date parameter."""
    try:
        logging.info("Fetching data from API.")
        access_token = get_oauth2_access_token(KDB_CLIENT_ID_NAME, KDB_CLIENT_SECRET_NAME)

        s_date = FormattedDate(start_date)
        e_date = FormattedDate(end_date)
        endpoint = ""

        # Build parameters dynamically based on the presence of modified_date
        params = f"startTradeDate={s_date.dot}&endTradeDate={e_date.dot}"
        if modified_date:
            m_date = FormattedDate(modified_date)
            params += f"&modifiedDate={m_date.dot}"

        url = f"{FX_PANORAMIC_SERVICE_URL}{endpoint}"
        logging.info(f"Constructed URL: {url}")

        response = get_raw_data(
            KDB_CLIENT_ID_NAME,
            KDB_CLIENT_SECRET_NAME,
            method="GET",
            url=url,
            parameters=params,
            payload={},
            stream=True,
            verify_ssl=False,
            headers={},
            with_token=True
        )

        logging.info("Data fetched successfully.")
        return response
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        raise

# COMMAND ----------

@enhanced_errors()
def store_data(response, start_date, end_date):
    """
    Store the fetched data into the Volume (FUSE) path in compressed gzip
    format. Skips uploading if the file already exists.
    """
    try:
        logging.info("Starting data storage process.")
        s_date = FormattedDate(start_date)
        e_date = FormattedDate(end_date)

        # Use today's date for year, month, and day
        year, month, day = date.today().year, f"{date.today().month:02}", f"{date.today().day:02}"
        target_path = volume_output_path(s_date.plain, e_date.plain, year, month, day)

        # Check if the file already exists
        if volume_path_exists(target_path):
            logging.info(f"File already exists for {s_date.plain} to {e_date.plain}. Skipping upload.")
            return

        # Compress the response content
        compressed_content = compress_data(response.content)
        if compressed_content is None:
            logging.error("Failed to compress data. Aborting upload.")
            return

        # Write the compressed data directly through the Volume FUSE mount
        with open(target_path, "wb") as f:
            f.write(compressed_content)

        logging.info(f"File uploaded successfully for {s_date.plain} to {e_date.plain}.")
    except Exception as e:
        logging.error(f"Error during data storage: {e}")
        raise

# COMMAND ----------

@enhanced_errors()
def fetch_and_store(start_date, end_date, modified_date=None):
    """
    Fetch data from the API and store it in the Volume (FUSE) path.
    Skips the API call and storage if the file already exists.
    """
    logging.info("Starting fetch and store process.")
    s_date = FormattedDate(start_date)
    e_date = FormattedDate(end_date)

    # Use today's date for year, month, and day
    year, month, day = date.today().year, f"{date.today().month:02}", f"{date.today().day:02}"
    target_path = volume_output_path(s_date.plain, e_date.plain, year, month, day)

    # Check if the file already exists
    if volume_path_exists(target_path):
        logging.info(f"File already exists for {s_date.plain} to {e_date.plain}. Skipping API call and upload.")
        return

    # Fetch data from the API
    response = fetch_data(start_date, end_date, modified_date)
    if response is None:
        logging.error("Failed to fetch data. Aborting storage process.")
        return

    # Store the fetched data
    store_data(response, start_date, end_date)
    logging.info("Fetch and store process completed successfully.")


# COMMAND ----------

# DBTITLE 1,Process and Fetch Date Range
@enhanced_errors()
def process_date_ranges_and_fetch(start_date, end_date, step_days):
    """
    Processes date ranges between the given start and end dates,
    and calls the fetch_and_store function for each range.
    """
    logging.info("Starting process for date ranges.")
    for s_date, e_date in generate_date_ranges(start_date, end_date, step_days):
        logging.info(f"Processing range: {s_date} to {e_date}")
        logging.info(f"The current datetime is {datetime.now()}")
        fetch_and_store(s_date, e_date)
    logging.info("Date range processing completed successfully.")

# COMMAND ----------

def to_datetime(str_date):
    year, month, day = str_date.split('-')
    return datetime(int(year), int(month), int(day))

def get_date_parameters():
    start_date_as_str = dbutils.widgets.get("START_DATE")
    end_date_as_str = dbutils.widgets.get("END_DATE")
    step_as_str = dbutils.widgets.get("DATE_INTERVAL_STEP_RANGE")

    start_date = datetime(2021, 11, 1) if not start_date_as_str else to_datetime(start_date_as_str)
    end_date = datetime.today() if not end_date_as_str else to_datetime(end_date_as_str)
    step_days = 15 if not step_as_str else int(step_as_str)

    return start_date, end_date, step_days

# COMMAND ----------

# DBTITLE 1,Main function
if __name__ == "__main__":
    try:
        process_date_ranges_and_fetch(*get_date_parameters())
    except Exception as e:
        logging.error("Notebook execution failed")
        raise