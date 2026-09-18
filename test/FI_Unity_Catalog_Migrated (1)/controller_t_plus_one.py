# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,Defining widget and getting notebook path from ADF
dbutils.widgets.text("fixed_income_daily_etl_notebook_path", "")
dbutils.widgets.text("cip_notebooks_path", "")

# COMMAND ----------

fixed_income_daily_etl_notebook_path = dbutils.widgets.get('fixed_income_daily_etl_notebook_path')
cip_notebooks_path = dbutils.widgets.get('cip_notebooks_path')

# COMMAND ----------

# DBTITLE 1,Defining all Notebook path
raw_fi_trades_nb = cip_notebooks_path + "raw_fi_trades_client_value_service.py"
bronze_fi_trades_nb = cip_notebooks_path + "bronze_fi_trades.py"
silver_fi_trade_nb = cip_notebooks_path + "silver_fi_trade.py"
silver_fi_trade_client_nb = cip_notebooks_path + "silver_fi_trade_client.py"

# COMMAND ----------

# DBTITLE 1,Parameters to pass to notebooks from ADF
dbutils.widgets.text("client_value_max_connections", "5")
dbutils.widgets.text("client_value_max_keepalive_connections", "2")
dbutils.widgets.text("verify_ssl", "False")
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
dbutils.widgets.text("CATALOG", "")

# COMMAND ----------

# Additional widgets to accept trade_start_date, trade_end_date, retry, rerun flag and rerun dates
dbutils.widgets.text("trade_start_date", "")
dbutils.widgets.text("trade_end_date", "")
dbutils.widgets.text("max_retries", "3")
dbutils.widgets.text("rerun_for_failed_date", "False")
dbutils.widgets.text("rerun_dates", "[]")

# COMMAND ----------

VERIFY_SSL = dbutils.widgets.get('verify_ssl')
CLIENT_VALUE_MAX_CONNECTIONS = int(dbutils.widgets.get('client_value_max_connections'))
CLIENT_VALUE_MAX_KEEPALIVE_CONNECTIONS = int(dbutils.widgets.get('client_value_max_keepalive_connections'))
rerun = dbutils.widgets.get('rerun')
debug = dbutils.widgets.get('debug')
env = dbutils.widgets.get('environment')
system = dbutils.widgets.get('system')
statuses = dbutils.widgets.get('trade_statuses')
client_id = dbutils.widgets.get('client_id')
client_secret = dbutils.widgets.get('client_secret')
dremio_username = dbutils.widgets.get('dremio_username')
dremio_personal_access_token = dbutils.widgets.get('dremio_personal_access_token')
input_trade_date = dbutils.widgets.get('trade_date')
preproc_partition_count = int(dbutils.widgets.get('preproc_partition_count'))
postproc_partition_count = int(dbutils.widgets.get('postproc_partition_count'))
chunk_size = int(dbutils.widgets.get('chunk_size'))

# COMMAND ----------

# CATALOG_NAME must be threaded through to every child notebook below - child
# notebooks launched via dbutils.notebook.run do not inherit catalog context.
CATALOG_NAME = dbutils.widgets.get('CATALOG')

# COMMAND ----------

# Retrieve the additional inputs
trade_start_date = dbutils.widgets.get('trade_start_date')
trade_end_date = dbutils.widgets.get('trade_end_date')
MAX_RETRIES = int(dbutils.widgets.get('max_retries'))  # Convert string to int
rerun_for_failed_date = dbutils.widgets.get("rerun_for_failed_date") == "True"
rerun_dates = eval(dbutils.widgets.get("rerun_dates"))  # Convert string to list


# COMMAND ----------

# DBTITLE 1,Run T+1 for specified dates
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed  # Import ThreadPoolExecutor and as_completed for parallel execution

# COMMAND ----------

# Function to calculate all weekdays between two dates
def get_weekdays(start_date, end_date):
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}. Dates must be in 'YYYY-MM-DD' format.")

    weekdays = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Monday to Friday are 0-4
            weekdays.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return weekdays

# COMMAND ----------

def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

# COMMAND ----------

# Function to run a notebook with retries
def run_notebook_with_retries(notebook_path, timeout, args, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            dbutils.notebook.run(notebook_path, timeout, args)
            return True  # Success
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for notebook {notebook_path}: {e}")
            if attempt == max_retries - 1:
                return False  # Failure after max retries

# COMMAND ----------

# Function to process a single trade date
def process_single_trade_date(input_trade_date):
    print("Processing trade date:", input_trade_date)
    success = False
    try:
        # Update arguments for each notebook with the current trade date
        args_to_tplus1_nb = {
            "client_value_max_connections": CLIENT_VALUE_MAX_CONNECTIONS,
            "client_value_max_keepalive_connections": CLIENT_VALUE_MAX_KEEPALIVE_CONNECTIONS,
            "verify_ssl": VERIFY_SSL,
            "environment": env,
            "rerun": rerun,
            "debug": debug,
            "system": system,
            "trade_statuses": statuses,
            "client_id": client_id,
            "client_secret": client_secret,
            "dremio_username": dremio_username,
            "dremio_personal_access_token": dremio_personal_access_token,
            "trade_date": input_trade_date,
            "preproc_partition_count": preproc_partition_count,
            "postproc_partition_count": postproc_partition_count,
            "chunk_size": chunk_size,
            "CATALOG": CATALOG_NAME
        }

        args_to_raw_fi_trades_nb = {
            "ADB_AUTH_CLIENT_ID": client_id,
            "ADB_AUTH_CLIENT_SECRET": client_secret,
            "TRADE_DATE": input_trade_date,
            "TRADE_SYSTEM": system,
            "CATALOG": CATALOG_NAME
        }

        args_to_bronze_fi_trades_nb = {
            "TRADE_DATE": input_trade_date,
            "TRADE_SYSTEM": system,
            "CATALOG": CATALOG_NAME
        }

        # Run notebooks with retries
        tplus1_success = run_notebook_with_retries(fixed_income_daily_etl_notebook_path, 2400, args_to_tplus1_nb)
        raw_fi_success = run_notebook_with_retries(raw_fi_trades_nb, 600, args_to_raw_fi_trades_nb)
        bronze_fi_success = run_notebook_with_retries(bronze_fi_trades_nb, 600, args_to_bronze_fi_trades_nb)

        if tplus1_success and raw_fi_success and bronze_fi_success:
            success = True
    except Exception as e:
        print(f"Failed to process trade date {input_trade_date}: {e}")
    return input_trade_date, success

# COMMAND ----------

# Determine the trade dates to process
if rerun_for_failed_date:
    trade_dates = rerun_dates  # Use the provided rerun dates
    print("Rerunning for failed dates:", trade_dates)
else:
    trade_dates = get_weekdays(trade_start_date, trade_end_date)  # Calculate weekdays
    print("Trade dates (weekdays only):", trade_dates)

# COMMAND ----------

# Check if the date range exceeds 10 days
if (datetime.strptime(trade_end_date, "%Y-%m-%d") - datetime.strptime(trade_start_date, "%Y-%m-%d")).days > 10:
    trade_date_chunks = split_list(trade_dates, 4)  # Split into 4 chunks
else:
    trade_date_chunks = [trade_dates]  # Use a single chunk


# COMMAND ----------

# Execute the trade dates dynamically with parallel processing
success_dates_all = []
failure_dates_all = []

# COMMAND ----------

# Use ThreadPoolExecutor to process trade dates dynamically
with ThreadPoolExecutor(max_workers=4) as executor:
    # Submit all trade dates as tasks
    future_to_date = {executor.submit(process_single_trade_date, trade_date): trade_date for trade_date in trade_dates}

    # Process tasks as they complete
    for future in as_completed(future_to_date):
        trade_date = future_to_date[future]
        try:
            trade_date, success = future.result()
            if success:
                success_dates_all.append(trade_date)
                print(f"Successfully processed trade date: {trade_date}")
            else:
                failure_dates_all.append(trade_date)
        except Exception as e:
            print(f"Error processing trade date {trade_date}: {e}")
            failure_dates_all.append(trade_date)


# COMMAND ----------

# Print the results
print("Successful trade dates:", success_dates_all)
print("Failed trade dates:", failure_dates_all)

# COMMAND ----------

# Combine success and failure dates into a dictionary to pass as the notebook exit variable
exit_results = {
    "success_dates": success_dates_all,
    "failure_dates": failure_dates_all
}

# COMMAND ----------

# DBTITLE 1,Update silver table and view
# Run the following notebooks after all parallel processes are completed
dbutils.notebook.run(silver_fi_trade_nb, 1200, {"CATALOG": CATALOG_NAME})
dbutils.notebook.run(silver_fi_trade_client_nb, 1200, {"CATALOG": CATALOG_NAME})

# COMMAND ----------

# DBTITLE 1,Return success and failed dates
# Pass the results as the notebook exit variable
dbutils.notebook.exit(str(exit_results))
