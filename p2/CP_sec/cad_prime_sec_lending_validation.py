# Databricks notebook source
import os
from collections import defaultdict, namedtuple
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from itertools import groupby
from typing import TypedDict

import plotly.express as px
import pyspark.sql.functions as F
from httpx import Client
from jinja2 import Environment
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window, WindowSpec

# COMMAND ----------

def is_first_day_of_fiscal_year(fiscal_date_year: date) -> bool:
    """
    Return True when the supplied date is the first business day
    of the November-starting fiscal year.
    """
    if not fiscal_date_year:
        return False

    month = fiscal_date_year.month

    if month != 11:
        return False

    original_start_date = date(
        year=fiscal_date_year.year,
        month=fiscal_date_year.month,
        day=fiscal_date_year.day,
    )
    rewind_date = original_start_date

    while (fiscal_date_year - timedelta(days=1)).month == 11:
        fiscal_date_year = fiscal_date_year - timedelta(days=1)

        if fiscal_date_year.weekday() not in (5, 6):
            rewind_date = fiscal_date_year

    return rewind_date == original_start_date

# COMMAND ----------

def get_last_date_for_fyear(year: int) -> date:
    """Return the last business day in October for the supplied year."""
    fy_end_date = date(year=year, month=10, day=31)

    while True:
        if fy_end_date.weekday() not in (5, 6):
            return fy_end_date

        fy_end_date = fy_end_date - timedelta(days=1)

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

dbutils.widgets.text("current_date", "")
dbutils.widgets.text("holiday_regions", "NYC,TOR")
dbutils.widgets.text("client_id", "")
dbutils.widgets.text("client_secret", "")

http_client = Client(verify=False)

current_date_widget = dbutils.widgets.get("current_date").strip()
current_date = (
    datetime.strptime(current_date_widget, "%Y-%m-%d").date()
    if current_date_widget
    else datetime.now().date()
)

if is_first_day_of_fiscal_year(current_date):
    current_date = get_last_date_for_fyear(current_date.year)

fiscal_year = (
    current_date.year
    if current_date.month < 11
    else current_date.year + 1
)

regions_to_observe = ",".join(
    [
        f"'{region.strip()}'"
        for region in dbutils.widgets.get("holiday_regions").split(",")
        if region.strip()
    ]
)

client_id = dbutils.widgets.get("client_id")
client_secret = dbutils.widgets.get("client_secret")

rivendell_auth_url = os.getenv("TDSCI_RIVENDELL_AUTH_URL")
tdsci_reference_data_service_url = (
    os.getenv("TDSCI_REFERENCE_DATA_SERVICE_URL")
    + f"/cad-prime-sec-lending-reports/update-or-create/{fiscal_year}"
)
rivendell_auth_scopes = os.getenv(
    "AUTH_SCOPES",
    "roles email audience",
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Get holiday calendar for all regions for the fiscal year

# COMMAND ----------

holiday_calendar_df = spark.sql(
    f"""
    SELECT *
    FROM `{CATALOG_NAME}`.`bronze`.`holiday_calendar`
    """
)

holiday_calendar_df = holiday_calendar_df.filter(
    (F.col("code").isin(
        [
            region.strip()
            for region in dbutils.widgets.get("holiday_regions").split(",")
            if region.strip()
        ]
    ))
    & (F.year(F.col("date")) == fiscal_year)
)

holiday_lookup_set = {
    row["date"] for row in holiday_calendar_df.collect()
}
broadcast_lookup = spark.sparkContext.broadcast(holiday_lookup_set)

display(holiday_calendar_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Custom UDFs to process date differences excluding holidays and weekends

# COMMAND ----------

def can_skip(c_date: date, previous_date: date) -> bool:
    """Return True when all dates between two observations are skippable."""
    delta = timedelta(days=1)
    last_date = c_date - delta

    while last_date != previous_date:
        is_weekend = last_date.weekday() in (5, 6)

        if is_weekend:
            last_date -= delta
        elif last_date in broadcast_lookup.value:
            last_date -= delta
        else:
            return False

    return True

# COMMAND ----------

def holiday_between(c_date: date, previous_date: date) -> bool:
    """Return True when a holiday occurs between two observation dates."""
    delta = timedelta(days=1)
    last_date = c_date - delta

    while last_date != previous_date:
        is_weekend = last_date.weekday() in (5, 6)

        if is_weekend:
            last_date -= delta
        elif last_date in broadcast_lookup.value:
            return True
        else:
            last_date -= delta

    return False


can_skip_udf = F.udf(can_skip, BooleanType())
holiday_between_udf = F.udf(holiday_between, BooleanType())

# COMMAND ----------

# MAGIC %md
# MAGIC # Get all clients and their date differences to the previous run

# COMMAND ----------

TRADE_DATE_COLUMN = "trade_date"
DEFAULT_FUTURE_DATE = "5000-01-01"

sl_bronze_df = spark.sql(
    f"""
    SELECT
        cptyname,
        contra_party,
        period_client_value_sum,
        Source_File
    FROM `{CATALOG_NAME}`.`bronze`.`cad_prime_securities_lending`
    """
)

# COMMAND ----------

# Extract trade date from Source_File.
sl_bronze_df = sl_bronze_df.withColumn(
    "trade_date",
    F.to_date(
        F.regexp_extract(
            F.col("Source_File"),
            r"(\d{2}-\d{2}-\d{4})",
            1,
        ),
        "MM-dd-yyyy",
    ),
)

# COMMAND ----------

# Fiscal year starts in November.
sl_bronze_df = sl_bronze_df.withColumn(
    "fiscal_year",
    F.when(
        F.month(F.col("trade_date")) < 11,
        F.year(F.col("trade_date")),
    ).otherwise(
        F.year(F.col("trade_date")) + 1
    ),
)

client_date_diff_df = sl_bronze_df.filter(
    F.col("fiscal_year") == fiscal_year
)

# COMMAND ----------

window_spec = Window.partitionBy("cptyname").orderBy("trade_date")

client_date_diff_df = (
    client_date_diff_df
    .withColumn(
        "current_date",
        F.col(TRADE_DATE_COLUMN),
    )
    .withColumn(
        "current_day_of_week",
        F.date_format(F.col(TRADE_DATE_COLUMN), "EEEE"),
    )
    .withColumn(
        "previous_date",
        F.lag(
            F.col(TRADE_DATE_COLUMN),
            1,
            DEFAULT_FUTURE_DATE,
        ).over(window_spec),
    )
    .withColumn(
        "previous_date_day_of_week",
        F.date_format(
            F.lag(
                F.col(TRADE_DATE_COLUMN),
                1,
                DEFAULT_FUTURE_DATE,
            ).over(window_spec),
            "EEEE",
        ),
    )
)

# COMMAND ----------

client_date_diff_df = client_date_diff_df.withColumn(
    "date_diff",
    F.datediff(
        F.col(TRADE_DATE_COLUMN),
        F.lag(
            F.col(TRADE_DATE_COLUMN),
            1,
            DEFAULT_FUTURE_DATE,
        ).over(window_spec),
    ),
)

# COMMAND ----------

weekend_cv = (
    (F.col("current_day_of_week") == "Monday")
    & (F.col("previous_date_day_of_week") == "Friday")
)

client_date_diff_df = client_date_diff_df.withColumn(
    "date_diff",
    F.when(weekend_cv, 1).otherwise(F.col("date_diff")),
)

client_date_diff_df = client_date_diff_df.withColumn(
    "can_skip_days",
    F.when(F.col("date_diff") <= 1, True).otherwise(False),
)

# COMMAND ----------

client_date_diff_df = client_date_diff_df.withColumn(
    "can_skip_days",
    F.when(
        (F.col("date_diff") > 1)
        & can_skip_udf(
            F.col("current_date"),
            F.col("previous_date"),
        ),
        True,
    ).otherwise(F.col("can_skip_days")),
)

# COMMAND ----------

client_date_diff_df = client_date_diff_df.withColumn(
    "date_diff",
    F.when(
        F.col("can_skip_days") & (F.col("date_diff") > 1),
        1,
    ).otherwise(F.col("date_diff")),
)

client_date_diff_df.createOrReplaceTempView("client_date_diff")

# COMMAND ----------

# MAGIC %md
# MAGIC # Further refine the client DataFrame
# MAGIC
# MAGIC 1. Total number of groups
# MAGIC 2. Total number of rows per client
# MAGIC 3. Create ideal client for the fiscal year

# COMMAND ----------

total_groups_df = spark.sql(
    """
    SELECT cptyname, COUNT(*) + 1 AS group_count
    FROM client_date_diff
    WHERE date_diff > 1
    GROUP BY cptyname

    UNION

    SELECT cptyname, 1
    FROM client_date_diff
    WHERE date_diff <= 1
      AND cptyname NOT IN (
          SELECT cptyname
          FROM client_date_diff
          WHERE date_diff > 1
      )
    GROUP BY cptyname
    """
)

total_groups_df.createOrReplaceTempView("total_groups")

# COMMAND ----------

total_per_client = spark.sql(
    """
    SELECT cptyname, COUNT(*) AS totals
    FROM client_date_diff
    GROUP BY cptyname
    """
)

total_per_client.createOrReplaceTempView("total_per_client")

# COMMAND ----------

start_date = (
    client_date_diff_df
    .agg(
        F.min(client_date_diff_df.trade_date).alias(
            "first_date_of_fiscal_year"
        )
    )
    .collect()[0]
    .first_date_of_fiscal_year
)

end_date = (
    current_date - timedelta(days=1)
    if current_date != get_last_date_for_fyear(current_date.year)
    else current_date
)


# COMMAND ----------

ideal_dates: list[date] = []

# COMMAND ----------

if not start_date:
    start_date = end_date

ff_date = date(
    year=start_date.year,
    month=start_date.month,
    day=start_date.day,
)

# COMMAND ----------

while ff_date < end_date:
    # Use weekdays and exclude holidays.
    if (
        ff_date.weekday() not in (5, 6)
        and ff_date not in holiday_lookup_set
    ):
        ideal_dates.append(ff_date)

    ff_date = ff_date + timedelta(days=1)

IdealClient = namedtuple(
    "IdealClient",
    field_names=["start_date", "end_date", "dates", "total_dates"],
)

# COMMAND ----------

ideal_client = IdealClient(
    start_date=start_date,
    end_date=end_date,
    dates=ideal_dates,
    total_dates=len(ideal_dates),
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge group, total and ideal-client information

# COMMAND ----------

final_client_df = spark.sql(
    f"""
    SELECT
        c.*,
        tg.group_count,
        tc.totals,
        {ideal_client.total_dates} AS fiscal_year_total
    FROM client_date_diff c
    JOIN total_groups tg
      ON tg.cptyname = c.cptyname
    JOIN total_per_client tc
      ON tc.cptyname = c.cptyname
    """
)

final_client_df.createOrReplaceTempView("final_client_view")

# COMMAND ----------

# MAGIC %md
# MAGIC # Get clients in voilation and capture validation errors

# COMMAND ----------

client_errors_df = spark.sql(
    """
    SELECT DISTINCT
        cptyname,
        'GapsDetected' AS ErrorType
    FROM final_client_view
    WHERE group_count > 1

    UNION

    SELECT DISTINCT
        cptyname,
        'MissingDaysInFiscalYear' AS ErrorType
    FROM final_client_view
    WHERE totals < fiscal_year_total
    """
)

# COMMAND ----------

client_errors_df.createOrReplaceTempView("client_errors")
total_errors = client_errors_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #  Clients that have no errors

# COMMAND ----------

clean_clients_df = spark.sql(
    """
    SELECT DISTINCT cptyname
    FROM client_date_diff
    WHERE cptyname NOT IN (
        SELECT DISTINCT cptyname
        FROM client_errors
    )
    """
)

clean_clients_df.createOrReplaceTempView("clean_clients")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Create groups for Plotly Gantt chart

# COMMAND ----------

def create_groups(client: str, records: list[Row]) -> dict:
    group = 1
    groups = defaultdict(list)

    for record in records:
        if record.date_diff <= 1:
            groups[(record.cptyname, group)].append((group, record))
        else:
            group += 1
            groups[(record.cptyname, group)].append((group, record))

    return {client: groups}


clients = (
    client_date_diff_df
    .select("cptyname")
    .distinct()
    .sort("cptyname")
    .collect()
)

groups = {}

for client_row in clients:
    client = client_row.cptyname

    records = (
        client_date_diff_df
        .select(
            "cptyname",
            "trade_date",
            "period_client_value_sum",
            "date_diff",
        )
        .filter(F.col("cptyname") == client)
        .sort("current_date")
        .collect()
    )

    groups.update(create_groups(client, records))

# COMMAND ----------

# Create Plotly data points

# COMMAND ----------

records = []

for index, client_row in enumerate(clients):
    client = client_row.cptyname

    # The remaining chart/report generation code was not visible
    # in the supplied screenshots.
    for key in groups[client].keys():
        pass