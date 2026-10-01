# Databricks notebook source
from typing import TypedDict
from datetime import date, datetime, timedelta
from collections import defaultdict, namedtuple
from dataclasses import dataclass, field
from decimal import Decimal
from itertools import groupby
import os

import plotly.express as px
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window, WindowSpec, Row
from pyspark.sql.types import (
    StructType, StructField, ArrayType, MapType,
    StringType, IntegerType, DateType, BooleanType
)
from httpx import Client
from jinja2 import Environment

# COMMAND ----------

def is_first_day_of_fiscal_year(fiscal_date_year: date):
    if not fiscal_date_year:
        return False

    month, day, year = fiscal_date_year.month, fiscal_date_year.day, fiscal_date_year.year

    if month != 11:
        return False

    original_start_date = date(
        year=fiscal_date_year.year,
        month=fiscal_date_year.month,
        day=fiscal_date_year.day,
    )
    rewind_date = date(
        year=fiscal_date_year.year,
        month=fiscal_date_year.month,
        day=fiscal_date_year.day,
    )

    while (fiscal_date_year - timedelta(days=1)).month == 11:
        fiscal_date_year = fiscal_date_year - timedelta(days=1)

        if fiscal_date_year.weekday() not in (5, 6):
            rewind_date = fiscal_date_year

    return rewind_date == original_start_date

# COMMAND ----------

def get_last_date_for_fyear(year: int):
    fy_start_date = date(year=year, month=10, day=31)

    while (fy_start_date - timedelta(days=1)).month == 10:
        if fy_start_date.weekday() not in (5, 6):
            return fy_start_date

        fy_start_date = fy_start_date - timedelta(days=1)

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

dbutils.widgets.text("current_date", "")
dbutils.widgets.text("holiday_regions", "NYC,TOR")
dbutils.widgets.text("client_id", "")
dbutils.widgets.text("client_secret", "")
http_client = Client(verify=False)

current_date = (
    datetime.strptime(dbutils.widgets.get("current_date"), "%Y-%m-%d").date()
    if dbutils.widgets.get("current_date").strip()
    else datetime.now().date()
)

if is_first_day_of_fiscal_year(current_date):
    current_date = get_last_date_for_fyear(current_date.year)

fiscal_year = current_date.year if current_date.month < 11 else current_date.year + 1
regions_to_observe = ",".join(
    [f"'{region.strip()}'" for region in dbutils.widgets.get("holiday_regions").split(",")]
)

client_id = dbutils.widgets.get("client_id")
client_secret = dbutils.widgets.get("client_secret")
rivendell_auth_url = os.getenv("TDSCI_RIVENDELL_AUTH_URL")
tdsci_reference_data_service_url = (
    os.getenv("TDSCI_REFERENCE_DATA_SERVICE_URL")
    + f"/cad-prime-reports/update-or-create/{fiscal_year}"
)
rivendell_auth_scopes = os.getenv("AUTH_SCOPES", "roles email audience")

# COMMAND ----------

# MAGIC %md
# MAGIC # Get holiday calendar for all regions for the fiscal year

# COMMAND ----------

holiday_calendar_df = spark.sql(
    f"""
    select * from `{CATALOG_NAME}`.`bronze`.`holiday_calendar`
    where code in ({regions_to_observe})
    """
)
holiday_calendar_df = holiday_calendar_df.filter(F.year(F.col("date")) == fiscal_year)

holiday_lookup_set = {row["date"] for row in holiday_calendar_df.collect()}
broadcast_lookup = spark.sparkContext.broadcast(holiday_lookup_set)

display(holiday_calendar_df)

# COMMAND ----------

# Custom UDF to process that diffs to exclude any holidays weekends

# COMMAND ----------

def can_skip(c_date: date, previous_date: date) -> bool:
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
    delta = timedelta(days=1)
    last_date = c_date - delta

    while last_date != previous_date:
        is_weekend = last_date.weekday() in (5, 6)

        if is_weekend:
            last_date -= delta
        elif last_date in broadcast_lookup.value:
            return True

    return False


# COMMAND ----------

can_skip_udf = F.udf(can_skip, BooleanType())
holiday_between_udf = F.udf(holiday_between, BooleanType())

# COMMAND ----------

# MAGIC %md
# MAGIC # Get all clients and their date differences to the previous run

# COMMAND ----------

TRADE_DATE_COLUMN = "Trade_Date"
DEFAULT_FUTURE_DATE = "5000-01-01"

# COMMAND ----------

client_date_diff_df = spark.sql(
    f"""
    select Firm_Name, Account_Type, PB_PnL_CAD, Trade_Date
    from `{CATALOG_NAME}`.`bronze`.`cad_prime_pnl_summary`
    where Fiscal_Year = '{fiscal_year}'
    """
)

# COMMAND ----------

window_spec = Window.partitionBy("Firm_Name").orderBy("Trade_Date")

client_date_diff_df = client_date_diff_df.withColumn("current_date", F.col(TRADE_DATE_COLUMN))
client_date_diff_df = client_date_diff_df.withColumn(
    "current_day_of_week", F.date_format(F.col(TRADE_DATE_COLUMN), "EEEE")
)
client_date_diff_df = client_date_diff_df.withColumn(
    "previous_date",
    F.lag(F.col(TRADE_DATE_COLUMN), 1, DEFAULT_FUTURE_DATE).over(window_spec),
)
client_date_diff_df = client_date_diff_df.withColumn(
    "previous_date_day_of_week",
    F.date_format(
        F.lag(F.col(TRADE_DATE_COLUMN), 1, DEFAULT_FUTURE_DATE).over(window_spec),
        "EEEE",
    ),
)

# COMMAND ----------

client_date_diff_df = client_date_diff_df.withColumn(
    "date_diff",
    F.date_diff(
        F.col(TRADE_DATE_COLUMN),
        F.lag(F.col(TRADE_DATE_COLUMN), 1, DEFAULT_FUTURE_DATE).over(window_spec),
    ),
)

# COMMAND ----------

weekend_cv = (
    (F.col("current_day_of_week") == "Monday")
    & (F.col("previous_date_day_of_week") == "Friday")
)
client_date_diff_df = client_date_diff_df.withColumn(
    "date_diff", F.when(weekend_cv, 1).otherwise(F.col("date_diff"))
)
client_date_diff_df = client_date_diff_df.withColumn(
    "can_skip_days", F.when(F.col("date_diff") <= 1, True).otherwise(False)
)
client_date_diff_df = client_date_diff_df.withColumn(
    "can_skip_days",
    F.when(
        (F.col("date_diff") > 1)
        & can_skip_udf(F.col("current_date"), F.col("previous_date")),
        True,
    ).otherwise(F.col("can_skip_days")),
)
client_date_diff_df = client_date_diff_df.withColumn(
    "date_diff",
    F.when(
        F.col("can_skip_days") & (F.col("date_diff") > 1), 1
    ).otherwise(F.col("date_diff")),
)
client_date_diff_df.createOrReplaceTempView("client_date_diff")

# COMMAND ----------

# MAGIC %md
# MAGIC # Future refine the client dataframe to include the following:
# MAGIC 1 - Total number of Groups
# MAGIC
# MAGIC 2 - Total number of rows per clients
# MAGIC
# MAGIC 3 - Create ideal client for given fiscal year
# MAGIC

# COMMAND ----------

total_groups_df = spark.sql(
    f"""
    select Firm_Name, count(*) + 1 as group_count
    from client_date_diff
    where date_diff > 1
    group by Firm_Name
    union
    select Firm_Name, 1
    from client_date_diff
    where date_diff <= 1
      and Firm_Name not in (select Firm_Name from client_date_diff where date_diff > 1)
    group by Firm_Name
    """
)
total_groups_df.createOrReplaceTempView("total_groups")

# COMMAND ----------

total_per_client = spark.sql(
    f"""
    select Firm_Name, count(*) as totals
    from client_date_diff
    group by Firm_Name
    """
)
total_per_client.createOrReplaceTempView("total_per_client")

# COMMAND ----------

start_date = (
    client_date_diff_df
    .agg(F.min(client_date_diff_df.Trade_Date).alias("first_date_of_fiscal_year"))
    .collect()[0]
    .first_date_of_fiscal_year
)
end_date = (
    current_date - timedelta(days=1)
    if current_date != get_last_date_for_fyear(current_date.year)
    else current_date
)
ideal_dates: list[date] = []

if not start_date:
    start_date = end_date

ff_date = date(year=start_date.year, month=start_date.month, day=start_date.day)

while ff_date < end_date:
    if ff_date.weekday() not in (5, 6) and ff_date not in holiday_lookup_set:
        ideal_dates.append(ff_date)
    ff_date = ff_date + timedelta(days=1)

IdealClient = namedtuple(
    "IdealClient", field_names=["start_date", "end_date", "dates", "total_dates"]
)
ideal_client = IdealClient(
    start_date=start_date,
    end_date=end_date,
    dates=ideal_dates,
    total_dates=len(ideal_dates),
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Now we merge the groups, total, and ideal client information into a new and final dataframe

# COMMAND ----------

final_client_df = spark.sql(
    f"""
    select c.*, tg.group_count, tc.totals, {ideal_client.total_dates} as fiscal_year_total
    from client_date_diff c
    join total_groups tg on tg.Firm_Name = c.Firm_Name
    join total_per_client tc on tc.Firm_Name = c.Firm_Name
    where tc.Firm_Name = tg.Firm_Name
    """
)
final_client_df.createOrReplaceTempView("final_client_view")

# COMMAND ----------

# MAGIC %md
# MAGIC # get clients in violation and capture their validation errors
# MAGIC

# COMMAND ----------

client_errors_df = spark.sql(
    f"""
    select distinct Firm_Name, "GapsDetected" as ErrorType
    from final_client_view
    where group_count > 1
    union
    select distinct Firm_Name, "MissingDaysInFiscalYear" as ErrorType
    from final_client_view
    where totals < fiscal_year_total
    """
)
client_errors_df.createOrReplaceTempView("client_errors")
total_errors = client_errors_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #clients that have no errors
# MAGIC

# COMMAND ----------

clean_clients_df = spark.sql(
    f"""
    select distinct Firm_Name
    from client_date_diff
    where Firm_Name not in (select distinct Firm_Name from client_errors)
    """
)
clean_clients_df.createOrReplaceTempView("clean_clients")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create groups for Gantt chat for plotly

# COMMAND ----------

def create_groups(client: str, records: list[Row]):
    group = 1
    groups = defaultdict(list)

    for rec in records:
        if rec.date_diff <= 1:
            groups[(rec.Firm_Name, group)].append((group, rec))
        else:
            group += 1
            groups[(rec.Firm_Name, group)].append((group, rec))

    return {client: groups}


clients = client_date_diff_df.select("Firm_Name").distinct().sort("Firm_Name").collect()
groups = {}

for client in clients:
    client = client.Firm_Name
    records = (
        client_date_diff_df
        .select("Firm_Name", "Trade_Date", "PB_PnL_CAD", "date_diff")
        .filter(F.col("Firm_Name") == client)
        .sort("current_date")
        .collect()
    )
    groups.update(create_groups(client, records))

# COMMAND ----------

# MAGIC %md
# MAGIC # Create plotly data points

# COMMAND ----------

records = []

for index, client in enumerate(clients):
    client = client.Firm_Name

    for key in groups[client].keys():
        group = groups[client][key]

        if group:
            start_date = group[0][1].Trade_Date.strftime("%Y-%m-%d")
            end_date = group[-1][1].Trade_Date.strftime("%Y-%m-%d")
            status = "Complete" if len(groups[client].keys()) == 1 else "Gap"
            records.append(
                {
                    "Firm_Name": client,
                    "Start": start_date,
                    "Finish": end_date,
                    "Resource": status,
                }
            )

# COMMAND ----------

# MAGIC %md
# MAGIC # Get missing dates for clients having gaps

# COMMAND ----------

def get_missing_dates(ideal_client, client):
    if not client:
        return defaultdict(list)

    client_missing_dates = defaultdict(list)
    all_dates = (covered_date for covered_date in ideal_client.dates)

    for key in groups[client].keys():
        group_data = groups[client][key]

        if not group_data:
            break

        start_date = group_data[0][1].Trade_Date
        end_date = group_data[-1][1].Trade_Date
        missing_dates_range = []

        for ideal_date in all_dates:
            if ideal_date < start_date:
                missing_dates_range.append(ideal_date)
            elif ideal_date >= start_date and ideal_date < end_date:
                continue
            elif ideal_date == end_date:
                break

        if missing_dates_range:
            client_missing_dates[client].append(
                (missing_dates_range[0], missing_dates_range[-1])
            )

    remaining_dates = list(all_dates)

    if remaining_dates:
        client_missing_dates[client].append((remaining_dates[0], remaining_dates[-1]))

    return client_missing_dates

# COMMAND ----------

missing_df = spark.sql(
    f"""
    select distinct Firm_Name
    from final_client_view
    where totals < fiscal_year_total
      and previous_date != '{DEFAULT_FUTURE_DATE}'
    order by Firm_Name
    """
)
missing_dates = [row.Firm_Name for row in missing_df.collect()]

client_missing_dates = defaultdict(list)

for missing_date_client in missing_dates:
    client_missing_dates.update(
        get_missing_dates(ideal_client=ideal_client, client=missing_date_client)
    )

# COMMAND ----------

def partition(l, size):
    for part_size in range(0, len(l), size):
        yield l[part_size:part_size + size]

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Dataframe to make it easier to store and query clients based on date ranges, etc

# COMMAND ----------

groups = []

days_missing_rows = spark.sql(
    f"""
    select Firm_Name, (fiscal_year_total - totals) as total
    from final_client_view
    group by Firm_Name, fiscal_year_total, totals
    """
).collect()

missing_data = {}
days_missing_records = [
    {r["Firm_Name"]: {"totals": r["total"]}} for r in days_missing_rows
]

for record in days_missing_records:
    missing_data.update(record)

for key, group in groupby(records, lambda x: x["Firm_Name"]):
    data_group = list(group)
    days_missing_in = max(0, missing_data[key]["totals"])
    gaps = len(data_group) - 1
    date_ranges_with_data = [
        {
            "start_date": datetime.strptime(f["Start"], "%Y-%m-%d"),
            "end_date": datetime.strptime(f["Finish"], "%Y-%m-%d"),
        }
        for f in data_group
    ]
    group_type = "INCOMPLETE" if gaps >= 1 or days_missing_in >= 1 else "COMPLETE"

    groups.append(
        {
            "firm_name": key,
            "number_of_gaps": gaps,
            "days_missing_in_fiscal_year": days_missing_in,
            "type": group_type,
            "date_ranges_with_data": date_ranges_with_data,
        }
    )

schema = StructType([
    StructField("firm_name", StringType(), False),
    StructField("number_of_gaps", IntegerType(), False),
    StructField("days_missing_in_fiscal_year", IntegerType(), False),
    StructField("type", StringType(), False),
    StructField(
        "date_ranges_with_data",
        ArrayType(
            StructType([
                StructField("start_date", DateType(), False),
                StructField("end_date", DateType(), False),
            ])
        ),
    ),
])

groups = spark.createDataFrame(groups, schema)
groups.createOrReplaceTempView("plotly_data_points")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create plotly Gantt chart

# COMMAND ----------

colors = {
    "Gap": "rgb(255, 10, 0)",
    "Complete": "rgb(10, 255, 70)",
}

min_bar_height = 40
padding = 10

if records:
    for record_partition in partition(records, len(records)):
        zero_duration = [
            record for record in record_partition if record["Start"] == record["Finish"]
        ]
        non_zero_duration = [
            record for record in record_partition if record["Start"] != record["Finish"]
        ]
        fig = None

        if not non_zero_duration and zero_duration:
            fig = px.timeline(
                zero_duration,
                x_start="Start",
                x_end="Finish",
                y="Firm_Name",
                color="Resource",
                color_discrete_map=colors,
            )
            calculated_height = len(zero_duration) * min_bar_height + padding

        if non_zero_duration:
            fig = px.timeline(
                non_zero_duration,
                x_start="Start",
                x_end="Finish",
                y="Firm_Name",
                color="Resource",
                color_discrete_map=colors,
            )
            calculated_height = len(non_zero_duration) * min_bar_height + padding

        if zero_duration and fig:
            fig.add_scatter(
                x=[record["Start"] for record in zero_duration],
                y=[record["Firm_Name"] for record in zero_duration],
                mode="markers",
                marker=dict(color="red", size=10),
                name="One Day",
            )

        if fig:
            fig.update_layout(
                xaxis=dict(showgrid=True),
                yaxis=dict(autorange="reversed"),
                height=calculated_height,
            )

            token_response = http_client.post(
                rivendell_auth_url,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": rivendell_auth_scopes,
                },
            )
            token_response.raise_for_status()

            token = token_response.json()["access_token"]
            headers = {"Authorization": f"Bearer {token}"}

            response = http_client.put(
                tdsci_reference_data_service_url,
                content=fig.to_html(full_html=False),
                headers=headers,
            )
            response.raise_for_status()
            fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create report

# COMMAND ----------

# MAGIC %md
# MAGIC # Reports need to be generating highlighting the following errors
# MAGIC
# MAGIC 1 - Clients with gaps in dates
# MAGIC
# MAGIC 2 - Clients missing entire date ranges for fiscal year

# COMMAND ----------

template_string = """
Hi Team,

We found some gaps with the CAD Prime feed. We found the following:

{% if has_gap_data -%}
Firm Names with gaps:
{% for client in gap_clients -%}
{{ client.Firm_Name }}: Dates with gap {{ client.previous_date }} to {{ client.current_date }} total gap days: {{ client.date_diff }}
{% endfor -%}
{% endif %}
{% if has_missing_days_in_fiscal_year -%}
Firms with gaps in the fiscal year:
{% for client, date_ranges in client_missing_dates.items() -%}
Client: {{ client }}
{% for start_date, end_date in date_ranges -%}
Missing Dates: {{ start_date }} to {{ end_date }}
{% endfor -%}
{% endfor -%}
{% endif -%}

Regards,

TDSCI Support Team
"""

# COMMAND ----------

if records and total_errors:
    env = Environment()
    template = env.from_string(template_string)

    has_gaps_in_data = (
        client_errors_df.filter(F.col("ErrorType") == "GapsDetected").count() >= 1
    )
    has_missing_days_in_fiscal_year = (
        client_errors_df
        .filter(F.col("ErrorType") == "MissingDaysInFiscalYear")
        .count()
        >= 1
    )

    gaps_df = spark.sql(
        f"""
        select *
        from final_client_view
        where group_count > 1
          and not can_skip_days
          and previous_date != '{DEFAULT_FUTURE_DATE}'
        order by Firm_Name, Trade_Date
        """
    )
    gaps = gaps_df.collect()

    output = template.render(
        has_gap_data=has_gaps_in_data,
        gap_clients=gaps,
        has_missing_days_in_fiscal_year=has_missing_days_in_fiscal_year,
        client_missing_dates=client_missing_dates,
    )

    print(output)

# COMMAND ----------

# MAGIC %md
# MAGIC # Report with gaps to review in production issues

# COMMAND ----------

if total_errors:
    display(
        gaps_df
        .filter(F.col("date_diff") >= 2)
        .sort("totals", ascending=True)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Report with missing dates in fiscal year to review in production issues

# COMMAND ----------

if total_errors:
    display(missing_df.select("Firm_Name").distinct().sort("Firm_Name"))