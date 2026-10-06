# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Holiday Calendar (Unity Catalog)
# MAGIC Builds `bronze.holiday_calendar` (managed table) from the Toronto, NYC and London
# MAGIC holiday calendar literals below, one row per holiday date.
# MAGIC
# MAGIC **Before running:** paste the original calendar literals into the three calendar cells. The notebook stops before writing if any calendar is empty, so it can never overwrite the table with no data.

# COMMAND ----------

# DBTITLE 1,Catalog context
import os

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG").strip() or os.getenv("CATALOG_NAME", "")

if not CATALOG_NAME:
    raise ValueError("CATALOG widget (or CATALOG_NAME env var) must be set.")

os.environ["CATALOG_NAME"] = CATALOG_NAME
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

BRONZE_HOLIDAY_CALENDAR = f"`{CATALOG_NAME}`.bronze.holiday_calendar"

# COMMAND ----------

# MAGIC %md
# MAGIC # Toronto holiday calendar

# COMMAND ----------

# TODO: paste the original Toronto calendar literal here (not visible in the source copy).
toronto_holiday_calendar = [
]

# COMMAND ----------

# MAGIC %md
# MAGIC # NYC Holiday Calendar

# COMMAND ----------

# TODO: paste the original NYC calendar literal here (not visible in the source copy).
nyc_holiday_calendar = [
]

# COMMAND ----------

# MAGIC %md
# MAGIC # LON Holiday Calendar

# COMMAND ----------

# TODO: paste the original London calendar literal here (not visible in the source copy).
london_holiday_calendar = [
]

# COMMAND ----------

# DBTITLE 1,Guard: all calendars must be populated
_empty = [
    name for name, cal in {
        "toronto_holiday_calendar": toronto_holiday_calendar,
        "nyc_holiday_calendar": nyc_holiday_calendar,
        "london_holiday_calendar": london_holiday_calendar,
    }.items()
    if not cal
]
if _empty:
    raise ValueError(
        f"Calendar literal(s) empty: {_empty}. Paste the original calendar data before "
        f"running; {BRONZE_HOLIDAY_CALENDAR} was not modified."
    )

# COMMAND ----------

from datetime import date

from delta.tables import DeltaTable
from pyspark.sql.functions import col, month, struct, to_date, to_timestamp, year
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType


# COMMAND ----------

# DBTITLE 1,Create bronze table (managed)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_HOLIDAY_CALENDAR} (
        date DATE,
        code STRING,
        currency STRING,
        audit STRUCT<userId: STRING, validFrom: LONG, timestamp: LONG, forceTimestamp: BOOLEAN, validFromDate: TIMESTAMP, timestampDate: TIMESTAMP>,
        source STRING
    )
    USING DELTA
""")


# COMMAND ----------

def individual_dates(calendar):
    """Explode a calendar record (list holding one dict) into one dict per holiday date."""
    calendar = calendar[0]
    holidays = sorted(calendar["dates"])

    copy_map = {
        "code": calendar["code"],
        "currency": calendar["currency"],
        "audit": calendar["audit"],
        "source": calendar["source"],
    }

    return [{"date": holiday, **copy_map} for holiday in holidays]

# COMMAND ----------

toronto_holidays = individual_dates(toronto_holiday_calendar)
nyc_holidays = individual_dates(nyc_holiday_calendar)
lon_holidays = individual_dates(london_holiday_calendar)

# COMMAND ----------

bronze_schema = StructType([
    StructField("date", LongType(), False),
    StructField("code", StringType(), False),
    StructField("currency", StringType(), False),
    StructField("source", StringType(), True),
    StructField("audit", StructType([
        StructField("userId", StringType(), True),
        StructField("validFrom", LongType(), True),
        StructField("timestamp", LongType(), True),
        StructField("forceTimestamp", BooleanType(), True),
        StructField("validFromDate", StringType(), True),
        StructField("timestampDate", StringType(), True),
    ])),
])

# COMMAND ----------

df = spark.createDataFrame(toronto_holidays, schema=bronze_schema)
df1 = spark.createDataFrame(nyc_holidays, schema=bronze_schema)
df2 = spark.createDataFrame(lon_holidays, schema=bronze_schema)

df_all = df.union(df1).union(df2)


# COMMAND ----------

df_all = df_all.withColumn("dateASStr", to_date(col("date").cast("string"), "yyyyMMdd"))
df_all = df_all.withColumn("validFromDateAsDate", to_timestamp(col("audit.validFromDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
df_all = df_all.withColumn("timestampDateAsDate", to_timestamp(col("audit.timestampDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
df_all = df_all.drop(col("date"))
df_all = df_all.withColumnRenamed("dateASStr", "date")
df_all = df_all.withColumn(
    "audit",
    struct(
        col("audit.userId"),
        col("audit.validFrom"),
        col("audit.timestamp"),
        col("audit.forceTimestamp"),
        col("validFromDateAsDate").alias("validFromDate"),
        col("timestampDateAsDate").alias("timestampDate"),
    ),
)
df_all = df_all.drop(col("validFromDateAsDate"), col("timestampDateAsDate"))

# COMMAND ----------

# Match the table's column order explicitly
df_all = df_all.select("date", "code", "currency", "audit", "source")

# COMMAND ----------

# DBTITLE 1,Write (full overwrite)
df_all.write.format("delta").mode("overwrite").saveAsTable(BRONZE_HOLIDAY_CALENDAR)

# COMMAND ----------

# DBTITLE 1,Checks
holiday_calendar_df = DeltaTable.forName(spark, BRONZE_HOLIDAY_CALENDAR).toDF()
holiday_calendar_df.printSchema()

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT code, count(*) AS totalHolidaysAcrossTheYears
        FROM {BRONZE_HOLIDAY_CALENDAR}
        GROUP BY code
    """)
)

# COMMAND ----------

display(
    holiday_calendar_df.distinct().filter(
        (col("code") == "TOR")
        & (year(col("date")) == 2025)
        & (month(col("date")) == 1)
    ).orderBy("date")
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT code, date
        FROM {BRONZE_HOLIDAY_CALENDAR}
        WHERE code = 'TOR' AND date BETWEEN '2024-12-31' AND '2025-02-01'
        ORDER BY date
    """)
)

# COMMAND ----------

display(
    holiday_calendar_df.filter(
        (col("code") == "TOR") & (col("date") == date(2025, 1, 4))
    )
)