# Databricks notebook source

# COMMAND ----------

# Unity Catalog environment selection.
dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG").strip()

if not CATALOG_NAME:
    raise ValueError("CATALOG parameter is required")

spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# Fully qualified UC managed Bronze table.
BRONZE_HOLIDAY_CALENDAR = (
    f"`{CATALOG_NAME}`.`bronze`.`holiday_calendar`"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Toronto holiday calendar

# COMMAND ----------

# The original Toronto calendar literal is folded in the supplied screenshot.
# Its individual records are not visible, so they cannot be reconstructed faithfully.
toronto_holiday_calendar = [
    # Original calendar records not visible in screenshot.
]

# COMMAND ----------

# MAGIC %md
# MAGIC # NYC Holiday Calendar

# COMMAND ----------

# The original NYC calendar literal is folded in the supplied screenshot.
# Its individual records are not visible, so they cannot be reconstructed faithfully.
nyc_holiday_calendar = [
    # Original calendar records not visible in screenshot.
]

# COMMAND ----------

# MAGIC %md
# MAGIC # LON Holiday Calendar

# COMMAND ----------

# The original London calendar literal is folded in the supplied screenshot.
# Its individual records are not visible, so they cannot be reconstructed faithfully.
london_holiday_calendar = [
    # Original calendar records not visible in screenshot.
]

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql.functions import lit, col, to_date, to_timestamp, struct, year, month, date_format
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DateType

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_HOLIDAY_CALENDAR} (
        date DATE,
        code STRING,
        currency STRING,
        audit STRUCT<
            userId: STRING,
            validFrom: LONG,
            timestamp: LONG,
            forceTimestamp: BOOLEAN,
            validFromDate: TIMESTAMP,
            timestampDate: TIMESTAMP
        >,
        source STRING
    )
    USING DELTA
""")

# COMMAND ----------

def individual_dates(calendar):
    calendar = calendar[0]
    holidays = sorted(calendar['dates'])

    copy_map = {
        'code': calendar['code'],
        'currency': calendar['currency'],
        'audit': calendar['audit'],
        'source': calendar['source']
    }

    return [{'date': holiday, **copy_map} for holiday in holidays]

# COMMAND ----------

toronto_holidays = individual_dates(toronto_holiday_calendar)
nyc_holidays = individual_dates(nyc_holiday_calendar)
lon_holidays = individual_dates(london_holiday_calendar)

# COMMAND ----------

bronze_schema = StructType([
    StructField('date', LongType(), False),
    StructField('code', StringType(), False),
    StructField('currency', StringType(), False),
    StructField('source', StringType(), True),
    StructField('audit', StructType([
        StructField('userId', StringType(), True),
        StructField('validFrom', LongType(), True),
        StructField('timestamp', LongType(), True),
        StructField('forceTimestamp', BooleanType(), True),
        StructField('validFromDate', StringType(), True),
        StructField('timestampDate', StringType(), True)
    ]))
])

# COMMAND ----------

df = spark.createDataFrame(toronto_holidays, schema=bronze_schema)
df1 = spark.createDataFrame(nyc_holidays, schema=bronze_schema)
df2 = spark.createDataFrame(lon_holidays, schema=bronze_schema)

df_all = df.union(df1).union(df2)

# COMMAND ----------

df_all = df_all.withColumn('dateASStr', to_date(col('date').cast('string'), 'yyyyMMdd'))
df_all = df_all.withColumn('validFromDateAsDate', to_timestamp(col('audit.validFromDate'), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
df_all = df_all.withColumn('timestampDateAsDate', to_timestamp(col('audit.timestampDate'), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
df_all = df_all.drop(col('date'))
df_all = df_all.withColumnRenamed('dateASStr', 'date')
df_all = df_all.withColumn(
    'audit',
    struct(
        col('audit.userId'),
        col('audit.validFrom'),
        col('audit.timestamp'),
        col('audit.forceTimestamp'),
        col('validFromDateAsDate').alias('validFromDate'),
        col('timestampDateAsDate').alias('timestampDate')
    )
)
df_all = df_all.drop(col('validFromDateAsDate'), col('timestampDateAsDate'))

# COMMAND ----------

(
    df_all.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(BRONZE_HOLIDAY_CALENDAR)
)

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, BRONZE_HOLIDAY_CALENDAR)
delta_table.toDF().printSchema()

# COMMAND ----------

df_all.printSchema()

# COMMAND ----------

spark.sql(f"""
    SELECT
        code,
        COUNT(*) AS totalHolidaysAcrossTheYears
    FROM {BRONZE_HOLIDAY_CALENDAR}
    GROUP BY code
""").show()

# COMMAND ----------

holiday_calendar_df = DeltaTable.forName(
    spark,
    BRONZE_HOLIDAY_CALENDAR
).toDF()

# Equivalent governed UC read:
# holiday_calendar_df = spark.table(BRONZE_HOLIDAY_CALENDAR)

# COMMAND ----------

from datetime import date

holiday_calendar_df.distinct().filter(
    (holiday_calendar_df.code == 'TOR')
    & (year(col('date')) == 2025)
    & (month(col('date')) == 1)
).orderBy('date').collect()

# COMMAND ----------

spark.sql(f"""
    SELECT code, date
    FROM {BRONZE_HOLIDAY_CALENDAR}
    WHERE code = 'TOR'
      AND date BETWEEN '2024-12-31' AND '2025-02-01'
    ORDER BY date
""").show()

# COMMAND ----------

holiday_calendar_df.filter(
    (holiday_calendar_df.code == 'tor'.upper())
    & (holiday_calendar_df.date == date(2025, 1, 4))
).collect()
