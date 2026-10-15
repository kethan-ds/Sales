# Databricks notebook source
import re
from contextlib import contextmanager
from typing import List, Tuple, Optional, Dict, Any

from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql import Row, Column
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import NullType, StringType, BinaryType, BooleanType
from pyspark.sql.types import DateType, TimestampType, TimestampNTZType, DayTimeIntervalType
from pyspark.sql.types import YearMonthIntervalType, CalendarIntervalType, ByteType, ShortType
from pyspark.sql.types import IntegerType, LongType, FloatType, DoubleType, DecimalType
from pyspark.sql.types import StructType, ArrayType, MapType

# COMMAND ----------

def get_row_changes(
    delta_table: DeltaTable,
    table_name: str,
    change_types: list[str],
    select_columns: list[str],
    version=1,
    with_post_image=False,
) -> List[Row]:
    latest_version_df = delta_table.history(version)
    latest_version = (
        latest_version_df.select("version")
        .orderBy("version", ascending=False)
        .first()["version"]
    )

    # get latest delta changes if any
    cdf = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", latest_version)
        .table(table_name)
    )

    if with_post_image:
        return (
            cdf.select("_change_type", *select_columns)
            .filter(cdf._change_type.like("%_postimage") | cdf._change_type.isin(change_types))
            .collect()
        )

    return (
        cdf.select("_change_type", *select_columns)
        .filter(cdf._change_type.isin(change_types))
        .collect()
    )


# COMMAND ----------

class NotebookExecutionException(Exception):
    pass

# COMMAND ----------

def _is_table_identifier(identifier: str) -> bool:
    # Unity Catalog: a governed table is referenced as catalog.schema.table (dotted, no
    # slash), whereas an ADLS/DBFS/Volume location contains a path separator.
    return "." in identifier and "/" not in identifier


def _read_delta_version(identifier: str, version: int) -> DataFrame:
    # Prefer reading via the governed Delta table name instead of a direct ADLS path,
    # since Unity Catalog does not grant direct storage access. Falls back to a path
    # read (e.g. a Unity Catalog Volume path) when a location is supplied.
    reader = spark.read.format("delta").option("versionAsOf", version)
    if _is_table_identifier(identifier):
        return reader.table(identifier)
    return reader.load(identifier)


def _read_delta_change_feed(
    identifier: str,
    starting_version: int,
    ending_version: int = None,
):
    # Read the change data feed via the governed table name when possible, otherwise
    # from a path/Volume location.
    reader = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", starting_version)
    )
    if ending_version is not None:
        reader = reader.option("endingVersion", ending_version)

    if _is_table_identifier(identifier):
        return reader.table(identifier)
    return reader.load(identifier)

# COMMAND ----------

@contextmanager
def perform_delta(path: str, other_df: DataFrame, **kwargs):
    kwargs["field_inserts"] = kwargs["field_setters"]

    with merge_in_transaction(spark, path=path, other_df=other_df, **kwargs) as dt:
        yield dt

# COMMAND ----------

def rollback_merge(delta_table: DeltaTable, path: str) -> None:
    first_record = (
        delta_table.history()
        .filter(F.lower(F.col("operation")) == F.lit("merge"))
        .select("version")
        .orderBy("version", ascending=False)
        .first()
    )

    if not first_record:
        raise Exception(f"Unable to obtain first row of history for delta table {path}")

    latest_version = first_record["version"]

    delta_table.restoreToVersion(latest_version - 1)

# COMMAND ----------

@contextmanager
def merge_in_transaction(
    spark_session: SparkSession,
    path: str,
    other_df: DataFrame,
    **kwargs,
):
    if _is_table_identifier(path):
        delta_table = DeltaTable.forName(spark_session, path)
    else:
        delta_table = DeltaTable.forPath(spark_session, path)

    if kwargs.get("delete_if_source_not_matched"):
        (
            delta_table.alias(kwargs["outer_alias_name"])
            .merge(
                other_df.alias(kwargs["inner_alias_name"]),
                kwargs["condition_for_matched"],
            )
            .whenMatchedUpdate(
                condition=kwargs.get("when_matched_update_condition", None),
                set=kwargs["field_setters"],
            )
            .whenNotMatchedInsert(values=kwargs["field_inserts"])
            # FIX: restored the condition kwarg. Without it, this deletes every
            # target row not present in other_df, even when the caller only meant
            # to prune a scoped window (e.g. "not matched AND tgt.fxDate >= X").
            .whenNotMatchedBySourceDelete(
                condition=kwargs.get("when_not_matched_by_source_delete_condition", None)
            )
            .execute()
        )
    else:
        (
            delta_table.alias(kwargs["outer_alias_name"])
            .merge(
                other_df.alias(kwargs["inner_alias_name"]),
                kwargs["condition_for_matched"],
            )
            .whenMatchedUpdate(
                condition=kwargs.get("when_matched_update_condition", None),
                set=kwargs["field_setters"],
            )
            .whenNotMatchedInsert(values=kwargs["field_inserts"])
            .execute()
        )

    try:
        yield delta_table
    except NotebookExecutionException as nex:
        rollback_merge(delta_table, path)
        raise nex
    except Exception as ex:
        if kwargs.get("roll_back_on_hard_error"):
            rollback_merge(delta_table, path)
        raise ex

# COMMAND ----------

def changed_detected(
    delta_table: DeltaTable,
    delta_table_path: str,
    join_columns: list[str],
    select_columns: list[Column],
    change_condition: Column,
    distinct_column: str,
    # FIX: this parameter was dropped from the signature while the body still
    # referenced `version`, causing a NameError on every call. Restored with the
    # same default (1) that get_row_changes uses.
    version: int = 1,
) -> list[Row]:
    try:
        latest_version_df = delta_table.history(version)
        latest_version = (
            latest_version_df.select("version")
            .orderBy("version", ascending=False)
            .first()["version"]
        )

        # Unity Catalog: read time-travel versions via the governed table name when
        # delta_table_path is a catalog.schema.table identifier, otherwise by path/Volume.
        before_merge_df = _read_delta_version(delta_table_path, latest_version - 1)
        after_merge_df = _read_delta_version(delta_table_path, latest_version)

        changes_df = (
            before_merge_df.alias("before")
            .join(after_merge_df.alias("after"), on=join_columns, how="inner")
            .select(*select_columns)
        )

        return (
            changes_df.filter(change_condition)
            .select(distinct_column)
            .distinct()
            .collect()
        )
    except Exception as ex:
        raise NotebookExecutionException(ex)

# COMMAND ----------

# SQL type mapping for Spark data types
_TYPE_MAP = {
    # Primitive types
    NullType: "NULL",
    StringType: "STRING",
    BinaryType: "BINARY",
    BooleanType: "BOOLEAN",
    DateType: "DATE",
    TimestampType: "TIMESTAMP",
    TimestampNTZType: "TIMESTAMP_NTZ",
    DayTimeIntervalType: "INTERVAL DAY TO SECOND",
    YearMonthIntervalType: "INTERVAL YEAR TO MONTH",
    CalendarIntervalType: "INTERVAL",
    # Numeric types
    ByteType: "TINYINT",
    ShortType: "SMALLINT",
    IntegerType: "INT",
    LongType: "BIGINT",
    FloatType: "FLOAT",
    DoubleType: "DOUBLE",
    # Decimal with dynamic precision/scale
    DecimalType: lambda dt: f"DECIMAL({dt.precision},{dt.scale})",
}

# COMMAND ----------

def _spark_type_to_sql(data_type):
    # Convert Spark DataType to SQL compatible string for Table creation

    for spark_type, sql_type in _TYPE_MAP.items():
        if isinstance(data_type, spark_type):
            return sql_type(data_type) if callable(sql_type) else sql_type

    if isinstance(data_type, ArrayType):
        return f"ARRAY<{_spark_type_to_sql(data_type.elementType)}>"
    if isinstance(data_type, MapType):
        return (
            f"MAP<{_spark_type_to_sql(data_type.keyType)}, "
            f"{_spark_type_to_sql(data_type.valueType)}>"
        )
    if isinstance(data_type, StructType):
        fields = ", ".join(
            f"{field.name}: {_spark_type_to_sql(field.dataType)}"
            for field in data_type.fields
        )
        return f"STRUCT<{fields}>"

    # Fallback
    raise TypeError(f"Unsupported Spark type: {type(data_type).__name__}")


special_characters = re.compile(r"[!@#$%^&*()<>?/\\|{}~:]")


def _escape_special_characters(name):
    if special_characters.search(name) is None:
        return name
    else:
        return f"`{name}`"


# COMMAND ----------

# Creates Medalion table given a StructType Data Model
# catalog: Unity Catalog name (e.g. tdsci_dev). If None, falls back to the
#   Hive-Metastore-style two-level `layer.table_name` naming used before UC.
# layer: (bronze, silver, gold) -- used as the UC schema name
# table_name: what you want to name the table
# schema: StructType
# location: optional external/Volume location for the table (UC-managed tables
#   generally don't need this; pass it only for path-backed tables)
# enableChangeDataFeed: (True, False)
def create_delta_table(
    layer: str,
    table_name: str,
    schema,
    enableChangeDataFeed: bool,
    catalog: str = None,
    location: str = None,
):
    full_table_name = (
        f"{catalog}.{layer}.{table_name.lower()}"
        if catalog
        else f"{layer}.{table_name.lower()}"
    )

    # Generate DDL from schema with full type support
    columns_ddl = ",\n".join(
        [
            f"{_escape_special_characters(field.name)} {_spark_type_to_sql(field.dataType)}"
            for field in schema.fields
        ]
    )

    location_clause = f"\n        LOCATION '{location}'" if location else ""

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {columns_ddl}
        )
        USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = {enableChangeDataFeed}){location_clause}
        """
    )

# COMMAND ----------

def get_latest_change_pivot(
    cols: List[str],
    *,
    spark_session: SparkSession,
    delta_location: str,
    filter_cond: str,
) -> Tuple[DataFrame, DataFrame]:
    # Unity Catalog: accepts either a catalog.schema.table identifier or a
    # path/Volume location, same dual-mode convention as merge_in_transaction.
    if "." in delta_location and "/" not in delta_location:
        dt = DeltaTable.forName(spark_session, delta_location)
    else:
        dt = DeltaTable.forPath(spark_session, delta_location)

    latest_version = dt.history(1).select("version").collect()[0].version

    changed_df = _read_delta_change_feed(
        delta_location,
        starting_version=latest_version,
        ending_version=latest_version,
    ).filter(filter_cond)

    columns = [c for c in changed_df.columns if not c.startswith("_")]

    pivot_df = (
        changed_df.groupBy("_commit_version", *cols)
        .pivot(
            "_change_type",
            ["insert", "update_preimage", "update_postimage", "delete"],
        )
        .agg(F.first(F.struct(*columns)))
    )

    return changed_df, pivot_df


# COMMAND ----------

def get_delta_min_versions(spark, table_path: str) -> Dict[str, Any]:

    def _list_log_versions(path: str):
        versions = [row['version'] for row in DeltaTable.forName(spark, path).history().select('version').collect()]
        versions.sort()
        return {'files': None, 'json_versions': versions, 'checkpoint_versions': []}

    def _get_cdf_enabled_now(path: str) -> Optional[bool]:
        try:
            detail = DeltaTable.forName(spark, path).detail().first().asDict(recursive=True)
            props = detail.get("properties") or {}
            val = props.get("delta.enableChangeDataFeed")

            if val is None:
                return False

            return str(val).strip().lower() == "true"
        except Exception:
            return None

    def _can_read_snapshot(path: str, version: int) -> bool:
        try:
            df = (
                spark.read.format("delta")
                .option("versionAsOf", version)
                .table(path)
            )
            df.limit(1).count()

            return True
        except Exception:
            return False

    def _can_read_cdf(path: str, version: int) -> bool:
        try:
            df = (
                spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", version)
                .option("endingVersion", version)
                .table(path)
            )
            df.limit(1).count()

            return True
        except Exception:
            return False

    def _find_first_true(low: int, high: int, predicate) -> Optional[int]:
        if low > high:
            return None

        if not predicate(high):
            return None

        ans = None
        left, right = low, high

        while left <= right:
            mid = (left + right) // 2

            if predicate(mid):
                ans = mid
                right = mid - 1
            else:
                left = mid + 1
        return ans


    try:
        is_delta = str(DeltaTable.forName(spark, table_path).detail().first()['format']).lower() == 'delta'
    except Exception:
        is_delta = None

    log_info = _list_log_versions(table_path)
    json_versions = log_info["json_versions"]
    checkpoint_versions = log_info["checkpoint_versions"]

    if not json_versions:
        return {
            "path": table_path,
            "is_delta_table": is_delta,
            "earliest_json_version": None,
            "earliest_checkpoint_version": None,
            "min_snapshot_version": None,
            "min_cdf_version": None,
            "max_version": None,
            "cdf_enabled_now": _get_cdf_enabled_now(table_path),
            "delta_log_file_count": None,
            "note": "No retained table history versions found."
        }

    earliest_json = json_versions[0]
    earliest_checkpoint = checkpoint_versions[0] if checkpoint_versions else None
    max_version = json_versions[-1]


    candidate_min_snapshot = earliest_checkpoint if earliest_checkpoint is not None else earliest_json

    min_snapshot_version = _find_first_true(
        candidate_min_snapshot,
        max_version,
        lambda v: _can_read_snapshot(table_path, v)
    )

    cdf_enabled_now = _get_cdf_enabled_now(table_path)


    cdf_probe_low = min_snapshot_version if min_snapshot_version is not None else candidate_min_snapshot
    min_cdf_version = _find_first_true(
        cdf_probe_low,
        max_version,
        lambda v: _can_read_cdf(table_path, v)
    )

    return {
        "path": table_path,
        "is_delta_table": is_delta,
        "earliest_json_version": None,
        "earliest_checkpoint_version": None,
        "min_snapshot_version": min_snapshot_version,
        "min_cdf_version": min_cdf_version,
        "max_version": max_version,
        "earliest_history_version": earliest_json,
        "cdf_enabled_now": cdf_enabled_now,
        "delta_log_file_count": None,
    }

# COMMAND ----------

def get_last_merge_metrics(spark_session: SparkSession, target_table_path: str, after_version: Optional[int] = None):
    hist = DeltaTable.forName(spark_session, target_table_path).history()

    cond = (F.col("operation") == F.lit("MERGE"))

    if after_version is not None:
        cond = cond & (F.col("version") > F.lit(after_version))

    rows = (
        hist.where(cond)
            .orderBy(F.col("version").desc())
            .limit(1)
            .collect()
    )

    if not rows:
        return {
            "status": "no_merge_found",
            "table": target_table_path,
            "after_version": after_version
        }

    row = rows[0]

    return {
        "status": "ok",
        "table": target_table_path,
        "version": row["version"],
        "timestamp": str(row["timestamp"]),
        "operation": row["operation"],
        "metrics": row["operationMetrics"] or {}
    }
