# Databricks notebook source
import os

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

ADB_TENANT_ID = os.getenv('ADB_TENANT_ID', '0ecb99ea-ca1a-4be6-96cc-ceb57b7b63d4')
ADB_SP_APP_ID = os.getenv('ADB_SP_APP_ID', '')
ADB_SP_PWD = os.getenv('ADB_SP_APP_PWD_NAME', '')
ADLS_ACCOUNT_URI = os.getenv('ADLS_ACCOUNT_URI', '')
ADLS_CONTAINER_NAME = os.getenv('ADLS_CONTAINER_NAME', '')
ADLS_ACCOUNT_NAME = os.getenv('ADLS_ACCOUNT_NAME', '')

# COMMAND ----------

def get_latest_file_from_dir(dir_path):
    files = dbutils.fs.ls(dir_path)
    files.sort(key=lambda f: f.modificationTime, reverse=True)
    return files[0]


def create_adls_service_client():
    # Setup ADLS Client
    credential = ClientSecretCredential(
        tenant_id=ADB_TENANT_ID,
        client_id=dbutils.secrets.get("tdsci-base", ADB_SP_APP_ID),
        client_secret=dbutils.secrets.get("tdsci-base", ADB_SP_PWD)
    )

    return DataLakeServiceClient(
        account_url=ADLS_ACCOUNT_URI,
        credential=credential
    )

def create_blob_service_client():
    # Setup ADLS Client
    credential = ClientSecretCredential(
        tenant_id=ADB_TENANT_ID,
        client_id=dbutils.secrets.get("tdsci-base", ADB_SP_APP_ID),
        client_secret=dbutils.secrets.get("tdsci-base", ADB_SP_PWD)
    )

    return BlobServiceClient(account_url=f"https://{ADLS_ACCOUNT_NAME}.blob.core.windows.net", credential=credential)

def get_directory_client(directory):
    return create_adls_service_client().get_file_system_client(ADLS_CONTAINER_NAME).get_directory_client(directory=directory)

def get_adls_base_path():
    return f"abfss://{ADLS_CONTAINER_NAME}@{ADLS_ACCOUNT_URI}/data/"

def save_adls_data_to_local_path(adls_blob_path: str, save_path: str, os_file_write_type: str = 'wb'):
    # Get the blob client
    blob_client = create_blob_service_client().get_blob_client(container=ADLS_CONTAINER_NAME, blob=adls_blob_path)

    # Download the file content
    with open(save_path, os_file_write_type) as f:
        f.write(blob_client.download_blob().readall())

def copy_adls_data_to_path(source: str, target: str, overwrite: bool = True):
    datalake_service_client = create_adls_service_client()

    file_system_client = datalake_service_client.get_file_system_client(file_system=ADLS_CONTAINER_NAME)

    # Get the source file client
    source_file_client = file_system_client.get_file_client(source)

    # Read the source file content
    source_file_data = source_file_client.download_file().readall()

    # Get the destination file client
    destination_file_client = file_system_client.get_file_client(target)

    # Upload the data to the destination file
    destination_file_client.upload_data(source_file_data, overwrite=overwrite)

# COMMAND ----------

import re
from typing import Optional, Dict, Any
from delta.tables import DeltaTable

def get_delta_min_versions(spark, table_path: str) -> Dict[str, Any]:
    """
    Inspect a Delta table path and return the minimum retained versions
    for normal Delta reads and Change Data Feed reads.

    Returns a dict like:
    {
      "path": ...,
      "is_delta_table": True,
      "earliest_json_version": 3585,
      "earliest_checkpoint_version": 3585,
      "min_snapshot_version": 3585,
      "min_cdf_version": 3585,
      "max_version": 5610,
      "cdf_enabled_now": True,
      "delta_log_file_count": 4088
    }
    """

    def _list_log_versions(path: str):
        log_path = f"{path.rstrip('/')}/_delta_log"
        files = dbutils.fs.ls(log_path)

        json_versions = []
        checkpoint_versions = []

        for f in files:
            name = f.name

            m_json = re.match(r"^(\d{20})\.json$", name)
            if m_json:
                json_versions.append(int(m_json.group(1)))
                continue

            m_ckpt = re.match(r"^(\d{20})\.checkpoint", name)
            if m_ckpt:
                checkpoint_versions.append(int(m_ckpt.group(1)))

        json_versions.sort()
        checkpoint_versions.sort()

        return {
            "files": files,
            "json_versions": json_versions,
            "checkpoint_versions": checkpoint_versions,
        }

    def _get_cdf_enabled_now(path: str) -> Optional[bool]:
        try:
            detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first().asDict(recursive=True)
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
                .load(path)
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
                .load(path)
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

    # basic validation
    try:
        is_delta = bool(DeltaTable.isDeltaTable(spark, table_path))
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
            "delta_log_file_count": len(log_info["files"]),
            "note": "No retained Delta JSON commit files found."
        }

    earliest_json = json_versions[0]
    earliest_checkpoint = checkpoint_versions[0] if checkpoint_versions else None
    max_version = json_versions[-1]

    # Best candidate lower bound for reconstructable snapshots
    candidate_min_snapshot = earliest_checkpoint if earliest_checkpoint is not None else earliest_json

    min_snapshot_version = _find_first_true(
        candidate_min_snapshot,
        max_version,
        lambda v: _can_read_snapshot(table_path, v)
    )

    cdf_enabled_now = _get_cdf_enabled_now(table_path)

    # If CDF is currently off, historical CDF may still exist, so we still probe.
    cdf_probe_low = min_snapshot_version if min_snapshot_version is not None else candidate_min_snapshot
    min_cdf_version = _find_first_true(
        cdf_probe_low,
        max_version,
        lambda v: _can_read_cdf(table_path, v)
    )

    return {
        "path": table_path,
        "is_delta_table": is_delta,
        "earliest_json_version": earliest_json,
        "earliest_checkpoint_version": earliest_checkpoint,
        "min_snapshot_version": min_snapshot_version,
        "min_cdf_version": min_cdf_version,
        "max_version": max_version,
        "cdf_enabled_now": cdf_enabled_now,
        "delta_log_file_count": len(log_info["files"]),
    }


# COMMAND ----------

import shutil
from pathlib import PurePosixPath

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG").strip()
if not CATALOG_NAME:
    raise ValueError("CATALOG parameter is required")
os.environ["CATALOG_NAME"] = CATALOG_NAME
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

def get_volume_base_path():
    base_path = VOLUME_BASE_PATH.strip().rstrip('/')
    if not base_path.startswith('/Volumes/') or len(base_path.split('/')) < 5:
        raise ValueError('volume_util.py must provide VOLUME_BASE_PATH under /Volumes/<catalog>/<schema>/<volume>')
    if '..' in PurePosixPath(base_path).parts:
        raise ValueError('VOLUME_BASE_PATH must not contain parent-directory components')
    return base_path + '/'

def get_volume_path(path: str) -> str:
    if path.startswith('dbfs:/Volumes/'):
        path = path[len('dbfs:'):]
    if '..' in PurePosixPath(path).parts:
        raise ValueError('Volume paths must not contain parent-directory components')
    if path.startswith('/Volumes/'):
        if len(path.split('/')) < 5:
            raise ValueError('A complete Volume path is required')
        return path
    if path.startswith('/') or '://' in path or path.startswith('dbfs:'):
        raise ValueError('Use a Volume path or a path relative to VOLUME_BASE_PATH')
    return get_volume_base_path() + path

def get_latest_file_from_dir_uc(dir_path):
    files = dbutils.fs.ls(get_volume_path(dir_path))
    files.sort(key=lambda f: f.modificationTime, reverse=True)
    return files[0]

def save_adls_data_to_local_path_uc(adls_blob_path: str, save_path: str, os_file_write_type: str = 'wb'):
    with open(get_volume_path(adls_blob_path), 'rb') as source:
        data = source.read()
    with open(save_path, os_file_write_type) as destination:
        destination.write(data)

def copy_adls_data_to_path_uc(source: str, target: str, overwrite: bool = True):
    with open(get_volume_path(source), 'rb') as source_file:
        with open(get_volume_path(target), 'wb' if overwrite else 'xb') as destination_file:
            shutil.copyfileobj(source_file, destination_file)

# COMMAND ----------

def get_delta_min_versions_uc(spark, table_path: str) -> Dict[str, Any]:

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
