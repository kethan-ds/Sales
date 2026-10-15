# Databricks notebook source
import os
import shutil
from pathlib import PurePosixPath

# COMMAND ----------

# MAGIC %run "./delta_table_utils_uc.py"

# COMMAND ----------

dbutils.widgets.text('VOLUME_BASE_PATH', '')

def get_adls_base_path():
    base_path = dbutils.widgets.get('VOLUME_BASE_PATH').strip().rstrip('/')
    if not base_path.startswith('/Volumes/') or len(base_path.split('/')) < 5:
        raise ValueError('Set VOLUME_BASE_PATH to /Volumes/<catalog>/<schema>/<volume>[/directory]')
    if '..' in PurePosixPath(base_path).parts:
        raise ValueError('VOLUME_BASE_PATH must not contain parent-directory components')
    return base_path + '/'

def _volume_path(path: str) -> str:
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
    return get_adls_base_path() + path

def get_latest_file_from_dir(dir_path):
    files = dbutils.fs.ls(_volume_path(dir_path))
    files.sort(key=lambda f: f.modificationTime, reverse=True)
    return files[0]

def save_adls_data_to_local_path(adls_blob_path: str, save_path: str, os_file_write_type: str = 'wb'):
    with open(_volume_path(adls_blob_path), 'rb') as source:
        data = source.read()
    with open(save_path, os_file_write_type) as destination:
        destination.write(data)

def copy_adls_data_to_path(source: str, target: str, overwrite: bool = True):
    with open(_volume_path(source), 'rb') as source_file:
        with open(_volume_path(target), 'wb' if overwrite else 'xb') as destination_file:
            shutil.copyfileobj(source_file, destination_file)
