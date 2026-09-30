# Databricks notebook source
# MAGIC %md
# MAGIC # JSON REST API (paged) -> Raw Volume ingestion template (Unity Catalog)
# MAGIC Pulls all pages from a paged REST endpoint and lands them as one dated JSON file
# MAGIC in the UC Volume `<catalog>.raw.data`.
# MAGIC
# MAGIC **UC change:** the ADLS SDK (`DataLakeServiceClient` + service principal) is replaced
# MAGIC by a direct write to the Volume path. Storage access is governed by the Volume grants.

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

import os
import json
import math
import copy
from datetime import datetime, timezone

import requests

# COMMAND ----------

# MAGIC %md
# MAGIC # Catalog context (must run before volume_util.py)

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG").strip()

if not CATALOG_NAME:
    raise ValueError("CATALOG widget is required (e.g. d4001-centralus-tdsci-cipcatalog).")

# volume_util.py computes VOLUME_BASE_PATH / EXTERNAL_LOCATION_BASE_PATH from this
# env var when it is %run, so it has to be set before the %run cell below.
os.environ["CATALOG_NAME"] = CATALOG_NAME
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC # Get configurations

# COMMAND ----------

# params
dbutils.widgets.text("SOURCE_REST_SERVICE_API_PATH", "/client-codes/pages")
dbutils.widgets.text(
    "SOURCE_REST_SERVICE_URL",
    "https://reference-data-asp.dev.client360.td.com/reference-data",
)
# Path is relative to the raw Volume (/Volumes/<catalog>/raw/data -> .../data/raw).
# The legacy ADLS form ("data/raw/...") is still accepted and normalised below,
# so existing ADF pipeline parameters keep working.
dbutils.widgets.text(
    "ADLS_DESTINATION_PATH",
    "reference_data/clients/c360_reference_data_service",
)
dbutils.widgets.text("ADB_AUTH_CLIENT_ID", "delta_lake_loader_client_id")
dbutils.widgets.text("ADB_AUTH_CLIENT_SECRET", "delta_lake_loader_client_secret")

SOURCE_REST_SERVICE_API_PATH = dbutils.widgets.get("SOURCE_REST_SERVICE_API_PATH")
SOURCE_REST_SERVICE_URL = dbutils.widgets.get("SOURCE_REST_SERVICE_URL")
ADLS_DESTINATION_PATH = dbutils.widgets.get("ADLS_DESTINATION_PATH")

# NOTE: behaviour unchanged from HMS version - these values are sent to the auth
# endpoint as-is. See review note on whether they should be secret lookups.
ADB_SECRET_CLIENT_ID_NAME = dbutils.widgets.get("ADB_AUTH_CLIENT_ID")
ADB_SECRET_CLIENT_SECRET_NAME = dbutils.widgets.get("ADB_AUTH_CLIENT_SECRET")

# env vars
AUTH_URL = os.getenv(
    "TDSCI_RIVENDELL_AUTH_URL",
    "https://auth-tor-dev.tds.td.com/auth/realms/Veritas/protocol/openid-connect/token",
)
AUTH_SCOPES = os.getenv("AUTH_SCOPES", "roles email audience vsroles")

SOURCE_REST_SERVICE_SSL_VERIFY = (
    os.getenv("SOURCE_REST_SERVICE_SSL_VERIFY", "false").lower() == "true"
)
SOURCE_REST_SERVICE_IS_STREAM = (
    os.getenv("SOURCE_REST_SERVICE_IS_STREAM", "false").lower() == "true"
)

SOURCE_REST_API_HEADERS = json.loads(os.getenv("SOURCE_REST_API_HEADERS", "{}"))
SOURCE_REST_API_QUERY_PARAMETERS = {}

# os.getenv returns a string when the variable is set - cast so paging maths works.
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "10000"))
FILE_NAME = os.getenv("FILE_NAME", "client-codes")

DATED_FILE_NAME = f"{FILE_NAME}-{datetime.now(timezone.utc).date().isoformat()}.json"

# COMMAND ----------

# MAGIC %md
# MAGIC # Resolve target Volume path

# COMMAND ----------

LEGACY_RAW_PREFIX = "data/raw"


def to_volume_relative_path(path: str) -> str:
    """Convert a legacy ADLS container path (data/raw/...) to a path relative to
    the raw Volume root. Paths already relative to the Volume are returned as-is."""
    p = (path or "").strip().strip("/")
    if p == LEGACY_RAW_PREFIX:
        return ""
    if p.startswith(LEGACY_RAW_PREFIX + "/"):
        return p[len(LEGACY_RAW_PREFIX) + 1:]
    return p


relative_destination = to_volume_relative_path(ADLS_DESTINATION_PATH)
target_dir = (
    f"{VOLUME_BASE_PATH}/{relative_destination}" if relative_destination else VOLUME_BASE_PATH
)
target_file = f"{target_dir}/{DATED_FILE_NAME}"

print(f"CATALOG_NAME      : {CATALOG_NAME}")
print(f"Target Volume file: {target_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Function to retrieve access token from auth url

# COMMAND ----------

@enhanced_errors()
def get_oauth2_access_token():
    auth_response = requests.post(
        url=AUTH_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": ADB_SECRET_CLIENT_ID_NAME,
            "client_secret": ADB_SECRET_CLIENT_SECRET_NAME,
            "scope": AUTH_SCOPES,
        },
        verify=False,
    )

    auth_response.raise_for_status()

    return auth_response.json()["access_token"]

# COMMAND ----------

# MAGIC %md
# MAGIC # Function to retrieve the data from the REST API

# COMMAND ----------

initial_request_body = {
    "draw": 1,
    "columns": [
        {
            "data": "id",
            "name": "id",
            "searchable": True,
            "orderable": True,
            "search": {"value": "", "regex": False},
        }
    ],
    "order": [{"column": 0, "dir": "asc"}],
    "start": 0,
    "length": PAGE_SIZE,
    "search": {"value": "", "regex": False},
}

# COMMAND ----------

@enhanced_errors()
def get_raw_paged_data(
    url: str,
    body: dict,
    stream: bool = False,
    verify_ssl: bool = False,
    headers: dict = None,
    page_size: int = 10,
):
    # Work on copies so the module-level body/headers are never mutated.
    request_body = copy.deepcopy(body)
    request_headers = dict(headers or {})
    request_headers["Authorization"] = f"Bearer {get_oauth2_access_token()}"

    with requests.Session() as session:
        first_page_response = session.post(
            url=url,
            json=request_body,
            headers=request_headers,
            verify=verify_ssl,
            stream=stream,
        )
        # Check HTTP status before parsing, so an error page is not reported as a JSON error.
        first_page_response.raise_for_status()
        first_page = first_page_response.json()

        data = [first_page]

        total_elements = int(first_page["recordsTotal"])
        num_pages = max(1, math.ceil(total_elements / page_size))

        for page_num in range(1, num_pages):
            request_body["start"] = page_num * page_size
            request_body["draw"] = 1 + page_num

            response = session.post(
                url=url,
                json=request_body,
                headers=request_headers,
                verify=verify_ssl,
                stream=stream,
            )

            response.raise_for_status()
            data.append(response.json())

    return data, total_elements

# COMMAND ----------

# MAGIC %md
# MAGIC # Run data download

# COMMAND ----------

raw_data, records_total = get_raw_paged_data(
    url=f"{SOURCE_REST_SERVICE_URL}{SOURCE_REST_SERVICE_API_PATH}",
    body=initial_request_body,
    stream=SOURCE_REST_SERVICE_IS_STREAM,
    verify_ssl=SOURCE_REST_SERVICE_SSL_VERIFY,
    headers=SOURCE_REST_API_HEADERS,
    page_size=PAGE_SIZE,
)

# COMMAND ----------

# Extract full list of records from pages
listed_raw_data = [item for page in raw_data for item in page["data"]]

print(f"Pages fetched   : {len(raw_data)}")
print(f"Records fetched : {len(listed_raw_data)} (API recordsTotal: {records_total})")

if len(listed_raw_data) != records_total:
    print(
        "WARNING: fetched record count does not match recordsTotal - "
        "source data may have changed during paging."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Write results to the raw Volume

# COMMAND ----------

with capture_errors("rest_api_raw_volume_write"):
    dbutils.fs.mkdirs(target_dir)

    # Volumes are FUSE-mounted, so standard Python file I/O writes to ADLS
    # under the governance of the Volume grants (no SDK / SP credentials needed).
    with open(target_file, "w", encoding="utf-8") as f:
        json.dump(listed_raw_data, f)

    external_file_path = target_file.replace(VOLUME_BASE_PATH, EXTERNAL_LOCATION_BASE_PATH, 1)

    print(f"Written (Volume) : {target_file}")
    print(f"Written (ADLS)   : {external_file_path}")

# COMMAND ----------

# Return landing details to the caller (ADF runOutput / dbutils.notebook.run).
dbutils.notebook.exit(json.dumps({
    "volume_path": target_file,
    "adls_path": external_file_path,
    "records": len(listed_raw_data),
}))

# COMMAND ----------

# End of Notebook