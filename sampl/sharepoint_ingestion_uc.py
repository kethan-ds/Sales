# Databricks notebook source
import requests
import json
import logging
import sys
from datetime import datetime, timedelta, date, timezone
import re
import os
import io
import zipfile
from pyspark.sql.types import *

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
    force=True,
)

# Create a logger instance
logger = logging.getLogger(__name__)

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG").strip()

if not CATALOG_NAME:
    raise ValueError("CATALOG parameter is required")

# volume_util.py resolves CATALOG_NAME/VOLUME_BASE_PATH from the environment.
# Set the job-supplied catalog before loading the shared utility so the Volume
# path is built for the same DEV/PAT/PROD catalog selected by this notebook.
os.environ["CATALOG_NAME"] = CATALOG_NAME

spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

dbutils.widgets.text("sharepoint_host_name", "")
dbutils.widgets.text("sharepoint_site_path", "")
dbutils.widgets.text("sharepoint_drive_id", "")

dbutils.widgets.text("sharepoint_client_id", "")
dbutils.widgets.text("sharepoint_client_secret", "")

# COMMAND ----------

SHAREPOINT_HOST_NAME     = dbutils.widgets.get("sharepoint_host_name").strip()
SHAREPOINT_SITE_PATH     = dbutils.widgets.get("sharepoint_site_path").strip()
SHAREPOINT_DRIVE_ID      = dbutils.widgets.get("sharepoint_drive_id").strip()

SHAREPOINT_CLIENT_ID     = dbutils.widgets.get("sharepoint_client_id")
SHAREPOINT_CLIENT_SECRET = dbutils.widgets.get("sharepoint_client_secret")

ADB_TENANT_ID = os.getenv("ADB_TENANT_ID", "").strip()
if not ADB_TENANT_ID:
    raise ValueError("ADB_TENANT_ID environment variable is required")

SHAREPOINT_BASE_FOLDER_PATH = 'C360 Datasets/sharepoint_ingestion'
TRACKER_TABLE = f"`{CATALOG_NAME}`.`raw`.`sharepoint_ingestion_tracker`"
SCOPE = "https://graph.microsoft.com/.default"
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
TOKEN_URL = f"https://login.microsoftonline.com/{ADB_TENANT_ID}/oauth2/v2.0/token"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TRACKER_TABLE} (
        product          STRING,
        file_name        STRING,
        type             STRING,
        trade_date       DATE,
        upload_timestamp TIMESTAMP,
        is_done_raw      BOOLEAN DEFAULT false,
        is_done_bronze   BOOLEAN DEFAULT false
    )
    USING DELTA
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
""")

# COMMAND ----------

class GraphTokenManager:
    """Manages a Microsoft Graph access token, refreshing it only when close to expiry."""
    def __init__(self, client_id: str, client_secret: str, buffer_seconds: int = 600):
        self._client_id = client_id
        self._client_secret = client_secret
        self._buffer_seconds = buffer_seconds
        self._token = None
        self._expiry = None

    def get_token(self) -> str:
        """Return a valid token, refreshing only if under buffer_seconds remain."""
        if self._token is None:
            logger.info("Fetching first access Token")
            self._fetch_token()
            return self._token

        remaining = (self._expiry - datetime.now(timezone.utc)).total_seconds()

        if remaining <= self._buffer_seconds:
            minutes, seconds = divmod(int(remaining), 60)
            logger.info(f"Token expiring soon ({minutes}m {seconds}s left) - refreshing")
            self._fetch_token()

        return self._token

    def _fetch_token(self):
        payload = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "scope": SCOPE,
        }

        try:
            response = requests.post(TOKEN_URL, data=payload, timeout=300)
            response.raise_for_status()
            token_data = response.json()
            token = token_data["access_token"]
            expires_in = token_data["expires_in"]
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else "N/A"
            logger.exception(f"HTTP error {status_code} fetching token from {TOKEN_URL}: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.exception(f"Connection error fetching token from {TOKEN_URL}: {e}")
            raise
        except requests.exceptions.Timeout:
            logger.exception(f"Request timed out after 300s fetching token from {TOKEN_URL}")
            raise
        except ValueError as e:
            logger.exception(f"Invalid JSON in token response from {TOKEN_URL}: {e}")
            raise

        if not token:
            raise ValueError(f"Access token missing in response from {TOKEN_URL}")

        self._token = token
        self._expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        expiry_minutes, expiry_seconds = divmod(expires_in, 60)
        logger.info(
            f"Token refreshed, expires at {self._expiry.strftime('%Y-%m-%d %H:%M:%S %Z')} "
            f"(in {expiry_minutes}m {expiry_seconds}s)"
        )

# COMMAND ----------

def graph_get(token_manager: GraphTokenManager, url: str, timeout: int = 600, stream: bool = False):
    headers = {"Authorization": f"Bearer {token_manager.get_token()}"}
    try:
        response = requests.get(url, headers=headers, timeout=timeout, stream=stream)
        response.raise_for_status()
        return response if stream else response.json()
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response else "N/A"
        logging.error(f"HTTP error {status_code} for {url}: {e}")
        raise
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Connection error for {url}: {e}")
        raise
    except requests.exceptions.Timeout:
        logging.error(f"Request timed out after {timeout}s for {url}")
        raise
    except ValueError as e:
        logging.error(f"Invalid JSON in response from {url}: {e}")
        raise

# COMMAND ----------

def graph_post(token_manager: GraphTokenManager, url: str, data=None, json=None, timeout: int = 900, stream: bool = False):
    headers = {"Authorization": f"Bearer {token_manager.get_token()}"}
    try:
        response = requests.post(
            url,
            headers=headers,
            data=data,
            json=json,
            timeout=timeout,
            stream=stream,
        )
        response.raise_for_status()
        return response if stream else response.json()
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response else "N/A"
        logging.error(f"HTTP error {status_code} for {url}: {e}")
        raise
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Connection error for {url}: {e}")
        raise
    except requests.exceptions.Timeout:
        logging.error(f"Request timed out after {timeout}s for {url}")
        raise
    except ValueError as e:
        logging.error(f"Invalid JSON in response from {url}: {e}")
        raise

# COMMAND ----------

def graph_delete(token_manager: GraphTokenManager, url: str, timeout: int = 900) -> None:
    headers = {"Authorization": f"Bearer {token_manager.get_token()}"}
    try:
        response = requests.delete(url, headers=headers, timeout=timeout)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response else "N/A"
        logging.error(f"HTTP error {status_code} for {url}: {e}")
        raise
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Connection error for {url}: {e}")
        raise
    except requests.exceptions.Timeout:
        logging.error(f"Request timed out after {timeout}s for {url}")
        raise

# COMMAND ----------

def resolve_site_id(token_manager: GraphTokenManager, host_name: str = SHAREPOINT_HOST_NAME, site_path: str = SHAREPOINT_SITE_PATH) -> str:
    url = f"{GRAPH_BASE_URL}/sites/{host_name}:/{site_path}"
    site = graph_get(token_manager, url)
    site_id = site["id"]

    if not site_id:
        raise ValueError(f"Could not resolve site_id for {host_name}/{site_path}")

    logging.info(f"Found Site ID for {url}")
    return site_id

# COMMAND ----------

def list_drives(token_manager: GraphTokenManager, site_id: str) -> list:
    """List all document libraries (drives) in a SharePoint site."""
    url = f"{GRAPH_BASE_URL}/sites/{site_id}/drives"
    return graph_get(token_manager, url).get("value", [])

# COMMAND ----------

def list_children(token_manager: GraphTokenManager, drive_id: str, base_folder_path: str = None, product_folder_id: str = None) -> list:
    """List all children in a Drive root or Product Folder"""
    if base_folder_path: # List childs (Files) in Product Folder (eg. td_cowen.csv)
        url = f"{GRAPH_BASE_URL}/drives/{drive_id}/root:/{base_folder_path}:/children"
    elif product_folder_id: # List childs (Product Folder) in (C360 Datasets/sharepoint_ingestion/products)
        url = f"{GRAPH_BASE_URL}/drives/{drive_id}/items/{product_folder_id}/children"
    else:
        error = "Missing necessary fields base_folder_path or item_id (1 Required)"
        logger.error(error)
        raise ValueError(error)

    list_of_children = []
    while url:
        data = graph_get(token_manager, url)
        list_of_children.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    return list_of_children

# COMMAND ----------

DATE_PATTERN = re.compile(
    r"(?P<year>(?:19|20)\d{2})"
    r"(?:[-_]?(?P<month>0[1-9]|1[0-2]))?"
    r"(?:[-_]?(?P<day>0[1-9]|[12]\d|3[01]))?"
)

def extract_date_parts(file_name: str) -> dict:
    """
    Extract year/month/day from a filename.
    year - Mandatory
    month - Madatory
    day - Optional
    """
    for match in DATE_PATTERN.finditer(file_name):
        if match.group("month"):
            return match.groupdict()

    raise ValueError(f"No date with at least year and month found in filename: '{file_name}'")

# COMMAND ----------

FISCAL_YEAR_PATTERN = re.compile(
    r"fiscal[\s_-]*(?P<fiscal_year>(?:19|20)\d{2})",
    re.IGNORECASE,
)

FISCAL_YEAR_START_MONTH = 11
FISCAL_YEAR_START_DAY = 1

def extract_fiscal_year_start(file_name: str) -> dict | None:
    """
    Extract the fiscal year from a filename containing 'fiscal' (case-insensitive)
    and return the calendar START date of that fiscal year as year/month/day.
    Returns None if the filename has no fiscal year.

    Example: fiscal 2026 starts on 2025-11-01 -> {'year': '2025', 'month': '11', 'day': '01'}
    """
    match = FISCAL_YEAR_PATTERN.search(file_name)
    if not match:
        return None

    start_year = int(match.group("fiscal_year")) - 1

    return {
        "year": str(start_year),
        "month": f"{FISCAL_YEAR_START_MONTH:02d}",
        "day": f"{FISCAL_YEAR_START_DAY:02d}",
    }

# COMMAND ----------

# Matches 'account' or 'accounts', case insensitive, with spaces and underscores before and/or acting as seperators
ACCOUNT_PATTERN = re.compile(r"(?<![A-Za-z])accounts?(?![A-Za-z])", re.IGNORECASE)

def classify_file_type(file_name: str) -> str:
    """Return 'account' if the filename contains 'account'/'accounts', else 'trade'."""
    return "account" if ACCOUNT_PATTERN.search(file_name) else "trade"

# COMMAND ----------

def read_from_sharepoint(token_manager: GraphTokenManager, download_url: str, timeout: int = 900) -> bytes:
    """Download a file's raw bytes from SharePoint via its pre-authenticated download URL."""
    response = graph_get(token_manager, download_url, timeout=timeout, stream=True)
    return response.content

# COMMAND ----------

def delete_sharepoint_file(token_manager: GraphTokenManager, drive_id: str, file_id: str) -> None:
    """Delete an item (file) from a SharePoint drive after successful UC Volume upload."""
    url = f"{GRAPH_BASE_URL}/drives/{drive_id}/items/{file_id}"
    graph_delete(token_manager, url)

# COMMAND ----------

def upload_file_to_volume(target_directory: str, file_name: str, data: bytes) -> str:
    """Write binary data directly to a Unity Catalog Volume path."""
    dbutils.fs.mkdirs(target_directory)
    target_path = f"{target_directory.rstrip('/')}/{file_name}"

    # /Volumes is available through FUSE, so binary formats such as XLSX and
    # Parquet can be written without using an ADLS storage client.
    with open(target_path, "wb") as f:
        f.write(data)

    return target_path

# COMMAND ----------

# Prefixes that mark temp/lock/metadata files
TEMP_PREFIXES = ("~$", "._", ".~lock.")

# Extensions/suffixes for temp, swap, backup, and partial-download files
TEMP_SUFFIXES = (
    ".tmp", ".temp", ".swp", ".swo", ".swx",
    ".bak", ".old", ".part", ".partial", ".crdownload",
    "~",
)

# Exact system filenames (compared case-insensitively)
TEMP_EXACT_NAMES = {
    "thumbs.db", ".ds_store", "desktop.ini",
    ".spotlight-v100", ".trashes",
}

def is_temp_file(file_name: str) -> bool:
    """Return True if file_name is a temporary, hidden, or system file."""
    name = file_name.strip()
    lowered = name.lower()
    return (
        lowered in TEMP_EXACT_NAMES
        or lowered.startswith(TEMP_PREFIXES)
        or lowered.endswith(TEMP_SUFFIXES)
        or name.startswith(".")
    )

# COMMAND ----------

def is_zip_file(file_name: str, file_data: bytes) -> bool:
    """True if the file looks like a zip (by extension or magic bytes)."""
    if file_name.lower().endswith(".zip"):
        return True
    return file_data[:4] == b"PK\x03\x04" # Checks if it's a Zip without the extension

def extract_zip(file_data: bytes) -> list[tuple[str, bytes]]:
    """Unzip in-memory zip bytes -> list of (name, bytes). Skips dirs and temp files."""
    extracted = []
    try:
        with zipfile.ZipFile(io.BytesIO(file_data)) as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                extracted_name = os.path.basename(info.filename)
                if not extracted_name or is_temp_file(extracted_name):
                    continue
                with zf.open(info) as f:
                    extracted.append((extracted_name, f.read()))
    except zipfile.BadZipFile as e:
        logger.exception(f"Corrupt or invalid zip: {e}")
        raise
    return extracted

# COMMAND ----------

def insert_tracker_record(
    product: str,
    file_name: str,
    file_type: str,
    trade_date: date,
    upload_timestamp: datetime,
) -> None:
    """Insert a record into the tracker table with is_done_raw/is_done_bronze set to False."""
    schema = StructType([
        StructField("product",          StringType(),    False),
        StructField("file_name",        StringType(),    False),
        StructField("type",             StringType(),    False),
        StructField("trade_date",       DateType(),      False),
        StructField("upload_timestamp", TimestampType(), False),
        StructField("is_done_raw",      BooleanType(),   False),
        StructField("is_done_bronze",   BooleanType(),   False),
    ])

    record = [(product, file_name, file_type, trade_date, upload_timestamp, False, False)]

    df = spark.createDataFrame(record, schema)
    df.write.format("delta").mode("append").saveAsTable(TRACKER_TABLE)

    logger.info(
        f"Inserted tracker record for '{file_name}' "
        f"(trade_date={trade_date}, upload_timestamp={upload_timestamp})"
    )

# COMMAND ----------

SUPPORTED_DATA_EXTENSIONS = {
    ".csv",
    ".xlsx", ".xls", ".xlsm",
    ".parquet",
    ".json"
}

def is_supported_data_file(file_name: str) -> bool:
    """True if the extension is a data format we ingest (case-insensitive)."""
    _, ext = os.path.splitext(file_name)
    return ext.lower() in SUPPORTED_DATA_EXTENSIONS

# COMMAND ----------

def classify_file(file_name: str, file_data: bytes) -> tuple[str, str]:
    """
    Validate a single file (standalone SharePoint file OR a zip member).

    Returns (status, reason):
      - "ok"      -> process this file
      - otherwise -> status is "invalid" and reason explains why
    """
    if is_temp_file(file_name):
        return "invalid", "temp/hidden file"
    if is_zip_file(file_name, file_data):
        return "invalid", "nested zip"
    if not is_supported_data_file(file_name):
        return "invalid", "unsupported file type"
    return "ok", ""

# COMMAND ----------

token_manager = GraphTokenManager(SHAREPOINT_CLIENT_ID, SHAREPOINT_CLIENT_SECRET)
site_id = resolve_site_id(token_manager, SHAREPOINT_HOST_NAME, SHAREPOINT_SITE_PATH)

# Find our drive
drives = list_drives(token_manager, site_id)
drive_match = [d for d in drives if d["id"] == SHAREPOINT_DRIVE_ID]
if not drive_match:
    raise ValueError(f"Drive '{SHAREPOINT_DRIVE_ID}' not found in site {site_id}")

drive = drive_match[0]
drive_id, drive_name = drive["id"], drive["name"]

# List Product folders
product_folders = list_children(token_manager, drive_id, base_folder_path=SHAREPOINT_BASE_FOLDER_PATH)

for product in product_folders:
    product_id, product_name = product["id"], product["name"]
    product_files = list_children(token_manager, drive_id, product_folder_id=product_id)

    for sp_file in product_files:
        file_id = sp_file["id"]
        file_name = sp_file["name"]

        try:
            # ---- Read the sharepoint  ----
            web_url   = sp_file["webUrl"]
            file_size = sp_file.get("size", "unknown")
            file_url  = sp_file["@microsoft.graph.downloadUrl"]
            logger.info(f"Reading from SharePoint: '{file_name}' (size: {file_size} bytes) from\n{web_url}")
            source_data = read_from_sharepoint(token_manager, file_url)
            logger.info(f"Read {len(source_data)} bytes for '{file_name}'")

            # ---- Normalize into a list of (name, bytes) ----
            is_zip = is_zip_file(file_name, source_data)
            if is_zip:
                logger.info(f"Extracting zip '{file_name}'")
                files_to_process = extract_zip(source_data)
                logger.info(f"  -> {len(files_to_process)} file(s) inside '{file_name}'")
            else:
                files_to_process = [(file_name, source_data)]

            today = datetime.now()
            today_path = today.strftime("%Y/%m/%d")
            volume_directory = f"{VOLUME_BASE_PATH}/sharepoint_ingestion/{product_name.lower()}/{today_path}"

            prepared = []          # (name, data, file_type, trade_date)
            source_ok = True

            for current_file_name, current_file_data in files_to_process:
                where = f"inside '{file_name}'" if is_zip else "standalone file"
                status, reason = classify_file(current_file_name, current_file_data)

                # Anything that isn't "ok" -> abort the whole source
                if status != "ok":
                    logger.error(f"{reason.capitalize()} '{current_file_name}' ({where}) - aborting entire source file")
                    source_ok = False
                    break

                if product_name in ['prism_product_mappings']:
                    trade_date = datetime.strptime(sp_file["createdDateTime"], "%Y-%m-%dT%H:%M:%SZ").date()
                    file_type = "product_mapping"
                    name, ext = os.path.splitext(current_file_name)
                    prepared.append((f"{name}_{sp_file['createdDateTime']}{ext}", current_file_data, file_type, trade_date))
                else:
                    try:
                        fiscal_start = extract_fiscal_year_start(current_file_name)
                        date_in_file_name = fiscal_start if fiscal_start else extract_date_parts(current_file_name)
                    except ValueError as e:
                        logger.exception(f"Could not extract date from '{current_file_name}' ({where}) - aborting entire source: {e}")
                        source_ok = False
                        break

                    file_type = classify_file_type(current_file_name)
                    trade_date = date(
                        int(date_in_file_name["year"]),
                        int(date_in_file_name["month"]),
                        int(date_in_file_name.get("day") or 1),
                    )
                    prepared.append((current_file_name, current_file_data, file_type, trade_date))

            if not source_ok:
                logger.warning(f"Not processing or deleting '{file_name}' - one or more files failed validation")
                continue

            if not prepared:
                logger.warning(f"No data files to process in '{file_name}' - keeping source")
                continue

            # Upload to UC Volume + Update Tracker for file or all files in zip ----
            for current_file_name, current_file_data, file_type, trade_date in prepared:
                logger.info(
                    f"Processing | product='{product_name}' source='{file_name}' "
                    f"file='{current_file_name}' type='{file_type}' dir='{volume_directory}'"
                )

                volume_path = f"{volume_directory}/{current_file_name}"
                logger.info(f"Uploading to UC Volume: '{volume_path}' ({len(current_file_data)} bytes)")
                upload_file_to_volume(
                    target_directory=volume_directory,
                    file_name=current_file_name,
                    data=current_file_data,
                )
                logger.info(f"Uploaded '{current_file_name}' to UC Volume")

                logger.info(f"Updating tracker for '{current_file_name}' (trade_date={trade_date})")
                insert_tracker_record(product_name, current_file_name, file_type, trade_date, today)

            # Delete file in sharepoint
            logger.info(f"Deleting source from SharePoint: '{file_name}'")
            delete_sharepoint_file(token_manager, drive_id, file_id)
            logger.info(f"Deleted source '{file_name}' from SharePoint")

        except Exception as e:
            logger.exception(f"Error processing '{file_name}' - skipping to next file: {e}")
            continue
