# Fixed Income (FI) – Legacy to Unity Catalog Migration

## Purpose

This README documents the Fixed Income (FI) Databricks migration from the legacy Hive Metastore / direct-ADLS implementation to the Unity Catalog (UC) implementation.

The migration was intentionally limited to **storage access, catalog/table resolution, governed Volume access, and orchestration parameters**. Existing FI business transformations, schemas, merge keys, T+1 logic, API logic, enrichment logic, and validation rules were preserved unless a change was required to make the pipeline work with Unity Catalog.

---

## 1. End-to-end FI flow

### Legacy

```text
REST / C360 APIs
      |
      v
Direct ADLS SDK / abfss:// paths
      |
      v
bronze.fi_desk / bronze.fi_inventory / bronze.fi_trades
      |
      v
silver.fi_desk_inventory / silver.fi_trades
      |
      v
silver.fi_trades_clients / silver.fi_trades_display_view
```

### Unity Catalog

```text
REST / C360 APIs
      |
      v
Unity Catalog Raw Volume (VOLUME_BASE_PATH)
      |
      v
<CATALOG>.bronze.fi_desk
<CATALOG>.bronze.fi_inventory
<CATALOG>.bronze.fi_trades
      |
      v
<CATALOG>.silver.fi_desk_inventory
<CATALOG>.silver.fi_trades
      |
      v
<CATALOG>.silver.fi_trades_clients
<CATALOG>.silver.fi_trades_display_view
```

---

## 2. Common migration rules used across FI

The FI migration follows the same pattern as the existing UC/CAD Prime migration:

| Legacy pattern | UC pattern |
|---|---|
| `%run "./adls_util.py"` | `%run "./volume_util.py"` where raw file access is required |
| `ADLS_CONTAINER_NAME` / `ADLS_ACCOUNT_URI` | Removed |
| `ADLS_RAW_DATA_PATH` / `ADLS_DESTINATION_PATH` | Removed from the migrated FI storage flow |
| `abfss://...` | `VOLUME_BASE_PATH/...` |
| Azure Data Lake SDK for raw writes | Direct writes through the UC Volume path |
| `bronze.table` / `silver.table` | ``<CATALOG>.bronze.table`` / ``<CATALOG>.silver.table`` |
| Delta `LOCATION 'abfss://...'` | Removed for UC-managed FI tables |
| `DeltaTable.forPath(...)` | `DeltaTable.forName(...)` |
| `spark.read.format("delta").load(path)` for registered tables | `spark.table(<catalog-qualified-table>)` |
| Session/default metastore resolution | Explicit `CATALOG` widget + `USE CATALOG` |
| Child notebooks inherit assumptions | Controller explicitly passes `CATALOG` |
| Direct storage authorization | UC Volume/table permissions |

The shared Volume utility resolves the raw Volume root as:

```text
/Volumes/<CATALOG>/raw/data
```

FI raw files are placed underneath that root.

---

# 3. File-by-file Legacy vs UC changes

## 3.1 `controller_t_plus_one.py`

### Purpose

Controls the T+1 FI flow and runs the daily ETL, Raw FI ingestion, Bronze FI ingestion, then the Silver FI notebooks.

### Legacy

The controller accepted FI runtime parameters such as:

- trade date
- trade system
- authentication values
- retry settings
- processing partition counts

It called child notebooks without passing a Unity Catalog value.

Silver notebooks were executed as:

```python
dbutils.notebook.run(silver_fi_trade_nb, 1200)
dbutils.notebook.run(silver_fi_trade_client_nb, 1200)
```

### UC changes

Added the catalog widget:

```python
dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
```

The same catalog is now passed to each relevant child notebook.

T+1 ETL arguments now include:

```python
"CATALOG": CATALOG_NAME
```

Raw FI arguments now include:

```python
"CATALOG": CATALOG_NAME
```

Bronze FI arguments now include:

```python
"CATALOG": CATALOG_NAME
```

Silver notebooks are now called with:

```python
dbutils.notebook.run(
    silver_fi_trade_nb,
    1200,
    {"CATALOG": CATALOG_NAME},
)

dbutils.notebook.run(
    silver_fi_trade_client_nb,
    1200,
    {"CATALOG": CATALOG_NAME},
)
```

### Business logic changed?

**No.**

Parallel date processing, retry behavior, success/failure collection, and notebook execution order are preserved.

### Deployment impact

ADF/job configuration must supply the correct `CATALOG` value for DEV/PAT/PROD.

---

## 3.2 `tdsci_fixed_income_daily_etl.py`

### Purpose

Runs the existing Fixed Income T+1 preprocessing/enrichment and Client Value processing.

### Legacy

This notebook contains the core T+1 application logic and external API/Dremio processing. It does not directly read or write the FI Bronze/Silver Delta tables through legacy ADLS paths.

### UC changes

Added a `CATALOG` widget so the controller can pass a consistent parameter to every FI child notebook:

```python
dbutils.widgets.text("CATALOG", "")
```

The value is not otherwise used by this notebook because it does not directly operate on Unity Catalog objects.

### Business logic changed?

**No.**

No T+1 enrichment, API, concurrency, authentication, trade preprocessing, or Client Value processing logic was intentionally changed for UC.

---

## 3.3 `raw_fi_trades_client_value_service.py`

### Purpose

Retrieves FI trades from the TDSCI Client Value Service and lands the raw CSV for Bronze processing.

### Legacy

Loaded:

```python
%run "./adls_util.py"
```

Created an ADLS file client using:

```python
get_directory_client(...).get_file_client(...)
```

and uploaded the API response using:

```python
file_client.upload_data(
    data=response.content,
    overwrite=True,
)
```

The raw location was conceptually:

```text
data/raw/trades/fixed_income/client_value_service/<TRADE_SYSTEM>/<year>/<month>/
```

### UC changes

Catalog selection added:

```python
dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")
```

ADLS utility replaced with:

```python
%run "./volume_util.py"
```

The raw destination is now built directly from the UC Volume root:

```python
target_dir = (
    f"{VOLUME_BASE_PATH}/trades/fixed_income/client_value_service/"
    f"{TRADE_SYSTEM}/{year}/{month}"
)
```

The directory is created using:

```python
dbutils.fs.mkdirs(target_dir)
```

The response is written directly through the FUSE-mounted Volume path:

```python
with open(output_path, "wb") as f:
    f.write(response.content)
```

### Filename convention

The migrated Raw and Bronze FI trade notebooks use the same filename:

```text
trades_client_value_service_<TRADE_SYSTEM>_<YYYY-MM-DD>.csv
```

This removes the old Raw/Bronze filename inconsistency.

### Removed

- `adls_util.py`
- `get_directory_client()`
- direct ADLS file client
- `upload_data()`
- direct storage-account path handling

### Business logic changed?

**No.**

Client Value Service URL, OAuth flow, trade-date calculation, and request parameters remain functionally the same.

---

## 3.4 `bronze_fi_trades.py`

### Purpose

Loads the raw FI Client Value Service CSV and MERGEs trades into the FI Bronze table.

### Legacy

Loaded:

```python
%run "./adls_util.py"
```

Built storage configuration from:

```python
ADLS_ACCOUNT_URI
ADLS_CONTAINER_NAME
```

Raw input was read from an `abfss://...` path.

Bronze was created at an explicit physical Delta location:

```sql
CREATE TABLE IF NOT EXISTS bronze.fi_trades (...)
USING DELTA
LOCATION '<bronze ADLS path>'
```

The Delta table was opened using:

```python
DeltaTable.forPath(spark, bronze_table_location)
```

### UC changes

Added:

```python
dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")
```

Uses:

```python
%run "./volume_util.py"
```

The raw file path is now direct Volume access:

```python
client_value_service_trades_path = (
    f"{VOLUME_BASE_PATH}/trades/fixed_income/client_value_service/"
    f"{TRADE_SYSTEM}/{trade_dt.year}/{trade_dt.month}/{trades_filename}"
)
```

The Bronze table is explicitly catalog-qualified:

```python
BRONZE_FI_TRADES_TABLE = (
    f"`{CATALOG_NAME}`.`bronze`.`fi_trades`"
)
```

The table is created as a UC-managed table:

```sql
CREATE TABLE IF NOT EXISTS <CATALOG>.bronze.fi_trades (...)
USING DELTA
```

No physical `LOCATION` is supplied.

The MERGE target is opened using:

```python
DeltaTable.forName(
    spark,
    BRONZE_FI_TRADES_TABLE,
)
```

### Removed

- `ADLS_ACCOUNT_URI`
- `ADLS_CONTAINER_NAME`
- `abfss://...`
- `bronze_table_location`
- `LOCATION`
- `DeltaTable.forPath()`
- legacy raw-path concatenation

### Preserved

- complete Bronze trade schema
- CSV parsing
- `tradeId + tradeSystem` MERGE condition
- schema auto-merge behavior
- update-all / insert-all semantics
- error handling

### Business logic changed?

**No.**

The change is storage/table-resolution only.

---

## 3.5 `bronze_fi_desk.py`

### Purpose

Reads the latest FI desk Excel file and refreshes the Bronze desk table.

### Legacy

Loaded:

```python
%run "./adls_util.py"
```

Used:

```python
ADLS_CONTAINER_NAME
ADLS_ACCOUNT_URI
ADLS_RAW_DATA_PATH
```

Built:

```text
abfss://<container>@<account>/<ADLS_RAW_DATA_PATH>
```

and wrote:

```python
.saveAsTable(TARGET_SCHEMA_NAME + ".fi_desk")
```

### UC changes

Added catalog selection and `volume_util.py`.

The raw location is now a direct Volume constant:

```python
RAW_FI_DESK_VOLUME_PATH = (
    f"{VOLUME_BASE_PATH}/fixed_income/desks"
)
```

The target is explicitly qualified:

```python
BRONZE_FI_DESK_TABLE = (
    f"`{CATALOG_NAME}`.`bronze`.`fi_desk`"
)
```

Latest-file lookup operates directly on the UC Volume:

```python
file = get_latest_file_from_dir(
    RAW_FI_DESK_VOLUME_PATH
)
```

The Excel file is still loaded using `com.crealytics.spark.excel`.

The target is written as a UC-managed table:

```python
.saveAsTable(BRONZE_FI_DESK_TABLE)
```

### Removed

- `ADLS_CONTAINER_NAME`
- `ADLS_ACCOUNT_URI`
- `ADLS_RAW_DATA_PATH`
- `TARGET_SCHEMA_NAME`
- `abfss://...`
- unused legacy imports

### Preserved

- desk schema
- latest-file behavior
- source filename capture
- overwrite behavior
- `overwriteSchema=true`

### Business logic changed?

**No.**

---

## 3.6 `bronze_fi_inventory.py`

### Purpose

Reads the latest FI inventory Excel file and refreshes the Bronze inventory table.

### Legacy

Used:

```python
ADLS_CONTAINER_NAME
ADLS_ACCOUNT_URI
ADLS_DESTINATION_PATH
```

and built an `abfss://...` folder.

The target was:

```text
bronze.fi_inventory
```

### UC changes

The raw path is now directly defined from the Volume root:

```python
RAW_FI_INVENTORY_VOLUME_PATH = (
    f"{VOLUME_BASE_PATH}/fixed_income/inventories"
)
```

The UC table is:

```python
BRONZE_FI_INVENTORY_TABLE = (
    f"`{CATALOG_NAME}`.`bronze`.`fi_inventory`"
)
```

The latest file is obtained with:

```python
file = get_latest_file_from_dir(
    RAW_FI_INVENTORY_VOLUME_PATH
)
```

`dbutils.fs.ls()` remains valid because it is operating on the `/Volumes/...` path provided by `VOLUME_BASE_PATH`.

The table is written with:

```python
.saveAsTable(BRONZE_FI_INVENTORY_TABLE)
```

### Removed

- all ADLS account/container settings
- `ADLS_DESTINATION_PATH`
- legacy path-normalization/conversion logic
- `abfss://...`
- `TARGET_SCHEMA_NAME`

### Preserved

- inventory schema
- latest-file selection by `modificationTime`
- Excel reader
- source filename capture
- overwrite semantics

### Business logic changed?

**No.**

---

## 3.7 `silver_fi_desk_inventory.py`

### Purpose

Creates the Silver view joining FI inventory information with desk/business information.

### Legacy

Used SQL with two-part names:

```sql
CREATE OR REPLACE VIEW silver.fi_desk_inventory AS
...
FROM bronze.fi_inventory i
INNER JOIN bronze.fi_desk d
  ON i.deskId = d.deskId
```

### UC changes

Added catalog selection.

Defined explicit UC object names:

```python
SILVER_FI_DESK_INVENTORY_VIEW = (
    f"`{CATALOG_NAME}`.`silver`.`fi_desk_inventory`"
)

BRONZE_FI_INVENTORY_TABLE = (
    f"`{CATALOG_NAME}`.`bronze`.`fi_inventory`"
)

BRONZE_FI_DESK_TABLE = (
    f"`{CATALOG_NAME}`.`bronze`.`fi_desk`"
)
```

The SQL now uses the catalog-qualified objects.

### Business logic changed?

**No.**

The selected columns and `i.deskId = d.deskId` join are unchanged.

---

## 3.8 `silver_fi_trade.py`

### Purpose

Transforms FI Bronze trades into the reporting-oriented FI Silver trade model.

### Legacy

Used ADLS path widgets:

```python
ADLS_TRADES_SOURCE_PATH
ADLS_DESTINATION_PATH
```

and environment variables:

```python
ADLS_CONTAINER_NAME
ADLS_ACCOUNT_URI
```

Bronze was read from a physical path:

```python
spark.read.format("delta").load(bronze_table_path)
```

Desk/inventory was read using:

```python
spark.table("silver.fi_desk_inventory")
```

Silver was created with:

```sql
CREATE TABLE IF NOT EXISTS silver.fi_trades (...)
USING DELTA
LOCATION '<ADLS path>'
```

The target Delta table used:

```python
DeltaTable.forPath(spark, silver_table_path)
```

### UC changes

Added explicit catalog selection.

Defined:

```python
BRONZE_FI_TRADES_TABLE = (
    f"`{CATALOG_NAME}`.`bronze`.`fi_trades`"
)

SILVER_FI_DESK_INVENTORY_VIEW = (
    f"`{CATALOG_NAME}`.`silver`.`fi_desk_inventory`"
)

SILVER_FI_TRADES_TABLE = (
    f"`{CATALOG_NAME}`.`silver`.`fi_trades`"
)
```

Bronze is now read through UC metadata:

```python
bronze_df = spark.table(
    BRONZE_FI_TRADES_TABLE
).select(relevant_columns)
```

Desk/inventory uses the catalog-qualified UC view:

```python
spark.table(SILVER_FI_DESK_INVENTORY_VIEW)
```

Silver is now a managed UC Delta table with no physical `LOCATION`:

```sql
CREATE TABLE IF NOT EXISTS <CATALOG>.silver.fi_trades (...)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
```

MERGE target:

```python
DeltaTable.forName(
    spark,
    SILVER_FI_TRADES_TABLE,
)
```

### Removed

- `ADLS_TRADES_SOURCE_PATH`
- `ADLS_DESTINATION_PATH`
- ADLS account/container settings
- physical Bronze Delta path
- physical Silver Delta path
- `LOCATION`
- `DeltaTable.forPath()`

### Preserved business logic

The following FI transformations are intentionally retained:

- relevant-column selection
- C360-to-reporting column renames
- fiscal quarter/year derivation
- trade volume calculations
- CAD/USD calculations
- DV01 calculations
- legacy product hierarchy preservation
- desk/business/inventory enrichment
- product-class enrichment
- Client Value waterfall
- client-facing formatting
- unmapped-product defaults
- Canadian Fixed Income naming adjustment
- Canadian DCM filtering
- final Silver column selection
- `tradeId + sourceSystem` MERGE key

### Business logic changed?

**No intentional business-rule change.**

The material change is UC storage/table access plus CDF enablement.

---

## 3.9 `silver_fi_trade_client.py`

### Purpose

Enriches FI Silver trades with C360 client attributes and creates the readable display view.

### Legacy

Constructed source/target table names from environment variables and created an explicit ADLS target path:

```python
destination_table_path = (
    "abfss://" + ADLS_CONTAINER_NAME + "@" +
    ADLS_ACCOUNT_URI + "/" + ADLS_DESTINATION_PATH
)
```

Created:

```sql
CREATE TABLE IF NOT EXISTS silver.fi_trades_clients
USING DELTA
LOCATION '<destination_table_path>'
```

Client mapping referenced:

```text
silver.clients
```

Display view referenced:

```text
silver.fi_trades_display_view
```

### UC changes

Added explicit catalog selection.

Defined:

```python
SILVER_FI_TRADES_TABLE = (
    f"`{CATALOG_NAME}`.`silver`.`fi_trades`"
)

SILVER_FI_TRADES_CLIENTS_TABLE = (
    f"`{CATALOG_NAME}`.`silver`.`fi_trades_clients`"
)

SILVER_CLIENTS_VIEW = (
    f"`{CATALOG_NAME}`.`silver`.`clients`"
)

SILVER_FI_TRADES_DISPLAY_VIEW = (
    f"`{CATALOG_NAME}`.`silver`.`fi_trades_display_view`"
)
```

The target is now created without an ADLS `LOCATION`:

```sql
CREATE TABLE IF NOT EXISTS <CATALOG>.silver.fi_trades_clients
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS SELECT ...
```

Client enrichment now explicitly joins:

```text
<CATALOG>.silver.clients
```

The display view is created under:

```text
<CATALOG>.silver.fi_trades_display_view
```

### Removed

- source/target schema environment resolution
- ADLS container/account configuration
- ADLS destination path
- `abfss://...`
- Delta `LOCATION`
- unqualified `silver.clients`
- unqualified display view name

### Preserved

Client matching remains:

```text
UPPER(t.sourceSystem) = UPPER(c.sourceSystem)
AND
UPPER(t.counterPartyCode) = UPPER(c.shortCode)
```

MERGE key remains:

```text
tradeId + sourceSystem
```

Existing client display fields and readable display-view columns are preserved.

### Business logic changed?

**No.**

---

## 3.10 `xls_rest_api_raw_ingestion_volume_template.py`

### Purpose

Reusable raw REST ingestion template for FI reference files such as desks/inventories.

### Legacy file

```text
xls_rest_api_raw_ingestion_adls_template.py
```

### Legacy behavior

Imported Azure storage libraries:

```python
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
```

Used:

```python
ADLS_DESTINATION_PATH
ADLS_CONTAINER_NAME
ADLS_ACCOUNT_URI
ADB_TENANT_ID
ADB_SP_APP_ID
ADB_SP_PWD
```

Created a storage credential and `DataLakeServiceClient`, then uploaded with:

```python
fc.upload_data(
    data=response.content,
    overwrite=True,
)
```

### UC changes

Renamed to:

```text
xls_rest_api_raw_ingestion_volume_template.py
```

Added:

```python
dbutils.widgets.text("CATALOG", "")
```

Added a Volume-relative destination parameter:

```python
dbutils.widgets.text(
    "VOLUME_RELATIVE_PATH",
    "fixed_income/inventories",
)
```

Catalog is selected and `volume_util.py` is loaded.

Destination is now:

```python
target_dir = (
    f"{VOLUME_BASE_PATH}/{VOLUME_RELATIVE_PATH}"
)
```

Directory creation:

```python
dbutils.fs.mkdirs(target_dir)
```

Response content is written directly to:

```python
output_path = f"{target_dir}/{file_name}"

with open(output_path, "wb") as f:
    f.write(response.content)
```

### Removed

- Azure Data Lake storage SDK imports
- storage service principal configuration
- ADLS account/container variables
- `DataLakeServiceClient`
- filesystem client
- directory client
- `ADLS_DESTINATION_PATH`
- direct ADLS upload

### Typical values

Inventory:

```text
VOLUME_RELATIVE_PATH=fixed_income/inventories
```

Desk:

```text
VOLUME_RELATIVE_PATH=fixed_income/desks
```

### Business logic changed?

**No.**

REST/API authentication and response retrieval remain intact; only the raw-file destination changed.

---

## 3.11 `tdsci_fixed_income_t_plus_one_validation.py`

### Migration result

**No UC code change required based on the supplied notebook.**

The supplied validation notebook primarily performs API/data validation and does not contain the FI Hive/ADLS table-path access patterns targeted by this migration.

It should remain part of the FI testing flow, but it was not rewritten solely for Unity Catalog.

---

# 4. FI path mapping

## Raw FI trades

### Legacy

```text
data/raw/trades/fixed_income/client_value_service/
    <TRADE_SYSTEM>/<year>/<month>/<file>.csv
```

### UC

```text
VOLUME_BASE_PATH/trades/fixed_income/client_value_service/
    <TRADE_SYSTEM>/<year>/<month>/<file>.csv
```

Example resolved location:

```text
/Volumes/<catalog>/raw/data/trades/fixed_income/
client_value_service/TOMS/2026/9/
trades_client_value_service_TOMS_2026-09-18.csv
```

## FI desk

```text
VOLUME_BASE_PATH/fixed_income/desks
```

## FI inventory

```text
VOLUME_BASE_PATH/fixed_income/inventories
```

---

# 5. FI UC object mapping

| Layer | UC object |
|---|---|
| Bronze | `<CATALOG>.bronze.fi_desk` |
| Bronze | `<CATALOG>.bronze.fi_inventory` |
| Bronze | `<CATALOG>.bronze.fi_trades` |
| Silver view | `<CATALOG>.silver.fi_desk_inventory` |
| Silver | `<CATALOG>.silver.fi_trades` |
| Silver | `<CATALOG>.silver.fi_trades_clients` |
| Silver view | `<CATALOG>.silver.fi_trades_display_view` |
| Shared client view | `<CATALOG>.silver.clients` |

---

# 6. Items intentionally preserved from Legacy

The following were **not** redesigned as part of the UC migration:

- FI trade schema
- FI desk schema
- FI inventory schema
- Client Value Service API contract
- authentication flow for business APIs
- default previous-business-day logic
- T+1 processing flow
- controller retry behavior
- parallel trade-date processing
- Bronze MERGE key: `tradeId + tradeSystem`
- Silver MERGE key: `tradeId + sourceSystem`
- desk-to-inventory join on `deskId`
- product hierarchy logic
- fiscal year / fiscal quarter derivations
- volume calculations
- FX calculations
- DV01 calculations
- Client Value waterfall
- client mapping by source system + counterparty short code
- output/display column mappings

This is important for Legacy-vs-UC reconciliation: differences should primarily come from environment/data timing or migration defects, not from intentionally changed FI business rules.

---

# 7. ADF / Job changes required

The orchestration layer should supply:

```text
CATALOG=<environment-specific UC catalog>
```

Examples conceptually:

```text
DEV -> <dev catalog>
PAT -> <pat catalog>
PROD -> <prod catalog>
```

For the reusable reference-data ingestion template, replace the old ADLS destination parameter with:

```text
VOLUME_RELATIVE_PATH
```

Typical values:

```text
fixed_income/desks
fixed_income/inventories
```

If ADF currently references:

```text
xls_rest_api_raw_ingestion_adls_template.py
```

update the notebook reference to:

```text
xls_rest_api_raw_ingestion_volume_template.py
```

---

# 8. Required runtime dependencies / permissions

The FI UC pipeline still requires its existing application/runtime dependencies, including where applicable:

- `com.crealytics.spark.excel` for Desk/Inventory Excel reads
- existing Client360 T+1 Python package/library
- REST connectivity to the Client Value / Business services
- Dremio connectivity where used by the T+1 application
- authentication/secret access required by those external services

Unity Catalog access must allow the job identity to:

- use the target catalog
- use the `raw`, `bronze`, and `silver` schemas as required
- read/write the Raw Volume
- create/read/write the FI Bronze/Silver managed tables as required
- create/replace the FI Silver views as required
- read `<CATALOG>.silver.clients`

---

# 9. Migration validation checklist

## Raw

- Confirm Raw Client Value CSV is created under the expected `/Volumes/...` directory.
- Confirm Raw and Bronze use the exact same filename convention.
- Confirm Desk and Inventory ingestion write files to the intended Volume subdirectories.

## Bronze

- Compare Legacy and UC `fi_desk` row counts and schema.
- Compare Legacy and UC `fi_inventory` row counts and schema.
- Compare `fi_trades` by trade date and source system.
- Verify rerunning the same Raw trade file updates existing `tradeId + tradeSystem` rows rather than duplicating them.

## Silver

- Compare `fi_trades` row counts by source system/trade date.
- Validate desk/inventory enrichment coverage.
- Validate product hierarchy output.
- Validate fiscal year/quarter output.
- Validate CAD/USD volume calculations.
- Validate DV01 calculations.
- Validate Client Value fields.
- Verify Canadian DCM filtering is unchanged.
- Verify Silver MERGE remains idempotent.

## Client enrichment

- Compare mapped vs unmapped client counts.
- Validate `sourceSystem + counterPartyCode` mapping behavior.
- Validate all display-view columns and names.

## UC-specific

- Verify no FI notebook performs direct `abfss://` access.
- Verify no FI notebook uses `DeltaTable.forPath()` for registered FI tables.
- Verify no FI managed table DDL contains a physical ADLS `LOCATION`.
- Verify all FI table/view references use the selected catalog.
- Verify controller passes the same `CATALOG` value to all FI child notebooks.
- Verify CDF is enabled on the intended Silver FI Delta tables.
- Verify job identity permissions on catalog/schema/table/Volume objects.

---

# 10. Summary of changed FI files

| File | Migration classification |
|---|---|
| `controller_t_plus_one.py` | Catalog/orchestration change |
| `tdsci_fixed_income_daily_etl.py` | Catalog parameter acceptance only |
| `raw_fi_trades_client_value_service.py` | Direct ADLS write → UC Volume write |
| `bronze_fi_trades.py` | ABFSS/path table → Volume + managed UC table |
| `bronze_fi_desk.py` | ABFSS Excel read → Volume Excel read + UC table |
| `bronze_fi_inventory.py` | ABFSS Excel read → Volume Excel read + UC table |
| `silver_fi_desk_inventory.py` | Two-part view/table names → three-part UC names |
| `silver_fi_trade.py` | Physical Delta paths → governed UC tables + CDF |
| `silver_fi_trade_client.py` | External ADLS target → managed UC table + qualified views |
| `xls_rest_api_raw_ingestion_volume_template.py` | ADLS SDK template → UC Volume ingestion template |
| `tdsci_fixed_income_t_plus_one_validation.py` | No migration code change required from supplied version |

---

## Final migration principle

For the FI pipeline:

```text
RAW FILES     -> use VOLUME_BASE_PATH (/Volumes/...)
DELTA TABLES  -> use <CATALOG>.<SCHEMA>.<TABLE>
VIEWS         -> use <CATALOG>.<SCHEMA>.<VIEW>
ORCHESTRATION -> pass CATALOG explicitly
```

Do not use direct `abfss://` paths for the migrated FI file flow and do not resolve registered FI Delta tables by physical storage path.
