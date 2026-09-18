# FI Unity Catalog volume refactor verification

Rechecked against the supplied CAD Prime / other UC notebooks.

## Final UC storage pattern
- Raw FI file access uses `VOLUME_BASE_PATH` directly.
- No migrated FI Python file contains `ADLS_*` path variables, `abfss://`, `DeltaTable.forPath()`, or a Delta `LOCATION` clause.
- Desk files use `VOLUME_BASE_PATH/fixed_income/desks`.
- Inventory files use `VOLUME_BASE_PATH/fixed_income/inventories`.
- Client Value FI trades use `VOLUME_BASE_PATH/trades/fixed_income/client_value_service/<TRADE_SYSTEM>/<year>/<month>/<file>`.
- Raw FI ingestion writes directly into the Volume using the `/Volumes` FUSE path.

## Final UC table pattern
- Bronze/Silver objects use explicit three-level identifiers with `CATALOG_NAME`.
- `bronze.fi_trades` and `silver.fi_trades` are accessed as registered UC tables.
- Delta merges use `DeltaTable.forName()`.
- Silver FI trade targets have Change Data Feed enabled.
- Desk/inventory enrichment uses the catalog-qualified `silver.fi_desk_inventory` view.
- FI client enrichment uses the catalog-qualified `silver.clients` view.

## Controller
`controller_t_plus_one.py` accepts `CATALOG` and propagates it to the daily ETL, Raw FI trades, Bronze FI trades, Silver FI trades, and Silver FI trade-client notebook calls.

## Removed compatibility/legacy logic
- Removed `ADLS_RAW_DATA_PATH` / `ADLS_DESTINATION_PATH` from migrated FI code.
- Removed `to_volume_relative_path()` helpers.
- Removed direct Azure Data Lake storage client usage from the REST ingestion template.
- Removed the legacy double-underscore FI trade filename fallback; Raw and Bronze now use the same single-underscore filename convention.
- Replaced the old generic ADLS-oriented template filename with `xls_rest_api_raw_ingestion_volume_template.py`.

## Validation performed
- All Python files pass Python AST syntax parsing.
- Static scan found no `ADLS_`, `abfss://`, `DeltaTable.forPath`, or `to_volume_relative_path` tokens in migrated FI Python files.

Runtime execution still requires the target catalog, `raw.data` Volume, UC permissions, libraries, and external/API connectivity to exist in the Databricks environment.
