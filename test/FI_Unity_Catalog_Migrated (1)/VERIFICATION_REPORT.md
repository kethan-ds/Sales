# FI Unity Catalog structural verification

Rechecked against the Unity Catalog notebooks supplied in this conversation.

## Result
The migrated FI notebooks follow the same UC migration structure used by the supplied CAD Prime, client mapping, Fund Financing, and All Dates UC notebooks:

- `CATALOG` widget / `CATALOG_NAME` and `USE CATALOG` where the notebook owns catalog selection.
- Three-level table/view names: `catalog.schema.object`.
- `volume_util.py` and `VOLUME_BASE_PATH` for raw file I/O.
- No direct `abfss://` paths in the migrated FI code.
- No Delta `LOCATION` clauses in the migrated FI code.
- `DeltaTable.forName()` replaces `DeltaTable.forPath()` for registered FI tables.
- `spark.table()` is used for governed Bronze/Silver table reads.
- Silver FI Delta targets enable Change Data Feed; the desk/inventory object remains a view.
- Controller propagates the same `CATALOG` value to FI child notebooks.
- Existing business transformations and merge keys are preserved.

## Three pre-existing UC files
The supplied `controller_t_plus_one.py`, `tdsci_fixed_income_daily_etl.py`, and `xls_rest_api_raw_ingestion_adls_template.py` were preserved as supplied (apart from a trailing newline when packaged).

## Intentional compatibility differences
1. `bronze_fi_trades.py` accepts both the single-underscore Raw filename and the legacy double-underscore filename because the supplied legacy Raw and Bronze notebooks disagree on the filename convention.
2. The legacy widget names such as `ADLS_DESTINATION_PATH` are retained where useful so existing ADF parameter names do not need to change. In the UC notebooks these values resolve to Volume-relative paths rather than direct ADLS paths.
3. The supplied UC codebase itself contains more than one ordering convention for `volume_util.py` and the `CATALOG` widget. The newly migrated FI notebooks use the common pattern: read `CATALOG`, `USE CATALOG`, then load `volume_util.py` when file access is required.
