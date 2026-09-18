# Fixed Income Unity Catalog migration package

## Reverified user-completed UC files
- controller_t_plus_one.py
- xls_rest_api_raw_ingestion_adls_template.py
- tdsci_fixed_income_daily_etl.py

## Newly migrated UC files
- raw_fi_trades_client_value_service.py
- bronze_fi_desk.py
- bronze_fi_inventory.py
- bronze_fi_trades.py
- silver_fi_desk_inventory.py
- silver_fi_trade.py
- silver_fi_trade_client.py

## No UC code change required from supplied source
- tdsci_fixed_income_t_plus_one_validation.py (API/dataframe validation only; no ADLS/Delta table path references were found)

## Main migration conventions
- CATALOG propagated from controller/ADF to child notebooks.
- Raw files use the governed Unity Catalog Volume via VOLUME_BASE_PATH.
- Bronze/Silver tables use catalog.schema.table identifiers.
- Explicit ADLS Delta LOCATION clauses are removed for migrated tables.
- DeltaTable.forPath is replaced by DeltaTable.forName for governed tables.
- Silver fi_trades and fi_trades_clients enable Delta Change Data Feed.
- Existing FI business transformations and merge keys are preserved.
- bronze_fi_trades accepts both the single-underscore Raw filename and the legacy double-underscore expected filename.
