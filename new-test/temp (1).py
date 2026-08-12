# Databricks notebook source
# MAGIC %md
# MAGIC ## Create UC Tables — Managed Across All Environments
# MAGIC
# MAGIC Policy change: bronze, silver, and raw are now ALL created as **managed**
# MAGIC tables (previously bronze/silver were external, pointing at ADLS via
# MAGIC `LOCATION`, while only raw was managed).
# MAGIC
# MAGIC IMPORTANT — behavior change to be aware of before running this broadly:
# MAGIC - Managed tables: `DROP TABLE` deletes the underlying data files, not just
# MAGIC   metadata. External tables did NOT delete data on drop. Treat the
# MAGIC   "Drop all tables" cell at the bottom as destructive from now on.
# MAGIC - This copies each dataset from its ADLS location into UC-managed storage.
# MAGIC   Any notebook that writes directly to these tables via
# MAGIC   `CREATE TABLE ... LOCATION '...'` (e.g. fx_bronze_layer.py) needs to be
# MAGIC   updated to write via `saveAsTable` instead, or it will conflict with the
# MAGIC   managed table created here.

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# DBTITLE 1,Configuration
dbutils.widgets.text("catalog_name", "")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

VOLUME_BASE_PATH = f"/Volumes/{CATALOG_NAME}/raw/data"

EXTERNAL_LOCATION_BASE_PATH = (
    spark.sql(f"DESCRIBE VOLUME `{CATALOG_NAME}`.raw.data")
        .select("storage_location")
        .first()[0]
        .rstrip("/")
)

ADLS_BASE_PATH = EXTERNAL_LOCATION_BASE_PATH.rsplit("/raw", 1)[0] + "/"

print(f"CATALOG_NAME    : {CATALOG_NAME}")
print(f"ADLS_BASE_PATH  : {ADLS_BASE_PATH}")

# COMMAND ----------

# DBTITLE 1,Table Definitions — (schema, table_name, relative_adls_path)
bronze_tables = [
    ("bronze", "cad_prime", "bronze/cad_prime"),
    ("bronze", "cad_prime_accts", "bronze/cad_prime_accts"),
    ("bronze", "cad_prime_pnl_summary", "bronze/cad_prime_pnl_summary"),
    ("bronze", "cad_prime_securities_lending", "bronze/cad_prime_securities_lending"),
    ("bronze", "clients", "bronze/clients"),
    ("bronze", "energy", "bronze/energy"),
    ("bronze", "fi_desk", "bronze/fi_desk"),
    ("bronze", "fi_inventory", "bronze/fi_inventory"),
    ("bronze", "fi_trades", "bronze/fi_trades"),
    ("bronze", "fund_financing_act", "bronze/fund_financing_act"),
    ("bronze", "fund_financing_cfs", "bronze/fund_financing_cfs"),
    ("bronze", "fx_panoramic", "bronze/fx_panoramic"),
    ("bronze", "fx_rates", "bronze/fx_rates"),
    ("bronze", "ged_notes", "bronze/ged_notes"),
    ("bronze", "ged_options", "bronze/ged_options"),
    ("bronze", "ged_swaps", "bronze/ged_swaps"),
    ("bronze", "gfi_panoramic_hierarchy", "bronze/gfi_panoramic_hierarchy"),
    ("bronze", "gfi_panoramic_trades", "bronze/gfi_panoramic_trades"),
    ("bronze", "gfi_panoramic_trades_test", "bronze/gfi_panoramic_trades_test"),
    ("bronze", "gov_finance", "bronze/gov_finance"),
    ("bronze", "high_yield", "bronze/high_yield"),
    ("bronze", "holiday_calendar", "bronze/holiday_calendar"),
    ("bronze", "ie_missing_rr_client", "bronze/ie_missing_rr_client"),
    ("bronze", "ie_rr_ism", "bronze/ie_rr_ism"),
    ("bronze", "ie_rr_ism_test", "bronze/ie_rr_ism_test"),
    ("bronze", "inst_eq", "bronze/inst_eq"),
    ("bronze", "metal", "bronze/metal"),
    ("bronze", "paam_clients", "bronze/paam_clients"),
    ("bronze", "td_cowen_account", "bronze/td_cowen_account"),
    ("bronze", "td_cowen_equities", "bronze/td_cowen_equities"),
    ("bronze", "tdsat", "bronze/tdsat"),
    ("bronze", "us_munis_general", "bronze/us_munis_general"),
    ("bronze", "us_prime_client_name_account_mapping", "bronze/us_prime_client_name_account_mapping"),
    ("bronze", "us_prime_general", "bronze/us_prime_general"),
    ("bronze", "us_prime_pb_client_pnl", "bronze/us_prime_pb_client_pnl"),
    ("bronze", "x_dealer_delta", "delta/x_dealer"),
]

silver_tables = [
    ("silver", "all_dates", "silver/all_dates"),
    ("silver", "all_dates_gfi_panoramic", "silver/all_dates_gfi_panoramic"),
    ("silver", "cad_prime_monthly", "silver/cad_prime_monthly"),
    ("silver", "cad_prime_pnl_summary", "silver/cad_prime_pnl_summary"),
    ("silver", "cad_prime_securities_lending", "silver/cad_prime_securities_lending"),
    ("silver", "fi_trades", "silver/fi_trades"),
    ("silver", "fi_trades_clients", "silver/fi_trades_clients"),
    ("silver", "fund_financing_act", "silver/fund_financing_act"),
    ("silver", "fund_financing_cfs", "silver/fund_financing_cfs"),
    ("silver", "fx_panoramic", "silver/fx_panoramic"),
    ("silver", "fx_rates", "silver/fx_rates"),
    ("silver", "gfi_panoramic_c360_merged_trades", "silver/gfi_panoramic_c360_merged_trades"),
    ("silver", "gfi_panoramic_trades", "silver/gfi_panoramic_trades"),
    ("silver", "td_cowen_equities", "silver/td_cowen_equities"),
    ("silver", "us_prime", "silver/us_prime"),
    ("silver", "us_prime_bkp", "silver/us_prime_bkp"),
    ("silver", "x_dealer", "silver/x_dealer"),
]

raw_tables = [
    ("raw", "cad_prime_raw_tracker_delta", "raw/tmp/cad_prime/cad_prime_raw_tracker_delta"),
    ("raw", "client_code_matches", "raw/client_code_matches"),
    ("raw", "prism_data", "raw/prism_data"),
    ("raw", "prism_last_modified", "raw/prism_last_modified"),
    ("raw", "prism_processing_state", "raw/prism_processing_state"),
    ("raw", "subordinate_data", "raw/subordinate_data"),
]

all_tables = bronze_tables + silver_tables + raw_tables

# COMMAND ----------

# DBTITLE 1,Create ALL tables as MANAGED (read from ADLS, write into UC-managed storage)
created_managed_tables = []
failed_tables = []

for schema_name, table_name, relative_path in all_tables:
    source_path = ADLS_BASE_PATH + relative_path
    target_table = f"`{CATALOG_NAME}`.`{schema_name}`.`{table_name}`"

    try:
        with capture_errors(f"managed_table_migration:{schema_name}.{table_name}"):
            df = spark.read.format("delta").load(source_path)

            (
                df.write
                  .format("delta")
                  .mode("overwrite")
                  .option("overwriteSchema", "true")
                  .saveAsTable(target_table)
            )

        created_managed_tables.append((schema_name, table_name, source_path))
        print(f"SUCCESS: {target_table}")

    except Exception as e:
        failed_tables.append((schema_name, table_name, str(e)))
        print(f"FAILED: {target_table} -> {e}")

# COMMAND ----------

# DBTITLE 1,Migration Summary
print("=" * 100)
print("Managed Table Migration Summary")
print(f"CATALOG_NAME            : {CATALOG_NAME}")
print(f"Total Tables Attempted  : {len(all_tables)}")
print(f"Managed Tables Created  : {len(created_managed_tables)}")
print(f"Failures                : {len(failed_tables)}")

if failed_tables:
    print("\nFailed Objects:")
    for failure in failed_tables:
        print(failure)
print("=" * 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop all tables (optional cleanup)
# MAGIC **CAUTION:** Now that everything is managed, this deletes the underlying
# MAGIC Delta files, not just UC metadata. Uncomment and run deliberately.

# COMMAND ----------

# dropped_tables = []
# drop_failures = []
#
# schemas = [
#     r.databaseName
#     for r in spark.sql(f"SHOW SCHEMAS IN `{CATALOG_NAME}`").collect()
#     if not r.databaseName.lower().startswith("information_schema")
# ]
#
# for schema in schemas:
#     print(f"\nProcessing schema: {schema}")
#
#     try:
#         tables = spark.catalog.listTables(f"`{CATALOG_NAME}`.{schema}")
#     except Exception as e:
#         print(f"Failed to list tables in `{CATALOG_NAME}`.{schema}: {e}")
#         continue
#
#     for tbl in tables:
#         if tbl.tableType.upper() == "VIEW":
#             continue  # remove if you want to drop views too
#
#         full_name = f"`{CATALOG_NAME}`.`{schema}`.`{tbl.name}`"
#
#         try:
#             spark.sql(f"DROP TABLE {full_name}")
#             dropped_tables.append(full_name)
#             print(f"Dropped: {full_name}")
#         except Exception as e:
#             drop_failures.append((full_name, str(e)))
#             print(f"Failed: {full_name} -> {e}")
#
# print("\n" + "=" * 100)
# print(f"Total Dropped : {len(dropped_tables)}")
# print(f"Total Failed  : {len(drop_failures)}")

# COMMAND ----------

# End of Notebook