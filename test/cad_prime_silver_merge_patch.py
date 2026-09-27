# Databricks notebook source
# Replacement cell for cad_prime_silver.py, targeting DBR 17.3.
# Replace ONLY the existing cv_post_processing_commit definition with this cell.
# Keep this definition after your imports/error_utils and before its existing call.
# This is a function patch, NOT a standalone replacement for the whole notebook.
#
# Unchanged: CV calculation (including lag/window), fiscal-year selection,
# failed-year handling, merge condition, updated columns, and all other merges.
# Only identical (merge key + all updated values) source rows are collapsed.
# Conflicting values are rejected; no arbitrary first/latest row is selected.
# Existing target rows are NOT deleted or deduplicated.
#
# Recovery: the bronze merge may already have committed before the CV failure.
# A full notebook retry may therefore find no new change-feed rows. If the failed
# session still holds cv_dfs_processed, rerun this definition and then the original
# cv_post_processing_commit call. Otherwise recalculate the affected fiscal years
# explicitly using the existing process_cv function under your recovery procedure.

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


@enhanced_errors()
def cv_post_processing_commit(
    silver_delta_table: DeltaTable,
    cv_dfs_processed: list[DataFrame],
    failed_years: set[int],
) -> None:
    if not cv_dfs_processed:
        return

    logger.info("Gathering successfully processed fiscal years...")
    successfully_processed = [cv_df for cv_df in cv_dfs_processed]

    if failed_years:
        logger.info(
            "Removing failing fiscal years from set of successfully processed "
            f"fiscal years: {failed_years}"
        )
        successfully_processed = [
            cv_df
            for cv_df in cv_dfs_processed
            if not cv_df.filter(F.col("fiscal_year").isin(failed_years)).collect()
        ]

    union_df = successfully_processed[0] if successfully_processed else None
    for index, processed in enumerate(successfully_processed):
        if index != 0:
            union_df = union_df.union(processed)

    if union_df is None:
        return

    merge_keys = ["client_display_name", "trade_date"]
    update_columns = [
        "client_value",
        "client_value_src",
        "trade_currency",
        "pb_pnl_cad",
        "product_class_type",
    ]

    # Distinct across BOTH keys AND every updated value. Never deduplicate by
    # merge_keys alone: doing that could silently choose a different CV value.
    merge_source = union_df.select(*(merge_keys + update_columns)).distinct()

    # Validate only keys that can match the target under the ORIGINAL equality
    # predicate. Unmatched/null keys cannot update anything and remain unchanged.
    target_keys = silver_delta_table.toDF().select(*merge_keys).distinct()
    matching_source = merge_source.join(target_keys, on=merge_keys, how="left_semi")
    conflicting_keys = (
        matching_source.groupBy(*merge_keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(10)
        .collect()
    )
    if conflicting_keys:
        samples = [row.asDict() for row in conflicting_keys]
        raise ValueError(
            "CV_MERGE_CONFLICT: multiple different update payloads exist for "
            "the same (client_display_name, trade_date). Identical payloads "
            "have already been removed. This function has not updated silver. "
            "Resolving these remaining rows requires correcting the input or "
            "confirming a business rule; no first/latest/summed value was chosen. "
            f"Up to 10 conflicting keys and distinct payload counts: {samples}"
        )

    logger.info("Merging interim DataFrame into silver to update client value...")
    (
        silver_delta_table.alias("silver_old")
        .merge(
            merge_source.alias("processed_silver"),
            "silver_old.client_display_name = "
            "processed_silver.client_display_name and "
            "silver_old.trade_date = processed_silver.trade_date",
        )
        .whenMatchedUpdate(
            set={
                "client_value": F.col("processed_silver.client_value"),
                "client_value_src": F.col("processed_silver.client_value_src"),
                "trade_currency": F.col("processed_silver.trade_currency"),
                "pb_pnl_cad": F.col("processed_silver.pb_pnl_cad"),
                "product_class_type": F.col("processed_silver.product_class_type"),
            }
        )
        .execute()
    )
    logger.info("Silver merge for client value done...")
