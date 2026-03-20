"""
silver_transform.py
-------------------
Stage 2 — Silver Layer (Cleanse, Normalise, Deduplicate)

Reads from the Bronze Delta table and applies:
  1. Type casting  (string → proper types)
  2. Currency normalisation  (GBP/EUR/INR → USD)
  3. Deduplication  (keep first occurrence of each order_id)
  4. Row-level filtering  (nulls, out-of-range amounts, future timestamps)
  5. Idempotent MERGE into Silver Delta table (safe to re-run)

Run:
    python src/silver_transform.py
"""
import os
import sys
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, BooleanType, TimestampType
from delta.tables import DeltaTable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import BRONZE_PATH, SILVER_PATH, FX_RATES
from src.utils import get_spark, ensure_dirs, logger


def cast_and_clean(df):
    """Apply type casts and basic cleaning."""
    return (
        df
        # ── Types ──────────────────────────────────────────────────────────
        .withColumn("amount",     F.col("amount").cast(DoubleType()))
        .withColumn("is_return",  F.col("is_return").cast(BooleanType()))
        .withColumn("event_ts",   F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))

        # ── Trim whitespace on key string columns ───────────────────────────
        .withColumn("order_id",         F.trim(F.col("order_id")))
        .withColumn("customer_id",      F.trim(F.col("customer_id")))
        .withColumn("region",           F.trim(F.col("region")))
        .withColumn("product_category", F.trim(F.col("product_category")))
        .withColumn("currency",         F.upper(F.trim(F.col("currency"))))
    )


def normalise_currency(df):
    """Convert all amounts to USD using FX_RATES from config."""
    expr = F.col("amount")           # default: already USD
    for ccy, rate in FX_RATES.items():
        if ccy == "USD":
            continue
        expr = F.when(F.col("currency") == ccy, F.col("amount") * rate).otherwise(expr)

    return df.withColumn("amount_usd", F.round(expr, 2))


def deduplicate(df):
    """
    Keep the first occurrence of each order_id (by event_ts).
    Window-based dedup preserves the earliest record.
    """
    from pyspark.sql.window import Window

    w = Window.partitionBy("order_id").orderBy("event_ts")
    return (
        df
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def apply_filters(df):
    """Remove rows that fail business rules."""
    return (
        df
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("amount_usd").isNotNull())
        .filter(F.col("amount_usd").between(0.01, 100_000))
        .filter(F.col("event_ts").isNotNull())
        .filter(F.col("event_ts") <= F.current_timestamp())   # reject future timestamps
    )


def add_derived_columns(df):
    """Add useful derived columns for downstream layers."""
    return (
        df
        .withColumn("order_date",     F.to_date("event_ts"))
        .withColumn("order_month",    F.date_format("event_ts", "yyyy-MM"))
        .withColumn("processed_at",   F.current_timestamp())
    )


def merge_into_silver(spark, silver_df) -> None:
    """
    Idempotent MERGE — if order_id already exists in Silver, update it;
    otherwise insert.  Safe to call multiple times (idempotent).
    """
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        silver_tbl = DeltaTable.forPath(spark, SILVER_PATH)
        (
            silver_tbl.alias("target")
            .merge(silver_df.alias("source"), "target.order_id = source.order_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("MERGE complete into existing Silver table.")
    else:
        # First run — create the table
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("order_date", "region")
            .save(SILVER_PATH)
        )
        logger.info("Silver table created (first run).")


def run_silver() -> None:
    ensure_dirs(SILVER_PATH)
    spark = get_spark("Silver-Transform")

    logger.info("Reading Bronze table: %s", BRONZE_PATH)
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    logger.info("  Bronze rows: %d", bronze_df.count())

    df = cast_and_clean(bronze_df)
    df = normalise_currency(df)
    df = deduplicate(df)
    df = apply_filters(df)
    df = add_derived_columns(df)

    silver_count = df.count()
    logger.info("Silver rows (after transforms): %d", silver_count)

    merge_into_silver(spark, df)
    logger.info("Silver layer complete.")


if __name__ == "__main__":
    run_silver()
