"""
bronze_ingest.py
----------------
Stage 1 — Bronze Layer (Raw Ingestion)

Reads raw source files (JSON / CSV) and writes them as-is into a Delta Lake
Bronze table.  No transformation is applied — this is the immutable audit trail.

Run:
    python src/bronze_ingest.py
"""
import os
import sys
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    BRONZE_PATH, WEB_ORDERS_PATH, MOBILE_EVENTS_PATH, INSTORE_PATH
)
from src.utils import get_spark, ensure_dirs, logger


# ── Raw source schema (enforced at ingest time) ───────────────────────────────
RAW_SCHEMA = StructType([
    StructField("order_id",         StringType(),  True),
    StructField("customer_id",      StringType(),  True),
    StructField("amount",           StringType(),  True),   # keep as string — cast in Silver
    StructField("currency",         StringType(),  True),
    StructField("product_category", StringType(),  True),
    StructField("product_id",       StringType(),  True),
    StructField("region",           StringType(),  True),
    StructField("channel",          StringType(),  True),
    StructField("is_return",        StringType(),  True),   # "True"/"False" string
    StructField("event_time",       StringType(),  True),
])


def ingest_source(spark, path: str, fmt: str, source_name: str):
    """Read one source file, tag it, and return a DataFrame."""
    logger.info("Reading %s source: %s", source_name, path)

    if fmt == "json":
        df = (
            spark.read
            .option("multiline", "true")
            .schema(RAW_SCHEMA)
            .json(path)
        )
    else:
        df = (
            spark.read
            .option("header", "true")
            .schema(RAW_SCHEMA)
            .csv(path)
        )

    df = (
        df
        .withColumn("source_system",  F.lit(source_name))
        .withColumn("ingested_at",    F.current_timestamp())
        .withColumn("pipeline_date",  F.current_date())
    )

    logger.info("  %s: %d rows", source_name, df.count())
    return df


def run_bronze() -> None:
    ensure_dirs(BRONZE_PATH)
    spark = get_spark("Bronze-Ingest")

    web_df    = ingest_source(spark, WEB_ORDERS_PATH,    "json", "web")
    mobile_df = ingest_source(spark, MOBILE_EVENTS_PATH, "csv",  "mobile")
    instore_df= ingest_source(spark, INSTORE_PATH,       "csv",  "instore")

    # Union all three sources (allowMissingColumns handles any schema drift)
    combined = (
        web_df
        .unionByName(mobile_df,  allowMissingColumns=True)
        .unionByName(instore_df, allowMissingColumns=True)
    )

    total = combined.count()
    logger.info("Writing %d rows to Bronze Delta table: %s", total, BRONZE_PATH)

    (
        combined.write
        .format("delta")
        .mode("append")             # append-only — never overwrite raw data
        .partitionBy("pipeline_date", "source_system")
        .save(BRONZE_PATH)
    )

    logger.info("Bronze layer complete. Rows written: %d", total)


if __name__ == "__main__":
    run_bronze()
