"""
test_transforms.py
------------------
Unit tests for Silver layer transformation functions.

Run:
    pytest tests/ -v
    pytest tests/ -v --cov=src --cov-report=term-missing
"""
import os
import sys
import pytest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

from src.utils import get_spark
from src.silver_transform import cast_and_clean, normalise_currency, deduplicate, apply_filters
from src.quality_checks import (
    check_null_completeness, check_amount_range,
    check_duplicate_orders, check_schema_conformance,
)


@pytest.fixture(scope="session")
def spark():
    return get_spark("TestSuite")


@pytest.fixture
def sample_bronze_df(spark):
    """Minimal Bronze-like DataFrame for testing."""
    data = [
        Row(order_id="ORD-001", customer_id="cust-1111-1111-1111-1111-111111111111",
            amount="150.00", currency="USD", product_category="Electronics",
            product_id="PROD-1001", region="North", channel="web",
            is_return="False", event_time="2024-03-01 10:00:00",
            source_system="web", ingested_at=datetime(2024, 3, 1, 10, 5, 0),
            pipeline_date=None),

        Row(order_id="ORD-002", customer_id="cust-2222-2222-2222-2222-222222222222",
            amount="200.00", currency="GBP", product_category="Clothing",
            product_id="PROD-2002", region="South", channel="mobile",
            is_return="False", event_time="2024-03-01 11:00:00",
            source_system="mobile", ingested_at=datetime(2024, 3, 1, 11, 5, 0),
            pipeline_date=None),

        Row(order_id="ORD-003", customer_id="cust-3333-3333-3333-3333-333333333333",
            amount=None, currency="EUR", product_category="Books",
            product_id="PROD-3003", region="East", channel="instore",
            is_return="True", event_time="2024-03-01 12:00:00",
            source_system="instore", ingested_at=datetime(2024, 3, 1, 12, 5, 0),
            pipeline_date=None),

        # Duplicate of ORD-001
        Row(order_id="ORD-001", customer_id="cust-1111-1111-1111-1111-111111111111",
            amount="150.00", currency="USD", product_category="Electronics",
            product_id="PROD-1001", region="North", channel="web",
            is_return="False", event_time="2024-03-01 10:00:00",
            source_system="web", ingested_at=datetime(2024, 3, 1, 10, 6, 0),
            pipeline_date=None),
    ]

    schema = StructType([
        StructField("order_id",         StringType(),    True),
        StructField("customer_id",      StringType(),    True),
        StructField("amount",           StringType(),    True),
        StructField("currency",         StringType(),    True),
        StructField("product_category", StringType(),    True),
        StructField("product_id",       StringType(),    True),
        StructField("region",           StringType(),    True),
        StructField("channel",          StringType(),    True),
        StructField("is_return",        StringType(),    True),
        StructField("event_time",       StringType(),    True),
        StructField("source_system",    StringType(),    True),
        StructField("ingested_at",      TimestampType(), True),
        StructField("pipeline_date",    StringType(),    True),
    ])
    return spark.createDataFrame(data, schema=schema)


# ── cast_and_clean ────────────────────────────────────────────────────────────

class TestCastAndClean:
    def test_amount_cast_to_double(self, sample_bronze_df):
        result = cast_and_clean(sample_bronze_df)
        dtype = dict(result.dtypes)["amount"]
        assert dtype == "double", f"Expected double, got {dtype}"

    def test_event_ts_is_timestamp(self, sample_bronze_df):
        result = cast_and_clean(sample_bronze_df)
        dtype = dict(result.dtypes)["event_ts"]
        assert dtype == "timestamp"

    def test_currency_uppercased(self, sample_bronze_df):
        result = cast_and_clean(sample_bronze_df)
        currencies = {r["currency"] for r in result.select("currency").collect()}
        assert all(c == c.upper() for c in currencies if c)

    def test_null_amount_preserved(self, sample_bronze_df):
        result = cast_and_clean(sample_bronze_df)
        null_count = result.filter(F.col("amount").isNull()).count()
        assert null_count == 1   # ORD-003 has null amount


# ── normalise_currency ────────────────────────────────────────────────────────

class TestNormaliseCurrency:
    def test_usd_unchanged(self, spark, sample_bronze_df):
        df = cast_and_clean(sample_bronze_df)
        df = normalise_currency(df)
        usd_row = df.filter(F.col("order_id") == "ORD-001").first()
        assert usd_row["amount_usd"] == 150.00

    def test_gbp_converted(self, spark, sample_bronze_df):
        df = cast_and_clean(sample_bronze_df)
        df = normalise_currency(df)
        gbp_row = df.filter(F.col("order_id") == "ORD-002").first()
        expected = round(200.00 * 1.27, 2)
        assert gbp_row["amount_usd"] == expected, f"Expected {expected}, got {gbp_row['amount_usd']}"

    def test_null_amount_stays_null(self, spark, sample_bronze_df):
        df = cast_and_clean(sample_bronze_df)
        df = normalise_currency(df)
        null_row = df.filter(F.col("order_id") == "ORD-003").first()
        assert null_row["amount_usd"] is None


# ── deduplicate ───────────────────────────────────────────────────────────────

class TestDeduplicate:
    def test_duplicate_removed(self, spark, sample_bronze_df):
        df = cast_and_clean(sample_bronze_df)
        df = normalise_currency(df)
        before = df.count()
        after  = deduplicate(df).count()
        # ORD-001 appears twice → should be 3 unique rows
        assert after == 3, f"Expected 3 rows after dedup, got {after}"

    def test_order_id_unique_after_dedup(self, spark, sample_bronze_df):
        df = cast_and_clean(sample_bronze_df)
        df = normalise_currency(df)
        df = deduplicate(df)
        total  = df.count()
        unique = df.select("order_id").distinct().count()
        assert total == unique


# ── apply_filters ─────────────────────────────────────────────────────────────

class TestApplyFilters:
    def test_null_amount_usd_removed(self, spark, sample_bronze_df):
        df = cast_and_clean(sample_bronze_df)
        df = normalise_currency(df)
        df = deduplicate(df)
        filtered = apply_filters(df)
        null_count = filtered.filter(F.col("amount_usd").isNull()).count()
        assert null_count == 0

    def test_row_count_reduced(self, spark, sample_bronze_df):
        df = cast_and_clean(sample_bronze_df)
        df = normalise_currency(df)
        df = deduplicate(df)
        before   = df.count()
        filtered = apply_filters(df)
        after    = filtered.count()
        assert after <= before


# ── quality checks ────────────────────────────────────────────────────────────

class TestQualityChecks:
    @pytest.fixture
    def silver_like_df(self, spark, sample_bronze_df):
        df = cast_and_clean(sample_bronze_df)
        df = normalise_currency(df)
        df = deduplicate(df)
        df = apply_filters(df)
        return df.withColumn("order_date", F.to_date("event_ts")) \
                 .withColumn("processed_at", F.current_timestamp())

    def test_null_completeness_returns_dict(self, silver_like_df):
        result = check_null_completeness(silver_like_df)
        assert "check_name" in result
        assert "score" in result
        assert "status" in result
        assert 0.0 <= result["score"] <= 1.0

    def test_amount_range_passes_for_valid_data(self, silver_like_df):
        result = check_amount_range(silver_like_df)
        # All remaining rows have valid amounts (null was filtered out)
        assert result["score"] == 1.0

    def test_duplicate_check_returns_pass(self, silver_like_df):
        result = check_duplicate_orders(silver_like_df)
        # After dedup, all order_ids are unique → score = 1.0
        assert result["score"] == 1.0

    def test_schema_conformance_detects_missing_col(self, spark, silver_like_df):
        # Drop a required column and verify detection
        df_missing = silver_like_df.drop("amount_usd")
        result = check_schema_conformance(df_missing)
        assert result["status"] == "fail"
        assert "amount_usd" in result["details"]
