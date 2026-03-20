import os

# ── Paths (all local, relative to project root) ──────────────────────────────
BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR        = os.path.join(BASE_DIR, "data")
RAW_DIR         = os.path.join(DATA_DIR, "raw")
DELTA_DIR       = os.path.join(DATA_DIR, "delta")

BRONZE_PATH     = os.path.join(DELTA_DIR, "bronze", "orders")
SILVER_PATH     = os.path.join(DELTA_DIR, "silver", "orders")
GOLD_PATH       = os.path.join(DELTA_DIR, "gold",   "daily_revenue")
QUALITY_LOG     = os.path.join(DELTA_DIR, "quality_log")

# ── Source file paths ─────────────────────────────────────────────────────────
WEB_ORDERS_PATH    = os.path.join(RAW_DIR, "web_orders.json")
MOBILE_EVENTS_PATH = os.path.join(RAW_DIR, "mobile_events.csv")
INSTORE_PATH       = os.path.join(RAW_DIR, "instore_pos.csv")

# ── Data generation settings ──────────────────────────────────────────────────
WEB_RECORDS     = 50_000   # increase to 200_000 for production-feel
MOBILE_RECORDS  = 50_000
INSTORE_RECORDS = 25_000

# ── Data quality thresholds ───────────────────────────────────────────────────
NULL_COMPLETENESS_THRESHOLD   = 0.95
AMOUNT_RANGE_THRESHOLD        = 0.95
DUPLICATE_THRESHOLD           = 0.97
REFERENTIAL_INT_THRESHOLD     = 0.95
SCHEMA_CONFORMANCE_THRESHOLD  = 1.00
TIMESTAMP_FRESHNESS_THRESHOLD = 0.90

# ── Currency FX rates (USD base) ──────────────────────────────────────────────
FX_RATES = {
    "USD": 1.00,
    "GBP": 1.27,
    "EUR": 1.09,
    "INR": 0.012,
}

# ── Spark config ──────────────────────────────────────────────────────────────
SPARK_APP_NAME  = "EcommercePipeline"
SPARK_MASTER    = "local[*]"
