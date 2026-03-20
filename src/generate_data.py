"""
generate_data.py
----------------
Generates synthetic e-commerce order data from three sources:
  • web_orders.json   (web channel)
  • mobile_events.csv (mobile channel)
  • instore_pos.csv   (in-store POS channel)

Run:
    python src/generate_data.py
"""
import os
import sys
import uuid
import random
import logging
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    RAW_DIR, WEB_ORDERS_PATH, MOBILE_EVENTS_PATH, INSTORE_PATH,
    WEB_RECORDS, MOBILE_RECORDS, INSTORE_RECORDS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger("generate_data")

fake = Faker()
Faker.seed(42)
random.seed(42)

CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Beauty", "Toys"]
REGIONS    = ["North", "South", "East", "West", "Central"]
CURRENCIES = ["USD", "USD", "USD", "GBP", "EUR", "INR"]   # weighted toward USD


def _random_event_time(days_back: int = 90) -> str:
    """Return a random timestamp within the last N days."""
    delta = timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return (datetime.now() - delta).strftime("%Y-%m-%d %H:%M:%S")


def make_orders(n: int, channel: str) -> pd.DataFrame:
    """
    Generate n synthetic order rows.

    Intentional data quality issues injected to demonstrate the pipeline:
      - 2 % null amounts           → caught by null_completeness check
      - 3 % duplicate order_ids   → caught by duplicate_detection check
      - 1 % future timestamps      → caught by timestamp_freshness check
      - Mix of currencies          → handled by currency normalisation
    """
    rows = []
    order_ids = [str(uuid.uuid4()) for _ in range(n)]

    # inject duplicates: repeat ~3 % of IDs
    dup_count = int(n * 0.03)
    dup_ids   = random.choices(order_ids[: n // 2], k=dup_count)
    order_ids.extend(dup_ids)
    random.shuffle(order_ids)
    order_ids = order_ids[:n]   # keep total at n

    for oid in order_ids:
        # inject null amounts (~2 %)
        amount = (
            None
            if random.random() < 0.02
            else round(random.uniform(5.0, 2000.0), 2)
        )

        # inject future timestamps (~1 %)
        if random.random() < 0.01:
            event_time = (datetime.now() + timedelta(hours=random.randint(1, 48))).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        else:
            event_time = _random_event_time()

        rows.append(
            {
                "order_id":         oid,
                "customer_id":      str(uuid.uuid4()),
                "amount":           amount,
                "currency":         random.choice(CURRENCIES),
                "product_category": random.choice(CATEGORIES),
                "product_id":       f"PROD-{random.randint(1000, 9999)}",
                "region":           random.choice(REGIONS),
                "channel":          channel,
                "is_return":        random.random() < 0.05,  # 5 % returns
                "event_time":       event_time,
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)

    logger.info("Generating %d web orders …", WEB_RECORDS)
    web_df = make_orders(WEB_RECORDS, "web")
    web_df.to_json(WEB_ORDERS_PATH, orient="records", indent=2)
    logger.info("  Saved → %s  (%d rows)", WEB_ORDERS_PATH, len(web_df))

    logger.info("Generating %d mobile events …", MOBILE_RECORDS)
    mob_df = make_orders(MOBILE_RECORDS, "mobile")
    mob_df.to_csv(MOBILE_EVENTS_PATH, index=False)
    logger.info("  Saved → %s  (%d rows)", MOBILE_EVENTS_PATH, len(mob_df))

    logger.info("Generating %d in-store POS records …", INSTORE_RECORDS)
    ins_df = make_orders(INSTORE_RECORDS, "instore")
    ins_df.to_csv(INSTORE_PATH, index=False)
    logger.info("  Saved → %s  (%d rows)", INSTORE_PATH, len(ins_df))

    total = len(web_df) + len(mob_df) + len(ins_df)
    logger.info("Done. Total records generated: %d", total)


if __name__ == "__main__":
    main()
