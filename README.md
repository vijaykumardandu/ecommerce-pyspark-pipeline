# ecommerce-pyspark-pipeline

I built this project while preparing for data engineering interviews. I had been reading
about Medallion architecture for a while and wanted to actually implement it end-to-end
rather than just knowing it theoretically. The e-commerce domain made sense because the
data problems are familiar — multiple source systems, currency differences, duplicate
events from mobile clients, late-arriving records.

The pipeline runs fully on a local machine. No cloud account needed to try it out.

---

## What it does

Takes raw sales data coming in from three channels — a web store, a mobile app, and
physical in-store POS terminals — and processes it through three layers:

- **Bronze** stores everything exactly as received, no changes, so there is always a way
  to go back and reprocess if something breaks downstream
- **Silver** is where the actual cleaning happens — fixing types, converting currencies
  to USD, removing duplicates, dropping rows that make no business sense (negative
  amounts, orders dated in the future)
- **Gold** is what an analyst or BI tool would actually query — daily revenue broken down
  by region, channel, and product category, plus a customer-level table with lifetime
  value and purchase history

There are also six data quality checks that run automatically after Silver is built.
One of them (timestamp freshness) flags historical backfill orders as "late" — which
is technically correct but not actually a data problem. I kept it in because that kind
of nuance is worth documenting.

---

## Stack

| What | Why I chose it |
|------|---------------|
| PySpark 3.5 | Industry standard for large-scale transformations |
| Delta Lake 3.0 | ACID transactions + time travel on local storage |
| Apache Airflow 2.8 | Scheduling — the DAG is included even though I run manually locally |
| Faker + pandas | Synthetic data generation — fast to set up, realistic enough |
| pytest | Unit tests for each transformation function |
| Python 3.10 | Stable, wide library support |

---

## Folder structure

```
ecommerce-pyspark-pipeline/
│
├── config/
│   └── settings.py          ← paths, quality thresholds, FX rates all in one place
│
├── src/
│   ├── generate_data.py     ← creates the fake source files in data/raw/
│   ├── bronze_ingest.py     ← reads raw files, tags with source + ingestion time
│   ├── silver_transform.py  ← cleaning, currency normalisation, dedup, MERGE
│   ├── quality_checks.py    ← 6 checks, writes results to a Delta quality_log table
│   ├── gold_aggregate.py    ← daily_revenue and customer_metrics tables
│   └── utils.py             ← SparkSession setup, shared logging
│
├── tests/
│   └── test_transforms.py   ← 12 unit tests covering the Silver transformations
│
├── dags/
│   └── ecommerce_pipeline_dag.py  ← Airflow DAG, runs daily at 2am
│
├── notebooks/
│   └── pipeline_walkthrough.ipynb ← step by step notebook version
│
├── run_pipeline.py          ← single script to run everything in order
└── requirements.txt
```

---

## Running it locally

You need Python 3.10+ and Java 11 or 17. Java is required by Spark — I caught this
the hard way when I got a vague JVM error on first run. Check with `java -version`
before installing anything else.

```bash
# 1. install dependencies
pip install -r requirements.txt

# 2. run the full pipeline
python run_pipeline.py
```

That runs all five stages: data generation → Bronze → Silver → quality checks → Gold.
Takes about 3 to 5 minutes depending on your machine.

If you want to run just one stage:

```bash
python run_pipeline.py --only bronze
python run_pipeline.py --only silver
python run_pipeline.py --only quality
python run_pipeline.py --only gold

# skip data generation if you already ran it once
python run_pipeline.py --skip-gen
```

---

## Running the tests

```bash
pytest tests/ -v

# with coverage
pytest tests/ -v --cov=src --cov-report=term-missing
```

12 tests covering type casting, currency conversion, deduplication logic, filter
behaviour, and four of the quality check functions.

---

## Data quality results

These run automatically as part of the pipeline. Results get written to a
`quality_log` Delta table so you can query them later.

| Check | What it looks at | Threshold | Typical result |
|-------|-----------------|-----------|----------------|
| Null completeness | order_id, customer_id, amount_usd | 95% | ~99% pass |
| Amount range | 0.01 to 100,000 USD | 95% | ~94% warn |
| Duplicate detection | order_id uniqueness | 97% | ~97% pass |
| Referential integrity | customer_id UUID format | 95% | ~98% pass |
| Schema conformance | all expected columns present | 100% | 100% pass |
| Timestamp freshness | event within 2h of ingestion | 90% | ~89% warn |

The timestamp freshness warning is expected. The synthetic data generates orders
spread over the past 90 days — those historical records will always look "late"
relative to when they were ingested. In a real pipeline I would set per-source
SLA windows rather than one global threshold. The affected rows get tagged with
a `source_lag_bucket` column rather than dropped.

The amount range warning comes from the injected outliers in the test data. In
production this would trigger an alert to investigate the source system.

---

## Things I would do differently with more time

- Replace the batch CSV source with a Kafka consumer to make it a proper streaming
  pipeline. The Bronze layer is already append-only so it would slot in cleanly.
- Add a Streamlit dashboard on top of the Gold tables instead of just printing
  aggregates to console.
- The currency FX rates are hardcoded in settings.py. They should come from an
  API or at least a config file that gets updated regularly.
- Write integration tests that spin up a real SparkSession and test the full
  Bronze to Silver flow, not just individual functions.
- The Airflow DAG is included but I have not fully tested it end-to-end with a
  live Airflow instance. I have been using `run_pipeline.py` directly.

---
