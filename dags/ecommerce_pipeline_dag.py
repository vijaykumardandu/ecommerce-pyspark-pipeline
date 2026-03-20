"""
ecommerce_pipeline_dag.py
-------------------------
Airflow DAG — schedules the pipeline to run daily at 02:00 UTC.

To use:
  1. Install Airflow: pip install apache-airflow
  2. Copy this file to your Airflow dags/ folder
  3. Set PIPELINE_DIR env var to your project root
  4. Start Airflow: airflow standalone

DAG graph:
  bronze_ingest → silver_transform → quality_checks → gold_aggregate
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# Make sure the project is on the Python path
PIPELINE_DIR = os.environ.get("PIPELINE_DIR", "/path/to/ecommerce-pyspark-pipeline")
if PIPELINE_DIR not in sys.path:
    sys.path.insert(0, PIPELINE_DIR)

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}


def _run_bronze(**ctx):
    from src.bronze_ingest import run_bronze
    run_bronze()


def _run_silver(**ctx):
    from src.silver_transform import run_silver
    run_silver()


def _run_quality(**ctx):
    from src.quality_checks import run_quality
    run_quality()


def _run_gold(**ctx):
    from src.gold_aggregate import run_gold
    run_gold()


def _branch_on_quality(**ctx):
    """Branch: proceed to Gold only if quality log shows no failures."""
    import json
    ti = ctx["ti"]
    # In a real pipeline you'd push quality results via XCom
    # Here we always proceed — replace with real check if needed
    return "gold_aggregate"


with DAG(
    dag_id="ecommerce_pyspark_pipeline",
    description="E-Commerce Sales Data Pipeline — Bronze → Silver → Quality → Gold",
    schedule_interval="0 2 * * *",      # daily at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "pyspark", "delta-lake"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    t_bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=_run_bronze,
        doc_md="Reads raw source files and writes to Bronze Delta table.",
    )

    t_silver = PythonOperator(
        task_id="silver_transform",
        python_callable=_run_silver,
        doc_md="Cleanses, normalises, and deduplicates Bronze → Silver.",
    )

    t_quality = PythonOperator(
        task_id="quality_checks",
        python_callable=_run_quality,
        doc_md="Runs 6 data quality checks and writes results to quality_log.",
    )

    t_gold = PythonOperator(
        task_id="gold_aggregate",
        python_callable=_run_gold,
        doc_md="Aggregates Silver into business-ready Gold tables.",
    )

    # Linear dependency chain
    start >> t_bronze >> t_silver >> t_quality >> t_gold >> end

