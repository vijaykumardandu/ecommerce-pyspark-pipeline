"""
run_pipeline.py
---------------
Master script — runs the full pipeline end-to-end in order:

  Step 1  generate_data   → creates raw source files in data/raw/
  Step 2  bronze_ingest   → reads raw files → Bronze Delta table
  Step 3  silver_transform→ cleans + normalises → Silver Delta table
  Step 4  quality_checks  → validates Silver → quality_log Delta table
  Step 5  gold_aggregate  → aggregates Silver → Gold Delta tables

Usage:
    python run_pipeline.py              # run all steps
    python run_pipeline.py --skip-gen   # skip data generation (if data/raw already exists)
"""
import sys
import time
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")

DIVIDER = "─" * 60


def step(name: str, fn, *args, **kwargs):
    logger.info(DIVIDER)
    logger.info("▶  %s", name)
    logger.info(DIVIDER)
    t0 = time.time()
    fn(*args, **kwargs)
    elapsed = time.time() - t0
    logger.info("✓  %s completed in %.1fs\n", name, elapsed)


def main():
    parser = argparse.ArgumentParser(description="E-Commerce PySpark Pipeline")
    parser.add_argument(
        "--skip-gen", action="store_true",
        help="Skip synthetic data generation (use existing files in data/raw/)"
    )
    parser.add_argument(
        "--only", choices=["generate", "bronze", "silver", "quality", "gold"],
        help="Run only one specific stage"
    )
    args = parser.parse_args()

    t_start = time.time()

    from src.generate_data    import main as generate_data
    from src.bronze_ingest    import run_bronze
    from src.silver_transform import run_silver
    from src.quality_checks   import run_quality
    from src.gold_aggregate   import run_gold

    if args.only:
        stage_map = {
            "generate": ("Generate synthetic data", generate_data),
            "bronze":   ("Bronze — raw ingestion",   run_bronze),
            "silver":   ("Silver — cleanse & transform", run_silver),
            "quality":  ("Quality checks",           run_quality),
            "gold":     ("Gold — business aggregates", run_gold),
        }
        name, fn = stage_map[args.only]
        step(name, fn)
    else:
        if not args.skip_gen:
            step("Step 1 — Generate synthetic data", generate_data)
        else:
            logger.info("Skipping data generation (--skip-gen flag set)")

        step("Step 2 — Bronze layer (raw ingestion)",       run_bronze)
        step("Step 3 — Silver layer (cleanse & transform)", run_silver)
        step("Step 4 — Data quality checks",                run_quality)
        step("Step 5 — Gold layer (business aggregates)",   run_gold)

    total = time.time() - t_start
    logger.info(DIVIDER)
    logger.info("Pipeline complete in %.1f seconds", total)
    logger.info(DIVIDER)


if __name__ == "__main__":
    main()
