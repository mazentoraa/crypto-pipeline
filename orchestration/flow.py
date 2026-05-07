"""
orchestration/flow.py
──────────────────────
Prefect 2.x workflow that orchestrates the full data pipeline.

Task dependency graph:
    ingest_crypto_prices()
           │
           ▼ (runs only after ingestion succeeds)
    run_data_transformations()

Schedule: every 5 minutes (configurable via FLOW_INTERVAL_MINUTES).

To run once manually:
    python -m orchestration.flow --once

To start the scheduled deployment:
    python -m orchestration.flow
"""

import argparse
from datetime import timedelta

from prefect import flow, task, get_run_logger
from prefect.tasks import exponential_backoff

from ingestion.batch_ingest import run_batch_ingestion
from transformation.transform import run_transformations
from utils.logger import get_logger

# Fallback logger (Prefect's run logger is only available inside task/flow context)
logger = get_logger(__name__)

FLOW_INTERVAL_MINUTES = 5


# ─────────────────────────────────────────────────────────────────────────────
# TASKS
# ─────────────────────────────────────────────────────────────────────────────

@task(
    name           = "batch-crypto-ingestion",
    description    = "Fetch live crypto prices from CoinGecko and store in PostgreSQL.",
    retries        = 3,
    retry_delay_seconds = exponential_backoff(backoff_factor=2),
    tags           = ["ingestion", "coingecko"],
)
def ingest_crypto_prices_task() -> int:
    """
    Prefect task wrapper around run_batch_ingestion().
    Retries up to 3 times with exponential back-off (2s, 4s, 8s).
    Returns the number of rows inserted.
    """
    pf_logger = get_run_logger()
    pf_logger.info("Task: batch crypto ingestion starting")

    rows = run_batch_ingestion()

    pf_logger.info(f"Task: batch crypto ingestion complete — {rows} rows inserted")
    return rows


@task(
    name           = "data-transformation",
    description    = "Clean, deduplicate, and aggregate stored data.",
    retries        = 2,
    retry_delay_seconds = exponential_backoff(backoff_factor=2),
    tags           = ["transformation", "pandas"],
)
def run_transformation_task() -> dict:
    """
    Prefect task wrapper around run_transformations().
    Retries up to 2 times.
    Returns a summary dict with row counts.
    """
    pf_logger = get_run_logger()
    pf_logger.info("Task: data transformation starting")

    prices_df, news_df = run_transformations()

    result = {
        "price_rows": len(prices_df),
        "news_rows":  len(news_df),
    }
    pf_logger.info(f"Task: transformation complete — {result}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# FLOW
# ─────────────────────────────────────────────────────────────────────────────

@flow(
    name        = "crypto-analytics-pipeline",
    description = "End-to-end pipeline: ingest crypto prices → transform data.",
    log_prints  = True,
)
def main_flow() -> dict:
    """
    Main Prefect flow.

    Execution order (Prefect respects Python data dependencies):
      1. ingest_crypto_prices_task()  — must finish first
      2. run_transformation_task()    — receives ingestion result as input signal

    If step 1 fails all retries, step 2 is never executed.
    """
    logger.info("Main flow started")

    # Step 1: Ingest raw data
    ingestion_result = ingest_crypto_prices_task()

    # Step 2: Transform (passing result creates an explicit dependency edge)
    transformation_result = run_transformation_task()

    summary = {
        "ingestion_rows": ingestion_result,
        **transformation_result,
    }

    logger.info("Main flow completed", **summary)
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the crypto analytics pipeline")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run the flow a single time and exit (no schedule)",
    )
    args = parser.parse_args()

    if args.once:
        # Run immediately, once
        result = main_flow()
        print(f"Flow finished: {result}")
    else:
        # Deploy with a repeating schedule via Prefect's built-in serve()
        print(f"Starting scheduled flow (every {FLOW_INTERVAL_MINUTES} minutes)...")
        print("Press Ctrl+C to stop.")
        main_flow.serve(
            name     = "crypto-analytics-pipeline-deployment",
            interval = timedelta(minutes=FLOW_INTERVAL_MINUTES),
        )