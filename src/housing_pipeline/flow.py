"""Run with: python -m housing_pipeline.flow"""
import logging
import os
from datetime import date
from pathlib import Path
from time import monotonic

from dotenv import load_dotenv
from prefect import flow, task
from .ingest import fetch_observations, read_checkpoint, window, write_checkpoint
from .warehouse import upsert

logger = logging.getLogger(__name__)

@task(retries=3, retry_delay_seconds=15)
def extract(start: date, end: date) -> list[dict]:
    key = os.getenv("FRED_API_KEY")
    if not key:
        raise RuntimeError("Set FRED_API_KEY")
    return fetch_observations(key, start, end)

@task(retries=2, retry_delay_seconds=10)
def load(rows: list[dict]) -> int:
    return upsert(rows)

@flow(name="fred-mortgage-ingestion", log_prints=True)
def ingest_flow() -> dict:
    load_dotenv()
    started = monotonic()
    checkpoint_path = Path(os.getenv("CHECKPOINT_PATH", ".state/fred.json"))
    start, end = window(read_checkpoint(checkpoint_path), date.today(),
                        int(os.getenv("LOOKBACK_DAYS", "35")))
    rows = extract(start, end)
    count = load(rows)
    # Advance only after successful warehouse commit.
    write_checkpoint(checkpoint_path, end)
    metrics = {"source": "fred", "series": "MORTGAGE30US", "window_start": str(start),
               "window_end": str(end), "rows_processed": count,
               "duration_seconds": round(monotonic() - started, 2), "status": "success"}
    logger.info("pipeline_metrics=%s", metrics)
    print(metrics)
    return metrics

if __name__ == "__main__":
    ingest_flow()
