"""Incremental FRED observations ingestion; local state for demonstration."""
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import requests

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
SERIES = "MORTGAGE30US"


def record_key(series_id: str, observation_date: str) -> str:
    return hashlib.sha256(f"{series_id}|{observation_date}".encode()).hexdigest()


def normalize(observations: list[dict], series_id: str = SERIES) -> list[dict]:
    rows = []
    for obs in observations:
        value = obs.get("value")
        if value in (None, ".", ""):
            continue
        day = date.fromisoformat(obs["date"]).isoformat()
        rows.append({"record_key": record_key(series_id, day), "series_id": series_id,
                     "observation_date": day, "value": float(value)})
    return rows


def fetch_observations(api_key: str, start: date, end: date, session=None) -> list[dict]:
    client = session or requests.Session()
    response = client.get(FRED_URL, params={"series_id": SERIES, "api_key": api_key,
        "file_type": "json", "observation_start": start.isoformat(),
        "observation_end": end.isoformat()}, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if "observations" not in payload:
        raise ValueError(f"Unexpected FRED response: {list(payload)}")
    return normalize(payload["observations"])


def read_checkpoint(path: Path) -> date | None:
    if not path.exists():
        return None
    return date.fromisoformat(json.loads(path.read_text())["last_successful_end"])


def window(checkpoint: date | None, today: date, lookback_days: int = 35) -> tuple[date, date]:
    if lookback_days < 0:
        raise ValueError("lookback_days must be nonnegative")
    start = checkpoint - timedelta(days=lookback_days) if checkpoint else today - timedelta(days=365)
    return min(start, today), today


def write_checkpoint(path: Path, end: date) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps({"last_successful_end": end.isoformat()}))
    temp.replace(path)
