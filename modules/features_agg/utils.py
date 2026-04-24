import os
import json
import logging
import statistics
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


def load_json(path: str) -> list:
    if not os.path.exists(path):
        logger.warning(f"File not found: {path} — returning empty list")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_mean(values: list) -> float:
    return round(statistics.mean(values), 4) if values else 0.0


def safe_std(values: list) -> float:
    return round(statistics.stdev(values), 4) if len(values) >= 2 else 0.0


def get_window_start(enriched: list) -> str:
    times = []
    for a in enriched:
        raw = a.get("ingestion_time")
        if raw:
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                times.append(dt)
            except Exception:
                pass

    if times:
        earliest = min(times)
        window_start = earliest - timedelta(hours=1)
    else:
        window_start = datetime.now(timezone.utc) - timedelta(hours=1)

    return window_start.isoformat()


def extract_sentiment(a: dict) -> float:
    if isinstance(a.get("final_sentiment_score"), (int, float)):
        return a["final_sentiment_score"]
    if isinstance(a.get("headline_sentiment_score"), (int, float)):
        return a["headline_sentiment_score"]
    return 0.0