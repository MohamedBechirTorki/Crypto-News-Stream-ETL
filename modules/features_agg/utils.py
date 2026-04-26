import os
import json
import logging
import statistics
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


def load_enriched(path: str) -> tuple[list, datetime | None]:
    if not os.path.exists(path):
        return [], None

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # Extract ingestion_time
    ingestion_time = None
    if isinstance(raw, dict):
        raw_time = raw.get("ingestion_time")
        if raw_time:
            try:
                dt = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
                ingestion_time = dt.astimezone(timezone.utc)
            except Exception:
                pass
        articles = enforce_dict_list(raw.get("articles", []))
    else:
        articles = enforce_dict_list(raw)

    return articles, ingestion_time

def enforce_dict_list(data):
    fixed = []
    for x in data:
        if isinstance(x, dict):
            fixed.append(x)
        elif isinstance(x, str):
            try:
                fixed.append(json.loads(x))
            except:
                continue
    return fixed

def load_json(path: str) -> list:
    if not os.path.exists(path):
        logger.warning(f"File not found: {path}")
        return []

    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    try:
        data = json.loads(raw)
    except Exception as e:
        logger.error(f"JSON parse error: {e}")
        return []

    # Handle wrapper format {"ingestion_time": "...", "articles": [...]}
    if isinstance(data, dict) and "articles" in data:
        return data["articles"]

    # Handle flat list (old format)
    if isinstance(data, list):
        return data

    return []

def safe_mean(values: list) -> float:
    return round(statistics.mean(values), 4) if values else 0.0


def safe_std(values: list) -> float:
    return round(statistics.stdev(values), 4) if len(values) >= 2 else 0.0



def get_window_start(enriched: list) -> datetime | None:
    times = []

    for a in enriched:
        if isinstance(a, str):
            try:
                a = json.loads(a)
            except Exception:
                continue
        if not isinstance(a, dict):
            continue

        raw = a.get("ingestion_time")
        if not raw:
            continue

        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            times.append(dt)
        except Exception:
            continue

    if not times:
        return None

    # Use earliest ingestion time — most conservative window anchor
    earliest = min(times)

    # Floor to the hour then subtract 1h
    floored = earliest.replace(minute=0, second=0, microsecond=0)
    return floored - timedelta(hours=1)

def extract_sentiment(a: dict) -> float:
    if isinstance(a, str):
        try:
            a = json.loads(a)
        except Exception:
            return 0.0

    if not isinstance(a, dict):
        return 0.0

    if isinstance(a.get("final_sentiment_score"), (int, float)):
        return a["final_sentiment_score"]
    if isinstance(a.get("headline_sentiment_score"), (int, float)):
        return a["headline_sentiment_score"]
    return 0.0