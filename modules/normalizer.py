import os
import json
import hashlib
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

RAW_PATH = os.path.join(REPO_ROOT, "data", "raw", "raw.json")
NORMALIZED_PATH = os.path.join(REPO_ROOT, "data", "normalized", "data.json")

def strip_html(text):
    return BeautifulSoup(text or "", "html.parser").get_text(" ").strip()

def normalize_whitespace(text):
    return re.sub(r"\s+", " ", text).strip()

def parse_date(ts):
    try:
        dt = dateutil_parser.parse(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except:
        return None

def hash_text(text):
    return hashlib.sha256(text.encode()).hexdigest()

def normalize_article(raw, ingestion_time):
    title = normalize_whitespace(strip_html(raw.get("title")))
    content = normalize_whitespace(strip_html(raw.get("content")))
    url = raw.get("url", "").strip()

    if not url or not title:
        return None

    published = parse_date(raw.get("raw_published_at")) or ingestion_time

    return {
        "id": hash_text(url),
        "title": title[:500],
        "content": content[:5000],
        "url": url,
        "source": raw.get("source"),
        "published_at": published.isoformat(),
        "ingestion_time": ingestion_time.isoformat(),
        "content_hash": hash_text(content),
        "text_length": len(content),
        "title_length": len(title),
    }

def run_normalization():
    with open(RAW_PATH, "r", encoding="utf-8") as f:
        raw_articles = json.load(f)

    ingestion_time = datetime.now(timezone.utc)

    normalized = []
    for r in raw_articles:
        n = normalize_article(r, ingestion_time)
        if n:
            normalized.append(n)

    os.makedirs(os.path.dirname(NORMALIZED_PATH), exist_ok=True)

    with open(NORMALIZED_PATH, "w", encoding="utf-8") as f:
        json.dump(normalized, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved NORMALIZED → {NORMALIZED_PATH}")
    return NORMALIZED_PATH


if __name__ == "__main__":
    run_normalization()