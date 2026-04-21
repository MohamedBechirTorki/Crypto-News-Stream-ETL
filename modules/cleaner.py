import os
import json
import hashlib
import re
import logging
from datetime import datetime, timedelta, timezone

from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------
# Paths
# ----------------------------
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

RAW_PATH = os.path.join(REPO_ROOT, "data", "raw", "raw.json")
OUTPUT_PATH = os.path.join(REPO_ROOT, "data", "cleaned", "data.json")

# ----------------------------
# Utils
# ----------------------------
def strip_html(text):
    return BeautifulSoup(text or "", "html.parser").get_text(" ").strip()

def normalize_whitespace(text):
    return re.sub(r"\s+", " ", text).strip()

def hash_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def parse_date(ts):
    try:
        dt = dateutil_parser.parse(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except:
        return None

# ----------------------------
# Normalization
# ----------------------------
def normalize_article(raw, ingestion_time):
    title = normalize_whitespace(strip_html(raw.get("title")))
    content = normalize_whitespace(strip_html(raw.get("content", "")))
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

def normalize(raw_articles):
    ingestion_time = datetime.now(timezone.utc)
    normalized = []

    for r in raw_articles:
        n = normalize_article(r, ingestion_time)
        if n:
            normalized.append(n)

    return normalized

# ----------------------------
# Cleaning
# ----------------------------
def is_in_window(published, start, end):
    return start <= published < end

def deduplicate(articles):
    seen = set()
    unique = []

    for a in articles:
        if a["id"] not in seen:
            seen.add(a["id"])
            unique.append(a)

    return unique

def clean(articles):
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=1)

    filtered = []
    for a in articles:
        pub = dateutil_parser.parse(a["published_at"])
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)

        if is_in_window(pub, start, now):
            filtered.append(a)

    return deduplicate(filtered)

# ----------------------------
# Pipeline
# ----------------------------
def run_cleaning():
    with open(RAW_PATH, "r", encoding="utf-8") as f:
        raw_articles = json.load(f)

    logger.info(f"Loaded {len(raw_articles)} raw articles")

    normalized = normalize(raw_articles)
    logger.info(f"Normalized → {len(normalized)} articles")

    cleaned = clean(normalized)
    logger.info(f"Cleaned → {len(cleaned)} articles")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved FINAL OUTPUT → {OUTPUT_PATH}")

    return OUTPUT_PATH


if __name__ == "__main__":
    run_cleaning()