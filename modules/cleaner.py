import os
import json
from datetime import datetime, timedelta, timezone
from dateutil import parser as dateutil_parser
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INPUT_PATH = os.path.join(REPO_ROOT, "data", "normalized", "data.json")
OUTPUT_PATH = os.path.join(REPO_ROOT, "data", "cleaned", "data.json")


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


def quality_filter(a):
    if a["text_length"] < 100:
        return False
    if a["title_length"] < 10:
        return False
    return True


def run_cleaning():
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        articles = json.load(f)

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=6)

    filtered = []

    for a in articles:
        pub = dateutil_parser.parse(a["published_at"])

        if is_in_window(pub, start, now) and quality_filter(a):
            filtered.append(a)

    cleaned = deduplicate(filtered)
    for i in range(len(cleaned)) :
        print(cleaned[i]["title"])
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved CLEANED → {OUTPUT_PATH}")
    return OUTPUT_PATH


if __name__ == "__main__":
    run_cleaning()