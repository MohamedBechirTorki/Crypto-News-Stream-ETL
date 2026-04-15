import os
import json
import hashlib
import feedparser
import logging
from datetime import datetime, timezone, timedelta
from dateutil import parser as dateutil_parser
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_RAW_DIR = os.path.join(REPO_ROOT, "data", "raw")

def resolve_data_raw_path(filename: str) -> str:
    return os.path.join(DATA_RAW_DIR, os.path.basename(filename))

# RSS SOURCES
RSS_FEEDS = {
    "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph": "https://cointelegraph.com/rss",
    "decrypt": "https://decrypt.co/feed",
    "bitcoinist": "https://bitcoinist.com/feed/",
    "cryptoslate": "https://cryptoslate.com/feed/",
    "newsbtc": "https://www.newsbtc.com/feed/",
    "theblock": "https://www.theblock.co/rss.xml",
    "bitcoin_mag": "https://bitcoinmagazine.com/.rss/full/",
    "cryptopanic": "https://cryptopanic.com/news/rss/",
    "coinjournal": "https://coinjournal.net/feed/",
    "investing_crypto": "https://www.investing.com/rss/news_301.rss",
    "seeking_alpha": "https://seekingalpha.com/tag/cryptocurrency.xml",
}

# WINDOW HELPERS

def get_current_window(execution_dt: datetime = None) -> tuple[datetime, datetime]:
    """
    Rolling 6-hour window:
    [execution_time - 6h, execution_time]
    """
    if execution_dt is None:
        execution_dt = datetime.now(timezone.utc)

    window_end = execution_dt
    window_start = execution_dt - timedelta(hours=6)

    return window_start, window_end

def parse_published_at(raw_timestamp: str) -> datetime | None:
    if not raw_timestamp:
        return None
    try:
        dt = dateutil_parser.parse(raw_timestamp)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def is_in_window(published_at, window_start, window_end, grace_minutes=30):
    return (window_start - timedelta(minutes=grace_minutes)) <= published_at < window_end

# CLEANING
def strip_html(text: str) -> str:
    if not text:
        return ""
    return BeautifulSoup(text, "html.parser").get_text(separator=" ").strip()

def normalize_whitespace(text: str) -> str:
    import re
    return re.sub(r"\s+", " ", text).strip()

def make_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

# FETCH SINGLE FEED
def fetch_rss_feed(source_name: str, url: str, limit: int = 50) -> list[dict]:
    try:
        response = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0"
        })

        if response.status_code != 200:
            logger.warning(f"[RSS] {source_name}: HTTP {response.status_code}")
            return []

        feed = feedparser.parse(response.content)

        if feed.bozo and not feed.entries:
            logger.warning(f"[RSS] {source_name}: malformed feed, skipping")
            return []

        articles = []

        for entry in feed.entries[:limit]:
            content = (
                entry.get("content", [{}])[0].get("value", "")
                or entry.get("summary", "")
                or entry.get("description", "")
            )

            content = content[:20000]

            raw_ts = entry.get("published") or entry.get("updated")

            articles.append({
                "title": entry.get("title", "").strip(),
                "content": content,
                "source": source_name,
                "url": entry.get("link", ""),
                "raw_published_at": raw_ts,
            })

        logger.info(f"[RSS] {source_name}: fetched {len(articles)} articles")
        return articles

    except requests.exceptions.Timeout:
        logger.warning(f"[RSS] {source_name}: TIMEOUT")
        return []

    except Exception as e:
        logger.error(f"[RSS] {source_name} failed: {e}")
        return []
# PARALLEL FETCH
def fetch_all_rss(limit_per_feed: int = 50) -> list[dict]:
    all_articles = []
    failed_sources = []

    def worker(item):
        name, url = item
        return name, fetch_rss_feed(name, url, limit_per_feed)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = executor.map(worker, RSS_FEEDS.items())

    for name, articles in results:
        if not articles:
            failed_sources.append(name)
        all_articles.extend(articles)

    logger.info(f"[RSS] total fetched: {len(all_articles)} | failed: {failed_sources}")
    return all_articles

# NORMALIZATION
def normalize_article(raw: dict, ingestion_time: datetime) -> dict | None:
    url = raw.get("url", "").strip()
    title = normalize_whitespace(strip_html(raw.get("title", "")))
    content = normalize_whitespace(strip_html(raw.get("content", "")))
    published_at = parse_published_at(raw.get("raw_published_at"))

    if not url or not url.startswith("http"):
        return None
    if not title:
        return None

    if published_at is None:
        published_at = ingestion_time
        fallback = True
    else:
        fallback = False

    return {
        "hash": make_hash(url),
        "title": title[:500],
        "content": content[:5000] if content else None,
        "source": raw.get("source", ""),
        "url": url,
        "published_at": published_at.isoformat(),
        "ingestion_time": ingestion_time.isoformat(),
        "timestamp_is_fallback": fallback,
    }

def normalize_all(raw_articles: list[dict], ingestion_time: datetime) -> list[dict]:
    normalized = []
    for i, raw in enumerate(raw_articles):
        if i % 50 == 0:
            logger.info(f"[NORM] processing {i}/{len(raw_articles)}")

        article = normalize_article(raw, ingestion_time)
        if article:
            normalized.append(article)

    return normalized

# FILTER
def filter_by_window(articles, window_start, window_end):
    in_window = []

    for a in articles:
        published_at = dateutil_parser.parse(a["published_at"])
        if is_in_window(published_at, window_start, window_end):
            in_window.append(a)
        
    return in_window

# DEDUP
def deduplicate(articles):
    seen = set()
    unique = []
    dup = []

    for a in articles:
        h = a["hash"]
        if h in seen:
            dup.append(a)
        else:
            seen.add(h)
            unique.append(a)

    return unique, dup

# SAVE
def save_output(articles, window_start):
    # Folder now includes full timestamp (date + hour)
    folder_name = window_start.strftime("%Y-%m-%d_%H-%M")
    out_dir = os.path.join(DATA_RAW_DIR, folder_name)
    os.makedirs(out_dir, exist_ok=True)

    main_path = os.path.join(out_dir, "articles.json")

    payload = {
        "window_start": window_start.isoformat(),
        "window_end": (window_start + timedelta(hours=6)).isoformat(),
        "article_count": len(articles),
        "articles": articles,
    }

    with open(main_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

# PIPELINE
def run_ingestion(limit_per_feed=50, execution_dt=None):
    ingestion_time = datetime.now(timezone.utc)
    window_start, window_end = get_current_window(execution_dt or ingestion_time)

    logger.info(f"[PIPELINE] window {window_start} → {window_end}")

    logger.info("[PIPELINE] fetching RSS...")
    raw = fetch_all_rss(limit_per_feed)

    logger.info("[PIPELINE] normalizing...")
    norm = normalize_all(raw, ingestion_time)

    logger.info("[PIPELINE] filtering window...")
    in_window = filter_by_window(norm, window_start, window_end)

    logger.info("[PIPELINE] deduplicating...")
    unique, dup = deduplicate(in_window)

    logger.info("[PIPELINE] saving...")
    save_output(unique, window_start)

    logger.info("[PIPELINE] DONE")

    return {
        "fetched": len(raw),
        "normalized": len(norm),
        "in_window": len(in_window),
        "duplicates": len(dup),
        "stored": len(unique),
    }

# MAIN
if __name__ == "__main__":
    stats = run_ingestion(limit_per_feed=100)
    print("\n=== SUMMARY ===")
    for k, v in stats.items():
        print(k, v)