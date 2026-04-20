import os
import json
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import feedparser
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# RSS SOURCES (later move to config/sources.yaml)
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


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_RAW_DIR = os.path.join(REPO_ROOT, "data", "raw")
DATA_RAW_PATH = os.path.join(REPO_ROOT, "data", "raw", "raw.json")


def fetch_rss_feed(source_name: str, url: str, limit: int = 50):
    try:
        response = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code != 200:
            return []

        feed = feedparser.parse(response.content)

        articles = []
        for entry in feed.entries[:limit]:
            articles.append({
                "title": entry.get("title", ""),
                "content": entry.get("summary", ""),
                "url": entry.get("link", ""),
                "source": source_name,
                "raw_published_at": entry.get("published") or entry.get("updated"),
            })

        return articles

    except Exception as e:
        logger.error(f"{source_name} failed: {e}")
        return []

def fetch_all(limit=50):
    all_articles = []

    def worker(item):
        name, url = item
        return fetch_rss_feed(name, url, limit)

    with ThreadPoolExecutor(max_workers=6) as executor:
        results = executor.map(worker, RSS_FEEDS.items())

    for res in results:
        all_articles.extend(res)

    return all_articles


def save_raw(articles):
    os.makedirs(os.path.dirname(DATA_RAW_PATH), exist_ok=True)

    with open(DATA_RAW_PATH, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    return DATA_RAW_PATH

def run_fetch():
    raw = fetch_all()
    path = save_raw(raw)

    logger.info(f"Saved {len(raw)} raw articles → {path}")
    return path

if __name__ == "__main__":
    run_fetch()