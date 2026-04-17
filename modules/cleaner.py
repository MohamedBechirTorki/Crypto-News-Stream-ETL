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

NOISE_KEYWORDS = [
    "daily recap",
    "weekly roundup",
    "morning brief",
    "what happened today",
    "top stories",
    "market wrap"
]

WEAK_FACTUAL_PATTERNS = [
    "price prediction",
    "analysis shows",
    "according to analysts"
]

def noise_score(a):
    score = 0
    title = a.get("title", "").lower()

    score += sum(1 for kw in NOISE_KEYWORDS if kw in title)
    score += sum(1 for p in WEAK_FACTUAL_PATTERNS if p in title)

    if len(title.split()) < 6:
        score += 1

    return score

def quality_filter(a):
    if a["text_length"] < 100:
        return False
    if a["title_length"] < 10:
        return False
    return noise_score(a) <= 1   


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


def compute_quality_score(a):
    score = 0

    title = a["title"].lower()
    content = (a.get("content") or "").lower()

    # Length scoring
    if a["text_length"] > 300:
        score += 2
    elif a["text_length"] > 150:
        score += 1

    # Keyword relevance
    IMPORTANT_KEYWORDS = [
        # People / politics
        "trump", "powell", "elon musk", "sec", "fed", "federal reserve",

        # Institutions
        "blackrock", "jpmorgan", "goldman sachs", "morgan stanley",
        "fidelity", "binance", "coinbase",

        # Regulation / law
        "regulation", "etf", "approval", "ban", "legal", "law", "compliance",

        # Macro / economy
        "inflation", "interest rate", "cpi", "recession", "liquidity",

        # Market-moving events
        "hack", "exploit", "bankruptcy", "crisis", "outflows", "inflows",
        "adoption", "integration", "partnership",

        # Crypto-specific strong signals
        "bitcoin etf", "ethereum etf", "halving", "staking", "mainnet",
        "upgrade", "fork"
    ]
    if any(k in title or k in content for k in IMPORTANT_KEYWORDS):
        score += 2

    # Penalize generic / low-value titles
    PENALTY_KEYWORDS = [
        "analysis",
        "price prediction",
        "price outlook",
        "could",
        "might",
        "likely",
        "expected",
        "forecast",
        "bull run",
        "bearish",
        "bullish",
        "what this means",
        "is this the",
        "next big",
        "set to",
        "teases",
        "signals",
        "opinion",
        "why",
        "could",
        "ai"
    ]
    if any(p in title for p in PENALTY_KEYWORDS):
        score -= 2


    return score

def compute_signal_score(a):
    return compute_quality_score(a) - (0.5 * noise_score(a))


def run_cleaning():
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        articles = json.load(f)

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=6)

    filtered = []
    for a in articles:
        pub = dateutil_parser.parse(a["published_at"])
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        
        score = compute_quality_score(a)
        noise = noise_score(a)

        a["quality_score"] = score
        a["noise_score"] = noise
        a["signal_score"] = score - 0.5 * noise
        if is_in_window(pub, start, now) and quality_filter(a) :
            filtered.append(a)
    cleaned = deduplicate(filtered)
    for i in range(len(cleaned)) :
        print(cleaned[i]["title"], "quality_score",cleaned[i]["quality_score"], "noise_score", cleaned[i]["noise_score"], "signal_score", cleaned[i]["signal_score"])
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(cleaned)} cleaned articles → {OUTPUT_PATH}")
    return OUTPUT_PATH


if __name__ == "__main__":
    run_cleaning()