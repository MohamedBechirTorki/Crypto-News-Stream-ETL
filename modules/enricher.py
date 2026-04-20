import os
import json
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

INPUT_PATH = os.path.join(REPO_ROOT, "data", "filtred", "data.json")
OUTPUT_PATH = os.path.join(REPO_ROOT, "data", "enriched", "data.json")

# CONFIG

SOURCE_WEIGHTS = {
    # 🏦 Tier 1 (high credibility / primary sources)
    "sec_press_releases": 1.0,
    "sec_litigation": 1.0,
    "cftc_news": 1.0,
    "federal_reserve": 1.0,
    "imf_blog": 0.95,
    "bank_of_england": 0.95,
    "ft_markets": 0.95,
    "ap_news_business": 0.95,
    "al_jazeera_business": 0.9,
    "cnbc_crypto": 0.9,
    "marketwatch": 0.9,

    # 🏢 Tier 2 (institutional + analytics firms)
    "chainalysis": 0.85,
    "glassnode": 0.85,
    "coinmetrics": 0.85,
    "messari": 0.85,
    "ark_invest": 0.85,

    # 🏦 Exchanges / official blogs
    "coinbase_blog": 0.8,
    "binance_blog": 0.8,
    "kraken_blog": 0.8,
    "okx_blog": 0.75,
    "bybit_blog": 0.75,

    # 🧠 Crypto research / mid-tier journalism
    "theblock": 0.75,
    "coindesk": 0.75,
    "cointelegraph": 0.65,
    "decrypt": 0.65,
    "bitcoin_mag": 0.7,
    "cryptoslate": 0.65,

    # ⚡ Aggregators / lower trust / hype-heavy
    "cryptopanic": 0.5,
    "coinjournal": 0.55,
    "beincrypto": 0.5,
    "ambcrypto": 0.5,
    "dailycoin": 0.5,
}

ASSETS = {
    "BTC": ["bitcoin", "btc", "coinbase", "bybit", "binance", "kraken"],
    "MARKET": ["fed", "cpi", "inflation", "rate cut", "dollar", 
            "liquidity", "iranian assets", "tariff", "treasury"],
    "ETH": ["ethereum", "eth"],
    "SOL": ["solana", "sol"],
    "XRP": ["xrp", "ripple"],
    "USDT": ["tether", "usdt"],
    "USDC": ["usdc"],
    "REGULATION": ["sec", "regulation", "ban", "lawsuit", "compliance"]
}

EVENT_RULES = {
    "hack": {
        "keywords": ["hack", "hacked", "exploit", "exploited", "drained", "breach", "attack", "siphon"],
        "priority": 5
    },
    "price_movement": {
        "keywords": ["surge", "rally", "pump", "dump", "crash", "plunge", "rebound", "jump"],
        "priority": 3
    },
    "regulation": {
        "keywords": ["sec", "cftc", "regulation", "lawsuit", "ban", "approved", "etf", "filing"],
        "priority": 5
    },
    "macro": {
        "keywords": ["fed", "cpi", "inflation", "interest rate", "recession", "gdp"],
        "priority": 4
    },
    "whale_move": {
        "keywords": ["whale", "transfer", "on-chain", "moved", "accumulation"],
        "priority": 4
    },
    "listing": {
        "keywords": ["listed", "binance", "coinbase", "kraken", "exchange"],
        "priority": 2
    }
}

TIMEFRAME_MAP = {
    "hack": "short",
    "price_movement": "short",
    "whale_move": "short",
    "listing": "short",
    "regulation": "mid",
    "macro": "mid"
}


# HELPERS

def normalize_text(text: str) -> str:
    return text.lower() if text else ""

def detect_assets(text: str):
    text = normalize_text(text)
    found = set()

    for asset, keywords in ASSETS.items():
        for kw in keywords:
            if kw in text:
                found.add(asset)
                break

    return list(found) if found else ["UNKNOWN"]


def classify_event(text: str):
    text = normalize_text(text)

    best_type = "unknown"
    best_score = 0

    for etype, rule in EVENT_RULES.items():
        hits = sum(1 for k in rule["keywords"] if k in text)

        if hits > 1 and rule["priority"] > best_score:
            best_type = etype
            best_score = rule["priority"]

    return best_type



def map_timeframe(event_type: str):
    return TIMEFRAME_MAP.get(event_type, "short")

def get_source_weight(source: str):
    return SOURCE_WEIGHTS.get(source, 0.6)  # default low trust

def compute_impact_score(text: str, event_type: str, source: str, assets: list):
    text = normalize_text(text)
    weight = get_source_weight(source)

    base_scores = {
        "hack": 5,
        "regulation": 5,
        "macro": 4,
        "whale_move": 4,
        "listing": 3,
        "price_movement": 2,
        "unknown": 1
    }

    base = base_scores.get(event_type, 1)

    # asset amplification
    asset_boost = 0.3 * len([a for a in assets if a != "UNKNOWN"])
    asset_boost = min(asset_boost, 2.0)

    score = (base + asset_boost) * weight

    if score >= 4:
        return 5
    elif score >= 3:
        return 4
    elif score >= 2:
        return 3
    elif score >= 1.2:
        return 2
    else:
        return 1

def classify_event(text: str):
    text = text.lower()

    best_type = "unknown"
    best_score = 0

    for etype, rule in EVENT_RULES.items():
        hits = sum(1 for k in rule["keywords"] if k in text)

        if hits > 0 and rule["priority"] > best_score:
            best_type = etype
            best_score = rule["priority"]

    return best_type
# ENRICHMENT CORE

def enrich_article(article: dict):
    text = f"{article.get('title', '')} {article.get('description', '')}"
    source = article.get("source", "") or ""

    event_type = classify_event(text)
    assets = detect_assets(text)
    timeframe = map_timeframe(event_type)
    source_weight = get_source_weight(source)

    impact_score = compute_impact_score(
        text,
        event_type,
        source,
        assets
    )

    article["event_type"] = event_type
    article["asset_mentioned"] = assets
    article["impact_score"] = impact_score
    article["affected_timeframe"] = timeframe
    article["source_weight"] = source_weight

    return article


# PIPELINE

def run():
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        articles = json.load(f)

    articles = [a for a in articles if a.get("is_noise") is False]
    enriched = []

    for a in articles:
        try:
            enriched.append(enrich_article(a))
        except Exception as e:
            logger.error(f"Failed article enrichment: {e}")
    for a in enriched:
        print(f"title {a.get('title', '')[:60]}\ncontent: {a.get('content', '')[:60]}\nsource: {a.get('source', '')}\nnot a noise\nevent type: {a.get('event_type', '')}\nassets: {a.get('asset_mentioned', [])}\nimpact_score: {a.get('impact_score', 0)}\ntimeframe: {a.get('affected_timeframe', '')}, source_weight: {a.get('source_weight', 0)}")
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(enriched)} enriched articles → {OUTPUT_PATH}")
    return OUTPUT_PATH


if __name__ == "__main__":
    run()