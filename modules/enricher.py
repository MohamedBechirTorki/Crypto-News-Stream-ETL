import os
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

REPO_ROOT = os.getenv('AIRFLOW_HOME', os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
INPUT_PATH  = os.path.join(REPO_ROOT, "data", "filtred", "data.json")
OUTPUT_PATH = os.path.join(REPO_ROOT, "data", "enriched", "data.json")

_finbert = None
def load_finbert():
    global _finbert

    if _finbert is not None:
        return _finbert

    from transformers import pipeline

    logger.info("Loading FinBERT model...")
    _finbert = pipeline(
        "text-classification",
        model="ProsusAI/finbert",
        tokenizer="ProsusAI/finbert",
        top_k=None,
        truncation=True,
        max_length=512,
    )
    logger.info("FinBERT ready.")

    return _finbert



SOURCE_WEIGHTS = {
    # Tier 1 — primary / regulatory
    "sec_press_releases": 1.0,
    "sec_litigation":     1.0,
    "cftc_news":          1.0,
    "federal_reserve":    1.0,
    "imf_blog":           0.95,
    "bank_of_england":    0.95,
    "ft_markets":         0.95,
    "ap_news_business":   0.95,
    "al_jazeera_business":0.90,
    "cnbc_crypto":        0.90,
    "marketwatch":        0.90,
    # Tier 2 — institutional / analytics
    "chainalysis":        0.85,
    "glassnode":          0.85,
    "coinmetrics":        0.85,
    "messari":            0.85,
    "ark_invest":         0.85,
    # Tier 3 — exchanges / official blogs
    "coinbase_blog":      0.80,
    "binance_blog":       0.80,
    "kraken_blog":        0.80,
    "okx_blog":           0.75,
    "bybit_blog":         0.75,
    # Tier 4 — crypto journalism
    "theblock":           0.75,
    "coindesk":           0.75,
    "bitcoin_mag":        0.70,
    "cointelegraph":      0.65,
    "decrypt":            0.65,
    "cryptoslate":        0.65,
    "newsbtc":            0.60,
    # Tier 5 — aggregators / hype-heavy
    "cryptopanic":        0.50,
    "coinjournal":        0.55,
    "beincrypto":         0.50,
    "seeking_alpha":      0.60,
}

# Keywords that confirm the article is crypto/macro relevant
# At least ONE must match, otherwise skip enrichment
CRYPTO_RELEVANCE = [
    "bitcoin", "btc", "ethereum", "eth", "crypto", "blockchain",
    "defi", "nft", "token", "wallet", "exchange", "stablecoin",
    "coinbase", "binance", "kraken", "bybit", "okx",
    "fed", "cpi", "inflation", "interest rate", "tariff",
    "treasury", "dollar", "recession", "gdp", "liquidity",
    "sec", "cftc", "regulation", "etf", "halving",
]

ASSETS = {
    "BTC":        ["bitcoin", "btc"],
    "ETH":        ["ethereum", "eth"],
    "SOL":        ["solana", "sol"],
    "XRP":        ["xrp", "ripple"],
    "USDT":       ["tether", "usdt"],
    "USDC":       ["usdc"],
    "AAVE":       ["aave"],
    "AVAX":       ["avalanche", "avax"],
    "DEFI":       ["defi", "decentralized finance"],
    "ETF":        ["etf", "exchange traded fund"],
    "MARKET":     ["fed", "cpi", "inflation", "rate cut", "dollar",
                   "liquidity", "tariff", "treasury", "gdp", "recession"],
    "REGULATION": ["sec", "cftc", "attorney general",
                   "lawsuit", "sues", "illegal", "ban",
                   "damages", "unlicensed", "enforcement"],
    "EXCHANGE":   ["coinbase", "gemini", "binance", "kraken",
                   "bybit", "okx", "kalshi", "hyperliquid"],
}


CATEGORY_MAP = {
    "regulation":    "regulation",
    "legal":         "regulation",
    "enforcement":   "regulation",
    "macro":         "macro",
    "monetary":      "macro",
    "hack":          "hack",
    "exploit":       "hack",
    "security":      "hack",
    "institutional": "institutional",
    "adoption":      "listing",
    "listing":       "listing",
    "on-chain":      "whale_move",
    "whale":         "whale_move",
    "defi":          "whale_move",
    "sentiment":     "price_movement",
    "technical":     "price_movement",
}

def standardize_category(llm_category: str) -> str:
    if not llm_category:
        return "unknown"
    cat = llm_category.lower()
    for key, mapped in CATEGORY_MAP.items():
        if key in cat:
            return mapped
    return "unknown"

# Each event type needs 2+ keyword hits to be classified
# priority breaks ties when two types both have 2+ hits
EVENT_RULES = {
    "hack": {
        "keywords": ["hack", "hacked", "exploit", "exploited",
                     "drained", "breach", "stolen", "siphon",
                     "vulnerability", "attack"],
        "priority": 5,
        "min_hits": 1,   
    },
    "regulation": {
        "keywords": ["sec", "cftc", "regulation", "lawsuit", "ban",
                     "approved", "etf", "filing", "legislation",
                     "compliance", "enforcement"],
        "priority": 5,
        "min_hits": 1,
    },
    "macro": {
        "keywords": ["fed", "cpi", "inflation", "interest rate",
                     "recession", "gdp", "tariff", "treasury",
                     "dollar index", "rate cut", "rate hike"],
        "priority": 4,
        "min_hits": 2,
    },
    "institutional": {
        "keywords": ["firm", "trump-backed", "corporate", "treasury",
                 "buys", "purchased", "acquires", "holdings",
                 "adds", "billion", "million", "institutional",
                 "strategy", "mstr", "microstrategy", "shares spike",
                 "etf inflow", "fund", "pre-ipo"],
        "priority": 4,
        "min_hits": 1,
    },
    "whale_move": {
        "keywords": ["whale", "on-chain", "accumulation",
                     "dormant wallet", "large transfer",
                     "moved", "outflow", "inflow"],
        "priority": 3,
        "min_hits": 1,   
    },
    "price_movement": {
        "keywords": ["surge", "rally", "pump", "dump", "crash",
                     "plunge", "rebound", "jump", "breakout",
                     "resistance", "support", "ath"],
        "priority": 2,
        "min_hits": 2,
    },
    "listing": {
        "keywords": ["listed", "listing", "launches", "new token",
                     "trading pair", "exchange listing"],
        "priority": 2,
        "min_hits": 2,
    },
}

TIMEFRAME_MAP = {
    "hack":           "short",
    "price_movement": "short",
    "whale_move":     "short",
    "listing":        "short",
    "institutional":  "mid",
    "regulation":     "mid",
    "macro":          "mid",
    "unknown":        "short",
}

# Impact base scores — 0 to 3 scale
IMPACT_BASE = {
    "hack":           3,
    "regulation":     3,
    "institutional":  3,
    "macro":          2,
    "whale_move":     2,
    "price_movement": 1,
    "listing":        1,
    "unknown":        0,
}

# HELPERS

def normalize(text: str) -> str:
    return text.lower() if text else ""

def get_text(article: dict) -> str:
    """Combine all available text fields."""
    parts = [
        article.get("title", ""),
        article.get("description", ""),
        article.get("content", ""),
    ]
    return " ".join(p for p in parts if p).strip()

def is_crypto_relevant(text: str) -> bool:
    t = normalize(text)
    return any(kw in t for kw in CRYPTO_RELEVANCE)

def detect_assets(text: str) -> list:
    t = normalize(text)
    found = {
        asset
        for asset, keywords in ASSETS.items()
        if any(kw in t for kw in keywords)
    }
    return list(found) if found else ["UNKNOWN"]

def classify_event(text: str, llm_category: str = "") -> str:
    """
    Primary: use LLM category (already understood context).
    Fallback: keyword rules if LLM category missing or unknown.
    """
    # Try LLM category first
    from_llm = standardize_category(llm_category)
    if from_llm != "unknown":
        return from_llm

    # Fallback to keyword rules
    t = normalize(text)
    best_type  = "unknown"
    best_score = 0

    for etype, rule in EVENT_RULES.items():
        hits = sum(1 for kw in rule["keywords"] if kw in t)
        min_hits = rule.get("min_hits", 1)
        if hits >= min_hits and rule["priority"] > best_score:
            best_type  = etype
            best_score = rule["priority"]

    return best_type

def get_source_weight(source: str) -> float:
    return SOURCE_WEIGHTS.get(source, 0.60)

def compute_impact(event_type: str, source: str, assets: list) -> int:
    """
    0-3 scale.
    Source weight fine-tunes by ±1 but never overrides the event tier.
    BTC or MARKET mention adds +1 if not already at 3.
    """
    base   = IMPACT_BASE.get(event_type, 0)
    weight = get_source_weight(source)

    # Source credibility fine-tune (±1 max)
    if base > 0:
        if weight >= 0.85:
            base = min(base + 1, 3)
        elif weight <= 0.55:
            base = max(base - 1, 0)

    # BTC/MARKET relevance boost
    if base < 3 and ("BTC" in assets or "MARKET" in assets or "REGULATION" in assets):
        base = min(base + 1, 3)

    return base

# FINBERT SENTIMENT

def _finbert_score(text: str) -> dict:
    if not text or len(text.strip()) < 10:
        return {
            "sentiment_label": "neutral",
            "sentiment_score": 0.0,
            "sentiment_conf":  0.0,
        }

    results = _finbert(_finbert_clean(text))[0]   # list of {label, score}
    probs   = {r["label"]: r["score"] for r in results}

    pos = probs.get("positive", 0.0)
    neg = probs.get("negative", 0.0)
    neu = probs.get("neutral",  0.0)

    # Signed score: positive → +1, negative → -1
    signed = round(pos - neg, 4)

    # Winning label
    label = max(probs, key=probs.get)
    conf  = round(probs[label], 4)

    return {
        "sentiment_label": label,
        "sentiment_score": signed,
        "sentiment_conf":  conf,
    }

def _finbert_clean(text: str) -> str:
    """Keep only first 400 chars — FinBERT is headline-focused."""
    return text[:400].strip()

def compute_sentiment(article: dict) -> dict:
    title   = article.get("title", "")
    content = (
        article.get("content", "")
        or article.get("description", "")
        or ""
    )

    headline_sent = _finbert_score(title)
    content_sent  = _finbert_score(content[:400]) if content else None

    if content_sent:
        # Weighted average: headline 40%, content 60%
        final_score = round(
            0.4 * headline_sent["sentiment_score"]
            + 0.6 * content_sent["sentiment_score"],
            4
        )
    else:
        final_score = headline_sent["sentiment_score"]

    # Final label from final score
    if final_score > 0.1:
        final_label = "positive"
    elif final_score < -0.1:
        final_label = "negative"
    else:
        final_label = "neutral"

    return {
        "headline_sentiment_score": headline_sent["sentiment_score"],
        "headline_sentiment_label": headline_sent["sentiment_label"],
        "headline_sentiment_conf":  headline_sent["sentiment_conf"],
        "content_sentiment_score":  content_sent["sentiment_score"] if content_sent else 0.0,
        "content_sentiment_label":  content_sent["sentiment_label"] if content_sent else "neutral",
        "final_sentiment_score":    final_score,
        "final_sentiment_label":    final_label,
    }

# ENRICHMENT CORE

def enrich_article(article: dict) -> dict | None:
    source = article.get("source", "") or ""


    text = get_text(article)

    # Gate 2 — crypto/macro relevance
    if not is_crypto_relevant(text):
        logger.info(f"[SKIP] not relevant: {article.get('title','')[:60]}")
        return None

    # Classification
    event_type = classify_event(text, article.get("category", ""))
    assets        = detect_assets(text)
    timeframe     = TIMEFRAME_MAP.get(event_type, "short")
    source_weight = get_source_weight(source)
    impact_score  = compute_impact(event_type, source, assets)

    # Sentiment via FinBERT
    sentiment = compute_sentiment(article)

    article.update({
        "event_type":     event_type,
        "asset_mentioned":assets,
        "impact_score":   impact_score,
        "timeframe":      timeframe,
        "source_weight":  source_weight,
        **sentiment,
    })

    logger.info(
        f"[ENRICHED] "
        f"title='{article.get('title','')[:55]}' | "
        f"event={event_type} | assets={assets} | "
        f"impact={impact_score} | "
        f"sent={sentiment['final_sentiment_score']} ({sentiment['final_sentiment_label']}) | "
        f"conf={sentiment['headline_sentiment_conf']}"
    )

    return article

# PIPELINE

ALLOWED_FIELDS = {
    "id",                  # keep if you still want it; remove if not
    "title",
    "content",
    "url",
    "source",
    "ingestion_time",
    "event_type",
    "asset_mentioned",
    "impact_score",
    "timeframe",
    "source_weight",

    # sentiment
    "headline_sentiment_score",
    "headline_sentiment_label",
    "headline_sentiment_conf",
    "content_sentiment_score",
    "content_sentiment_label",
    "final_sentiment_score",
    "final_sentiment_label",
}

def run_enricher():
    from transformers import pipeline
    load_finbert()
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    articles = data.get("articles", [])
    ingestion_time = data.get("ingestion_time")

    # Only process articles that passed the noise gate
    candidates = [a for a in articles if a.get("is_noise") is False]
    logger.info(f"[PIPELINE] {len(candidates)} non-noise articles to enrich")

    enriched = []
    skipped  = 0

    for a in candidates:
        try:
            result = enrich_article(a)
            if result:
                enriched.append(result)
            else:
                skipped += 1
        except Exception as e:
            logger.error(f"[ERROR] {a.get('title','')[:50]} → {e}")

    logger.info(
        f"[PIPELINE] enriched={len(enriched)} | "
        f"skipped={skipped} | "
        f"total_in={len(candidates)}"
    )

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    def filter_fields(article: dict) -> dict:
        return {k: v for k, v in article.items() if k in ALLOWED_FIELDS}

    cleaned = [filter_fields(a) for a in enriched]

    payload = {
        "ingestion_time": ingestion_time,
        "articles": cleaned
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(enriched)} articles → {OUTPUT_PATH}")
    return OUTPUT_PATH


if __name__ == "__main__":
    run_enricher()