import os
import json
import logging
import statistics
from datetime import datetime, timezone, timedelta
from features_agg.market_features import build_market_features
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Input paths
ENRICHED_PATH = os.path.join(REPO_ROOT, "data", "enriched", "data.json")
FILTERED_PATH = os.path.join(REPO_ROOT, "data", "filtred",  "data.json")
CLEANED_PATH  = os.path.join(REPO_ROOT, "data", "cleaned",  "data.json")

# Output path
OUTPUT_PATH   = os.path.join(REPO_ROOT, "data", "features", "window_features.json")

# HELPERS

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
    """
    window_start = earliest ingestion_time in enriched batch - 1 hour.
    Falls back to current UTC time if no ingestion_time found.
    """
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


def extract_sentiment(a):
    # Priority 1: full content sentiment (already computed)
    if isinstance(a.get("final_sentiment_score"), (int, float)):
        return a["final_sentiment_score"]

    # Priority 2: headline sentiment (FinBERT output)
    if isinstance(a.get("headline_sentiment_score"), (int, float)):
        return a["headline_sentiment_score"]

    # Priority 3: fallback neutral
    return 0.0

# CORE AGGREGATION

def compute_window_features(
    enriched:  list,
    filtered:  list,
    cleaned:   list,
) -> dict:
    if not enriched:
        return {
            "window_start": get_window_start(cleaned),

            # volume
            "article_count": 0,
            "total_raw": len(cleaned),
            "noise_ratio": None,

            # sentiment
            "avg_sentiment": 0,
            "max_sentiment": 0,
            "min_sentiment": 0,
            "sentiment_std": 0,
            "weighted_avg_sentiment": 0,
            "positive_count": 0,
            "negative_count": 0,
            "neutral_count": 0,
            "avg_confidence": 0,

            # impact
            "avg_impact": 0,
            "max_impact": 0,
            "high_impact_count": 0,
            "mid_impact_count": 0,
            "low_impact_count": 0,
            "net_impact_sentiment": 0,

            # event flags
            "has_regulation": False,
            "has_hack": False,
            "has_institutional": False,
            "has_macro": False,
            "has_whale": False,
            "has_geopolitical": False,
            "has_listing": False,
            "has_defi": False,
            "has_security": False,

            # counts
            "regulation_count": 0,
            "hack_count": 0,
            "institutional_count": 0,
            "macro_count": 0,

            # assets
            "btc_mentions": 0,
            "eth_mentions": 0,
            "regulation_mentions": 0,
            "exchange_mentions": 0,
            "market_mentions": 0,
            "defi_mentions": 0,
            "etf_mentions": 0,

            # sources
            "avg_source_weight": 0,
            "max_source_weight": 0,

            # momentum
            "sentiment_momentum": 0,

            # price features (always null)
            "btc_open": None,
            "btc_close": None,
            "btc_high": None,
            "btc_low": None,
            "btc_volume": None,
            "btc_return_6h": None,
            "btc_return_24h": None,
            "rsi_14": None,
            "macd": None,
            "bb_position": None,
            "volume_ratio": None,
            "btc_dominance": None,
            "fear_greed_index": None,
            "etf_net_flow_24h": None,
            "dxy": None,
            "gold_price": None,
            "sp500_return_1d": None,
            "us_10y_yield": None,
        }

    
    # Window timestamp
    
    window_start = get_window_start(enriched)

    
    # Article counts + noise ratio
    
    article_count = len(enriched)           # non-noise articles
    total_raw     = len(cleaned)            # all articles including noise
    noise_count   = total_raw - article_count
    noise_ratio   = round(noise_count / total_raw, 4) if total_raw > 0 else 0.0

    
    # Sentiment features (from enriched)
    
    sentiments = [extract_sentiment(a) for a in enriched]
    avg_sentiment = safe_mean(sentiments)
    max_sentiment = round(max(sentiments), 4) if sentiments else 0.0
    min_sentiment = round(min(sentiments), 4) if sentiments else 0.0
    sentiment_std = safe_std(sentiments)

    # Positive / negative / neutral breakdown
    positive_count = sum(1 for s in sentiments if s >  0.1)
    negative_count = sum(1 for s in sentiments if s < -0.1)
    neutral_count  = sum(1 for s in sentiments if -0.1 <= s <= 0.1)

    # Weighted sentiment — high impact articles count more
    weighted_sentiments = []
    for a in enriched:
        score  = a.get("final_sentiment_score", 0.0)
        impact = a.get("impact_score", 0)
        weight = impact if impact > 0 else 0.5   # floor weight of 0.5
        weighted_sentiments.extend([score] * int(weight * 2))
    weighted_avg_sentiment = safe_mean(weighted_sentiments)

    
    # Impact features
    
    impacts = [
        a["impact_score"]
        for a in enriched
        if isinstance(a.get("impact_score"), (int, float))
    ]

    avg_impact        = safe_mean(impacts)
    max_impact        = int(max(impacts)) if impacts else 0
    high_impact_count = sum(1 for i in impacts if i >= 3)
    mid_impact_count  = sum(1 for i in impacts if i == 2)
    low_impact_count  = sum(1 for i in impacts if i <= 1)

    
    # Event type flags (one-hot)
    
    event_types = [a.get("event_type", "unknown") for a in enriched]

    has_regulation   = "regulation"   in event_types
    has_hack         = "hack"         in event_types
    has_institutional= "institutional"in event_types
    has_macro        = "macro"        in event_types
    has_whale        = "whale_move"   in event_types
    has_geopolitical = "geopolitical" in event_types
    has_listing      = "listing"      in event_types
    has_defi         = "defi"         in event_types
    has_security     = "security"     in event_types

    # Event type counts
    regulation_count    = event_types.count("regulation")
    hack_count          = event_types.count("hack")
    institutional_count = event_types.count("institutional")
    macro_count         = event_types.count("macro")

    
    # Asset mention counts
    
    def count_asset(asset_key: str) -> int:
        return sum(
            1 for a in enriched
            if asset_key in (a.get("asset_mentioned") or [])
        )

    btc_mentions        = count_asset("BTC")
    eth_mentions        = count_asset("ETH")
    regulation_mentions = count_asset("REGULATION")
    exchange_mentions   = count_asset("EXCHANGE")
    market_mentions     = count_asset("MARKET")
    defi_mentions       = count_asset("DEFI")
    etf_mentions        = count_asset("ETF")

    
    # Source credibility
    
    weights = [
        a["source_weight"]
        for a in enriched
        if isinstance(a.get("source_weight"), (int, float))
    ]

    avg_source_weight = safe_mean(weights)
    max_source_weight = round(max(weights), 4) if weights else 0.0

    
    # Confidence (FinBERT)
    
    confs = [
        a["headline_sentiment_conf"]
        for a in enriched
        if isinstance(a.get("headline_sentiment_conf"), (int, float))
    ]
    avg_confidence = safe_mean(confs)

    
    # Bearish / Bullish signal strength
    
    bullish_impact = sum(
        a.get("impact_score", 0)
        for a in enriched
        if a.get("final_sentiment_score", 0) > 0.1
    )
    bearish_impact = sum(
        a.get("impact_score", 0)
        for a in enriched
        if a.get("final_sentiment_score", 0) < -0.1
    )
    net_impact_sentiment = bullish_impact - bearish_impact

    
    # Assemble final feature vector
    
    features = {

        # === WINDOW METADATA ===
        "window_start":             window_start,
        "sentiment_momentum":       None,   # fill after collecting multiple windows

        # === VOLUME / NOISE ===
        "article_count":            article_count,
        "total_raw":                total_raw,
        "noise_ratio":              noise_ratio,

        # === SENTIMENT ===
        "avg_sentiment":            avg_sentiment,
        "max_sentiment":            max_sentiment,
        "min_sentiment":            min_sentiment,
        "sentiment_std":            sentiment_std,
        "weighted_avg_sentiment":   weighted_avg_sentiment,
        "positive_count":           positive_count,
        "negative_count":           negative_count,
        "neutral_count":            neutral_count,
        "avg_confidence":           avg_confidence,

        # === IMPACT ===
        "avg_impact":               avg_impact,
        "max_impact":               max_impact,
        "high_impact_count":        high_impact_count,
        "mid_impact_count":         mid_impact_count,
        "low_impact_count":         low_impact_count,
        "net_impact_sentiment":     net_impact_sentiment,   # bullish - bearish weighted

        # === EVENT TYPE FLAGS ===
        "has_regulation":           has_regulation,
        "has_hack":                 has_hack,
        "has_institutional":        has_institutional,
        "has_macro":                has_macro,
        "has_whale":                has_whale,
        "has_geopolitical":         has_geopolitical,
        "has_listing":              has_listing,
        "has_defi":                 has_defi,
        "has_security":             has_security,

        # === EVENT TYPE COUNTS ===
        "regulation_count":         regulation_count,
        "hack_count":               hack_count,
        "institutional_count":      institutional_count,
        "macro_count":              macro_count,

        # === ASSET MENTIONS ===
        "btc_mentions":             btc_mentions,
        "eth_mentions":             eth_mentions,
        "regulation_mentions":      regulation_mentions,
        "exchange_mentions":        exchange_mentions,
        "market_mentions":          market_mentions,
        "defi_mentions":            defi_mentions,
        "etf_mentions":             etf_mentions,

        # === SOURCE CREDIBILITY ===
        "avg_source_weight":        avg_source_weight,
        "max_source_weight":        max_source_weight,

        # === PRICE / MACRO FEATURES (fill later) ===
        "btc_open":                 None,
        "btc_close":                None,
        "btc_high":                 None,
        "btc_low":                  None,
        "btc_volume":               None,
        "btc_return_6h":            None,
        "btc_return_24h":           None,
        "rsi_14":                   None,
        "macd":                     None,
        "bb_position":              None,
        "volume_ratio":             None,
        "btc_dominance":            None,
        "fear_greed_index":         None,
        "etf_net_flow_24h":         None,
        "dxy":                      None,
        "gold_price":               None,
        "sp500_return_1d":          None,
        "us_10y_yield":             None,
    }

    return features



# PIPELINE

def run():
    logger.info("[FEATURES] Loading data...")

    enriched = load_json(ENRICHED_PATH)
    filtered = load_json(FILTERED_PATH)
    cleaned  = load_json(CLEANED_PATH)

    logger.info(
        f"[FEATURES] enriched={len(enriched)} | "
        f"filtered={len(filtered)} | "
        f"cleaned={len(cleaned)}"
    )            

    features = compute_window_features(enriched, filtered, cleaned)
    market = build_market_features(datetime.fromisoformat(get_window_start(enriched)))

    features.update(market)

    # Load existing history to append
    history_path = os.path.join(REPO_ROOT, "data", "features", "history.json")
    if os.path.exists(history_path):
        with open(history_path, "r", encoding="utf-8") as f:
            history = json.load(f)
    else:
        history = []

    if history:
        prev_sentiment = history[-1]["avg_sentiment"]
        features["sentiment_momentum"] = round(
            features["avg_sentiment"] - prev_sentiment, 4
        )
    else:
        features["sentiment_momentum"] = 0.0

    history.append(features)

    # Save history
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    # Save latest window features
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(features, f, ensure_ascii=False, indent=2)

    # Log summary
    logger.info(f"[FEATURES] window_start:       {features['window_start']}")
    logger.info(f"[FEATURES] article_count:      {features['article_count']}")
    logger.info(f"[FEATURES] noise_ratio:        {features['noise_ratio']}")
    logger.info(f"[FEATURES] avg_sentiment:      {features['avg_sentiment']}")
    logger.info(f"[FEATURES] avg_impact:         {features['avg_impact']}")
    logger.info(f"[FEATURES] max_impact:         {features['max_impact']}")
    logger.info(f"[FEATURES] high_impact_count:  {features['high_impact_count']}")
    logger.info(f"[FEATURES] has_regulation:     {features['has_regulation']}")
    logger.info(f"[FEATURES] has_hack:           {features['has_hack']}")
    logger.info(f"[FEATURES] has_macro:          {features['has_macro']}")
    logger.info(f"[FEATURES] btc_mentions:       {features['btc_mentions']}")
    logger.info(f"[FEATURES] net_impact_sent:    {features['net_impact_sentiment']}")
    logger.info(f"[FEATURES] Saved → {OUTPUT_PATH}")

    return features


if __name__ == "__main__":
    run()