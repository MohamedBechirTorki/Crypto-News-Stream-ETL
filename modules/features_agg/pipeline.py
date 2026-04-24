import os
import json
import logging
from datetime import datetime

from .utils import load_json, get_window_start
from .window_computer import compute_window_features
import features_agg.market_features as mf
import features_agg.yahoo_finance as yf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Path configuration (same as original)
REPO_ROOT = os.getenv('AIRFLOW_HOME', os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
ENRICHED_PATH = os.path.join(REPO_ROOT, "data", "enriched", "data.json")
FILTERED_PATH = os.path.join(REPO_ROOT, "data", "filtred", "data.json")
CLEANED_PATH = os.path.join(REPO_ROOT, "data", "cleaned", "data.json")
OUTPUT_PATH = os.path.join(REPO_ROOT, "data", "features", "window_features.json")


def run_aggregator():
    logger.info("[FEATURES] Loading data...")

    enriched = load_json(ENRICHED_PATH)
    filtered = load_json(FILTERED_PATH)
    cleaned = load_json(CLEANED_PATH)

    logger.info(
        f"[FEATURES] enriched={len(enriched)} | "
        f"filtered={len(filtered)} | "
        f"cleaned={len(cleaned)}"
    )

    # Core window features (sentiment, impact, mentions, etc.)
    features = compute_window_features(enriched, filtered, cleaned)

    # Add market features from Binance
    market = mf.build_market_features(datetime.fromisoformat(get_window_start(enriched)))
    features.update(market)

    # Add macro features from Yahoo Finance
    market = yf.build_market_features()
    features.update(market)

    # History management and sentiment momentum
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
    os.makedirs(os.path.dirname(history_path), exist_ok=True)
    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    # Save latest window features
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
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
    run_aggregator()