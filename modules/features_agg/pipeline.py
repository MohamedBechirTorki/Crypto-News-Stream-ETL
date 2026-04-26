import os
import json
import logging
from datetime import timedelta

from .utils import enforce_dict_list, load_enriched, load_json
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

    # Load enriched — get articles + ingestion_time separately
    enriched, ingestion_time = load_enriched(ENRICHED_PATH)
    filtered = enforce_dict_list(load_json(FILTERED_PATH))
    cleaned  = enforce_dict_list(load_json(CLEANED_PATH))

    if ingestion_time is None:
        raise ValueError("No valid ingestion_time found in enriched data")

    # Compute window_start from ingestion_time directly
    window_start = ingestion_time.replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(hours=1)

    logger.info(
        f"[FEATURES] enriched={len(enriched)} | "
        f"filtered={len(filtered)} | "
        f"cleaned={len(cleaned)} | "
        f"ingestion_time={ingestion_time} | "
        f"window_start={window_start}"
    )

    # Core window features
    features = compute_window_features(enriched, filtered, cleaned)
    features["window_start"] = window_start.isoformat()
    
    # Market + macro features
    market = mf.build_market_features(window_start)
    features.update(market)

    macro = yf.build_market_features()
    features.update(macro)

    # Sentiment momentum
    history_path = os.path.join(REPO_ROOT, "data", "features", "history.json")
    history = []
    if os.path.exists(history_path):
        with open(history_path, "r", encoding="utf-8") as f:
            history = json.load(f)

    features["sentiment_momentum"] = round(
        features["avg_sentiment"] - history[-1]["avg_sentiment"], 4
    ) if history else 0.0

    history.append(features)

    # Save
    os.makedirs(os.path.dirname(history_path), exist_ok=True)
    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(features, f, ensure_ascii=False, indent=2)

    logger.info(f"[FEATURES] window_start:      {features['window_start']}")
    logger.info(f"[FEATURES] article_count:     {features['article_count']}")
    logger.info(f"[FEATURES] noise_ratio:       {features['noise_ratio']}")
    logger.info(f"[FEATURES] avg_sentiment:     {features['avg_sentiment']}")
    logger.info(f"[FEATURES] avg_impact:        {features['avg_impact']}")
    logger.info(f"[FEATURES] sentiment_momentum:{features['sentiment_momentum']}")
    logger.info(f"[FEATURES] Saved → {OUTPUT_PATH}")

    return features


if __name__ == "__main__":
    run_aggregator()