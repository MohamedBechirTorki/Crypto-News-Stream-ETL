import os
import json
import sqlite3
import logging

logger = logging.getLogger(__name__)

REPO_ROOT = os.getenv('AIRFLOW_HOME', os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
OUTPUT_PATH = os.path.join(REPO_ROOT, "data", "features", "window_features.json")
DB_PATH = os.path.join(REPO_ROOT, "data", "features.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


def create_table():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(open(os.path.join(REPO_ROOT, "sql", "schema.sql")).read())

    conn.commit()
    conn.close()


def load_features():
    if not os.path.exists(OUTPUT_PATH):
        logger.error("Features file not found")
        return None

    with open(OUTPUT_PATH, "r") as f:
        return json.load(f)


def to_bool_int(value):
    return 1 if value else 0


def insert_features(data):
    conn = get_connection()
    cur = conn.cursor()

    query = """
    INSERT OR REPLACE INTO window_features VALUES (
        :window_start,
        :sentiment_momentum,
        :article_count,
        :total_raw,
        :noise_ratio,

        :avg_sentiment,
        :max_sentiment,
        :min_sentiment,
        :sentiment_std,
        :weighted_avg_sentiment,

        :positive_count,
        :negative_count,
        :neutral_count,

        :avg_confidence,

        :avg_impact,
        :max_impact,
        :high_impact_count,
        :mid_impact_count,
        :low_impact_count,
        :net_impact_sentiment,

        :has_regulation,
        :has_hack,
        :has_institutional,
        :has_macro,
        :has_whale,
        :has_geopolitical,
        :has_listing,
        :has_defi,
        :has_security,

        :regulation_count,
        :hack_count,
        :institutional_count,
        :macro_count,

        :btc_mentions,
        :eth_mentions,
        :regulation_mentions,
        :exchange_mentions,
        :market_mentions,
        :defi_mentions,
        :etf_mentions,

        :avg_source_weight,
        :max_source_weight,

        :btc_open,
        :btc_close,
        :btc_high,
        :btc_low,
        :btc_volume,

        :btc_return_6h,
        :btc_return_24h,

        :rsi_14,
        :macd,
        :bb_position,
        :volume_ratio,
        :btc_dominance,

        :fear_greed_index,
        :etf_net_flow_24h,

        :dxy,
        :gold_price,
        :sp500_return_1d,
        :us_10y_yield
    )
    """

    # normalize booleans
    for key in data:
        if key.startswith("has_"):
            data[key] = to_bool_int(data[key])

    cur.execute(query, data)

    conn.commit()
    conn.close()

    logger.info(f"[DB] Stored window {data['window_start']}")


def save_to_db():
    logger.info("[STORAGE] Loading features...")
    data = load_features()

    if not data:
        return

    insert_features(data)

if __name__ == "__main__":
    create_table()
    save_to_db()