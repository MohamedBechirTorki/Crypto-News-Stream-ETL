import os
import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_PATH = os.path.join(REPO_ROOT, "data", "features", "window_features.json")

# PostgreSQL connection config — set these as env vars in Airflow
DB_CONFIG = {
    "host":     os.getenv("PG_HOST"),
    "port":     int(os.getenv("PG_PORT")),
    "dbname":   os.getenv("PG_DB"),
    "user":     os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
}
# CONNECTION

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# SCHEMA

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS window_features (
    window_start            TIMESTAMPTZ PRIMARY KEY,
    sentiment_momentum      FLOAT,

    -- volume / noise
    article_count           INTEGER,
    total_raw               INTEGER,
    noise_ratio             FLOAT,

    -- sentiment
    avg_sentiment           FLOAT,
    max_sentiment           FLOAT,
    min_sentiment           FLOAT,
    sentiment_std           FLOAT,
    weighted_avg_sentiment  FLOAT,
    positive_count          INTEGER,
    negative_count          INTEGER,
    neutral_count           INTEGER,
    avg_confidence          FLOAT,

    -- impact
    avg_impact              FLOAT,
    max_impact              INTEGER,
    high_impact_count       INTEGER,
    mid_impact_count        INTEGER,
    low_impact_count        INTEGER,
    net_impact_sentiment    FLOAT,

    -- event flags
    has_regulation          BOOLEAN,
    has_hack                BOOLEAN,
    has_institutional       BOOLEAN,
    has_macro               BOOLEAN,
    has_whale               BOOLEAN,
    has_geopolitical        BOOLEAN,
    has_listing             BOOLEAN,
    has_defi                BOOLEAN,
    has_security            BOOLEAN,

    -- event counts
    regulation_count        INTEGER,
    hack_count              INTEGER,
    institutional_count     INTEGER,
    macro_count             INTEGER,

    -- asset mentions
    btc_mentions            INTEGER,
    eth_mentions            INTEGER,
    regulation_mentions     INTEGER,
    exchange_mentions       INTEGER,
    market_mentions         INTEGER,
    defi_mentions           INTEGER,
    etf_mentions            INTEGER,

    -- source credibility
    avg_source_weight       FLOAT,
    max_source_weight       FLOAT,

    -- price / technical
    btc_open                FLOAT,
    btc_close               FLOAT,
    btc_high                FLOAT,
    btc_low                 FLOAT,
    btc_volume              FLOAT,
    btc_return_6h           FLOAT,
    btc_return_24h          FLOAT,
    rsi_14                  FLOAT,
    macd                    FLOAT,
    bb_position             FLOAT,
    volume_ratio            FLOAT,

    -- market
    btc_dominance           FLOAT,
    fear_greed_index        FLOAT,
    etf_net_flow_24h        FLOAT,

    -- macro
    dxy                     FLOAT,
    gold_price              FLOAT,
    sp500_return_1d         FLOAT,
    us_10y_yield            FLOAT,

    -- metadata
    created_at              TIMESTAMPTZ DEFAULT NOW()
);
"""

def create_table():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        logger.info("[DB] Table window_features ready")
    except Exception as e:
        conn.rollback()
        logger.error(f"[DB] create_table failed: {e}")
        raise
    finally:
        conn.close()

# LOAD

def load_features() -> dict | None:
    if not os.path.exists(OUTPUT_PATH):
        logger.error(f"[STORAGE] Features file not found: {OUTPUT_PATH}")
        return None
    with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# INSERT

INSERT_SQL = """
INSERT INTO window_features (
    window_start, sentiment_momentum,
    article_count, total_raw, noise_ratio,
    avg_sentiment, max_sentiment, min_sentiment, sentiment_std,
    weighted_avg_sentiment, positive_count, negative_count,
    neutral_count, avg_confidence,
    avg_impact, max_impact, high_impact_count, mid_impact_count,
    low_impact_count, net_impact_sentiment,
    has_regulation, has_hack, has_institutional, has_macro,
    has_whale, has_geopolitical, has_listing, has_defi, has_security,
    regulation_count, hack_count, institutional_count, macro_count,
    btc_mentions, eth_mentions, regulation_mentions, exchange_mentions,
    market_mentions, defi_mentions, etf_mentions,
    avg_source_weight, max_source_weight,
    btc_open, btc_close, btc_high, btc_low, btc_volume,
    btc_return_6h, btc_return_24h,
    rsi_14, macd, bb_position, volume_ratio,
    btc_dominance, fear_greed_index, etf_net_flow_24h,
    dxy, gold_price, sp500_return_1d, us_10y_yield
)
VALUES (
    %(window_start)s, %(sentiment_momentum)s,
    %(article_count)s, %(total_raw)s, %(noise_ratio)s,
    %(avg_sentiment)s, %(max_sentiment)s, %(min_sentiment)s, %(sentiment_std)s,
    %(weighted_avg_sentiment)s, %(positive_count)s, %(negative_count)s,
    %(neutral_count)s, %(avg_confidence)s,
    %(avg_impact)s, %(max_impact)s, %(high_impact_count)s, %(mid_impact_count)s,
    %(low_impact_count)s, %(net_impact_sentiment)s,
    %(has_regulation)s, %(has_hack)s, %(has_institutional)s, %(has_macro)s,
    %(has_whale)s, %(has_geopolitical)s, %(has_listing)s, %(has_defi)s,
    %(has_security)s,
    %(regulation_count)s, %(hack_count)s, %(institutional_count)s, %(macro_count)s,
    %(btc_mentions)s, %(eth_mentions)s, %(regulation_mentions)s,
    %(exchange_mentions)s, %(market_mentions)s, %(defi_mentions)s, %(etf_mentions)s,
    %(avg_source_weight)s, %(max_source_weight)s,
    %(btc_open)s, %(btc_close)s, %(btc_high)s, %(btc_low)s, %(btc_volume)s,
    %(btc_return_6h)s, %(btc_return_24h)s,
    %(rsi_14)s, %(macd)s, %(bb_position)s, %(volume_ratio)s,
    %(btc_dominance)s, %(fear_greed_index)s, %(etf_net_flow_24h)s,
    %(dxy)s, %(gold_price)s, %(sp500_return_1d)s, %(us_10y_yield)s
)
ON CONFLICT (window_start) DO UPDATE SET
    sentiment_momentum      = EXCLUDED.sentiment_momentum,
    article_count           = EXCLUDED.article_count,
    total_raw               = EXCLUDED.total_raw,
    noise_ratio             = EXCLUDED.noise_ratio,
    avg_sentiment           = EXCLUDED.avg_sentiment,
    max_sentiment           = EXCLUDED.max_sentiment,
    min_sentiment           = EXCLUDED.min_sentiment,
    sentiment_std           = EXCLUDED.sentiment_std,
    weighted_avg_sentiment  = EXCLUDED.weighted_avg_sentiment,
    positive_count          = EXCLUDED.positive_count,
    negative_count          = EXCLUDED.negative_count,
    neutral_count           = EXCLUDED.neutral_count,
    avg_confidence          = EXCLUDED.avg_confidence,
    avg_impact              = EXCLUDED.avg_impact,
    max_impact              = EXCLUDED.max_impact,
    high_impact_count       = EXCLUDED.high_impact_count,
    mid_impact_count        = EXCLUDED.mid_impact_count,
    low_impact_count        = EXCLUDED.low_impact_count,
    net_impact_sentiment    = EXCLUDED.net_impact_sentiment,
    has_regulation          = EXCLUDED.has_regulation,
    has_hack                = EXCLUDED.has_hack,
    has_institutional       = EXCLUDED.has_institutional,
    has_macro               = EXCLUDED.has_macro,
    has_whale               = EXCLUDED.has_whale,
    has_geopolitical        = EXCLUDED.has_geopolitical,
    has_listing             = EXCLUDED.has_listing,
    has_defi                = EXCLUDED.has_defi,
    has_security            = EXCLUDED.has_security,
    regulation_count        = EXCLUDED.regulation_count,
    hack_count              = EXCLUDED.hack_count,
    institutional_count     = EXCLUDED.institutional_count,
    macro_count             = EXCLUDED.macro_count,
    btc_mentions            = EXCLUDED.btc_mentions,
    eth_mentions            = EXCLUDED.eth_mentions,
    regulation_mentions     = EXCLUDED.regulation_mentions,
    exchange_mentions       = EXCLUDED.exchange_mentions,
    market_mentions         = EXCLUDED.market_mentions,
    defi_mentions           = EXCLUDED.defi_mentions,
    etf_mentions            = EXCLUDED.etf_mentions,
    avg_source_weight       = EXCLUDED.avg_source_weight,
    max_source_weight       = EXCLUDED.max_source_weight,
    btc_open                = EXCLUDED.btc_open,
    btc_close               = EXCLUDED.btc_close,
    btc_high                = EXCLUDED.btc_high,
    btc_low                 = EXCLUDED.btc_low,
    btc_volume              = EXCLUDED.btc_volume,
    btc_return_6h           = EXCLUDED.btc_return_6h,
    btc_return_24h          = EXCLUDED.btc_return_24h,
    rsi_14                  = EXCLUDED.rsi_14,
    macd                    = EXCLUDED.macd,
    bb_position             = EXCLUDED.bb_position,
    volume_ratio            = EXCLUDED.volume_ratio,
    btc_dominance           = EXCLUDED.btc_dominance,
    fear_greed_index        = EXCLUDED.fear_greed_index,
    etf_net_flow_24h        = EXCLUDED.etf_net_flow_24h,
    dxy                     = EXCLUDED.dxy,
    gold_price              = EXCLUDED.gold_price,
    sp500_return_1d         = EXCLUDED.sp500_return_1d,
    us_10y_yield            = EXCLUDED.us_10y_yield,
    created_at              = NOW();
"""

def insert_features(data: dict):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(INSERT_SQL, data)
        conn.commit()
        logger.info(f"[DB] Stored window {data['window_start']}")
    except Exception as e:
        conn.rollback()
        logger.error(f"[DB] insert_features failed: {e}")
        raise
    finally:
        conn.close()

# PIPELINE

def save_to_db():
    logger.info("[STORAGE] Loading features...")
    create_table()
    data = load_features()
    if not data:
        return
    insert_features(data)

# MAIN

if __name__ == "__main__":
    save_to_db()