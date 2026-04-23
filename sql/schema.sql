CREATE TABLE IF NOT EXISTS window_features (
    window_start TEXT PRIMARY KEY,

    sentiment_momentum REAL,
    article_count INTEGER,
    total_raw INTEGER,
    noise_ratio REAL,

    avg_sentiment REAL,
    max_sentiment REAL,
    min_sentiment REAL,
    sentiment_std REAL,
    weighted_avg_sentiment REAL,

    positive_count INTEGER,
    negative_count INTEGER,
    neutral_count INTEGER,

    avg_confidence REAL,

    avg_impact REAL,
    max_impact INTEGER,
    high_impact_count INTEGER,
    mid_impact_count INTEGER,
    low_impact_count INTEGER,
    net_impact_sentiment REAL,

    has_regulation INTEGER,
    has_hack INTEGER,
    has_institutional INTEGER,
    has_macro INTEGER,
    has_whale INTEGER,
    has_geopolitical INTEGER,
    has_listing INTEGER,
    has_defi INTEGER,
    has_security INTEGER,

    regulation_count INTEGER,
    hack_count INTEGER,
    institutional_count INTEGER,
    macro_count INTEGER,

    btc_mentions INTEGER,
    eth_mentions INTEGER,
    regulation_mentions INTEGER,
    exchange_mentions INTEGER,
    market_mentions INTEGER,
    defi_mentions INTEGER,
    etf_mentions INTEGER,

    avg_source_weight REAL,
    max_source_weight REAL,

    btc_open REAL,
    btc_close REAL,
    btc_high REAL,
    btc_low REAL,
    btc_volume REAL,

    btc_return_6h REAL,
    btc_return_24h REAL,

    rsi_14 REAL,
    macd REAL,
    bb_position REAL,
    volume_ratio REAL,
    btc_dominance REAL,

    fear_greed_index REAL,
    etf_net_flow_24h REAL,

    dxy REAL,
    gold_price REAL,
    sp500_return_1d REAL,
    us_10y_yield REAL
);