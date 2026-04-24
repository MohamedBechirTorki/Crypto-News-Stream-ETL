from .utils import get_window_start, safe_mean, safe_std, extract_sentiment
from .btc_dominance import get_btc_dominance


def compute_window_features(enriched: list, filtered: list, cleaned: list) -> dict:
    """
    Compute all window‑level features from enriched, filtered and cleaned articles.
    Price/macro fields are left as None – they are filled later by the pipeline.
    """
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
    article_count = len(enriched)
    total_raw = len(cleaned)
    noise_count = total_raw - article_count
    noise_ratio = round(noise_count / total_raw, 4) if total_raw > 0 else 0.0

    # Sentiment features
    sentiments = [extract_sentiment(a) for a in enriched]
    avg_sentiment = safe_mean(sentiments)
    max_sentiment = round(max(sentiments), 4) if sentiments else 0.0
    min_sentiment = round(min(sentiments), 4) if sentiments else 0.0
    sentiment_std = safe_std(sentiments)

    positive_count = sum(1 for s in sentiments if s > 0.1)
    negative_count = sum(1 for s in sentiments if s < -0.1)
    neutral_count = sum(1 for s in sentiments if -0.1 <= s <= 0.1)

    # Weighted sentiment by impact
    weighted_sentiments = []
    for a in enriched:
        score = a.get("final_sentiment_score", 0.0)
        impact = a.get("impact_score", 0)
        weight = impact if impact > 0 else 0.5
        weighted_sentiments.extend([score] * int(weight * 2))
    weighted_avg_sentiment = safe_mean(weighted_sentiments)

    # Impact features
    impacts = [
        a["impact_score"]
        for a in enriched
        if isinstance(a.get("impact_score"), (int, float))
    ]
    avg_impact = safe_mean(impacts)
    max_impact = int(max(impacts)) if impacts else 0
    high_impact_count = sum(1 for i in impacts if i >= 3)
    mid_impact_count = sum(1 for i in impacts if i == 2)
    low_impact_count = sum(1 for i in impacts if i <= 1)

    # Event type flags & counts
    event_types = [a.get("event_type", "unknown") for a in enriched]
    has_regulation = "regulation" in event_types
    has_hack = "hack" in event_types
    has_institutional = "institutional" in event_types
    has_macro = "macro" in event_types
    has_whale = "whale_move" in event_types
    has_geopolitical = "geopolitical" in event_types
    has_listing = "listing" in event_types
    has_defi = "defi" in event_types
    has_security = "security" in event_types

    regulation_count = event_types.count("regulation")
    hack_count = event_types.count("hack")
    institutional_count = event_types.count("institutional")
    macro_count = event_types.count("macro")

    # Asset mentions
    def count_asset(asset_key: str) -> int:
        return sum(1 for a in enriched if asset_key in (a.get("asset_mentioned") or []))

    btc_mentions = count_asset("BTC")
    eth_mentions = count_asset("ETH")
    regulation_mentions = count_asset("REGULATION")
    exchange_mentions = count_asset("EXCHANGE")
    market_mentions = count_asset("MARKET")
    defi_mentions = count_asset("DEFI")
    etf_mentions = count_asset("ETF")

    # Source credibility
    weights = [
        a["source_weight"]
        for a in enriched
        if isinstance(a.get("source_weight"), (int, float))
    ]
    avg_source_weight = safe_mean(weights)
    max_source_weight = round(max(weights), 4) if weights else 0.0

    # Confidence
    confs = [
        a["headline_sentiment_conf"]
        for a in enriched
        if isinstance(a.get("headline_sentiment_conf"), (int, float))
    ]
    avg_confidence = safe_mean(confs)

    # Bullish / bearish net impact
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

    btc_dom = get_btc_dominance()

    features = {
        "window_start": window_start,
        "sentiment_momentum": None,       # filled later from history

        # volume / noise
        "article_count": article_count,
        "total_raw": total_raw,
        "noise_ratio": noise_ratio,

        # sentiment
        "avg_sentiment": avg_sentiment,
        "max_sentiment": max_sentiment,
        "min_sentiment": min_sentiment,
        "sentiment_std": sentiment_std,
        "weighted_avg_sentiment": weighted_avg_sentiment,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "neutral_count": neutral_count,
        "avg_confidence": avg_confidence,

        # impact
        "avg_impact": avg_impact,
        "max_impact": max_impact,
        "high_impact_count": high_impact_count,
        "mid_impact_count": mid_impact_count,
        "low_impact_count": low_impact_count,
        "net_impact_sentiment": net_impact_sentiment,

        # events
        "has_regulation": has_regulation,
        "has_hack": has_hack,
        "has_institutional": has_institutional,
        "has_macro": has_macro,
        "has_whale": has_whale,
        "has_geopolitical": has_geopolitical,
        "has_listing": has_listing,
        "has_defi": has_defi,
        "has_security": has_security,

        "regulation_count": regulation_count,
        "hack_count": hack_count,
        "institutional_count": institutional_count,
        "macro_count": macro_count,

        # asset mentions
        "btc_mentions": btc_mentions,
        "eth_mentions": eth_mentions,
        "regulation_mentions": regulation_mentions,
        "exchange_mentions": exchange_mentions,
        "market_mentions": market_mentions,
        "defi_mentions": defi_mentions,
        "etf_mentions": etf_mentions,

        # source credibility
        "avg_source_weight": avg_source_weight,
        "max_source_weight": max_source_weight,

        # price / macro (to be filled later)
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
        "btc_dominance": btc_dom,
        "fear_greed_index": None,
        "etf_net_flow_24h": None,
        "dxy": None,
        "gold_price": None,
        "sp500_return_1d": None,
        "us_10y_yield": None,
    }

    return features