import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

# CONFIG
BINANCE_URL = "https://api.binance.com/api/v3/klines"
FNG_URL = "https://api.alternative.me/fng/"

# HELPERS

def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def fetch_btc_ohlcv(window_start: datetime, interval="1h", limit=100):
    end_time = to_ms(window_start)

    params = {
        "symbol": "BTCUSDT",
        "interval": interval,
        "endTime": end_time,
        "limit": limit
    }

    r = requests.get(BINANCE_URL, params=params)
    data = r.json()

    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "trades", "tbbav", "tbqav", "ignore"
    ])

    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)

    return df


# INDICATORS

def rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def macd(series):
    ema12 = series.ewm(span=12).mean()
    ema26 = series.ewm(span=26).mean()
    macd_line = ema12 - ema26
    signal = macd_line.ewm(span=9).mean()
    return macd_line.iloc[-1] - signal.iloc[-1]


def bollinger_position(series, period=20):
    ma = series.rolling(period).mean()
    std = series.rolling(period).std()
    upper = ma + 2 * std
    lower = ma - 2 * std

    price = series.iloc[-1]
    if upper.iloc[-1] - lower.iloc[-1] == 0:
        return 0.5

    return (price - lower.iloc[-1]) / (upper.iloc[-1] - lower.iloc[-1])


def volume_ratio(volume):
    return volume.iloc[-1] / volume.rolling(20).mean().iloc[-1]


# MACRO DATA

def fetch_fear_greed():
    try:
        r = requests.get(FNG_URL)
        return float(r.json()["data"][0]["value"])
    except:
        return None


# MAIN FUNCTION

def build_market_features(window_start: datetime):
    df = fetch_btc_ohlcv(window_start)

    close = df["close"]
    volume = df["volume"]

    last_close = close.iloc[-1]

    # safe fallback for returns
    def safe_return(n):
        if len(close) < n:
            return None
        return (last_close / close.iloc[-n]) - 1

    features = {
        # OHLCV
        "btc_open": float(df["open"].iloc[-1]),
        "btc_close": float(df["close"].iloc[-1]),
        "btc_high": float(df["high"].iloc[-1]),
        "btc_low": float(df["low"].iloc[-1]),
        "btc_volume": float(df["volume"].iloc[-1]),

        # returns
        "btc_return_6h": safe_return(6),
        "btc_return_24h": safe_return(24),

        # indicators
        "rsi_14": float(rsi(close).iloc[-1]) if len(close) > 14 else None,
        "macd": float(macd(close)) if len(close) > 26 else None,
        "bb_position": float(bollinger_position(close)),
        "volume_ratio": float(volume_ratio(volume)),

        "etf_net_flow_24h": None,       # needs external API
    }

    return features