import logging
import yfinance as yf

logger = logging.getLogger(__name__)

def _safe_last(ticker: str, period: str = "2d") -> float | None:
    try:
        data = yf.Ticker(ticker).history(period=period)
        if data.empty or len(data) < 1:
            logger.warning(f"[YF] {ticker} — empty response")
            return None
        return round(float(data["Close"].iloc[-1]), 4)
    except Exception as e:
        logger.warning(f"[YF] {ticker} failed: {e}")
        return None

def _safe_return(ticker: str) -> float | None:
    try:
        data = yf.Ticker(ticker).history(period="3d")
        if data.empty or len(data) < 2:
            logger.warning(f"[YF] {ticker} — not enough rows for return")
            return None
        prev = float(data["Close"].iloc[-2])
        curr = float(data["Close"].iloc[-1])
        return round((curr - prev) / prev, 6)
    except Exception as e:
        logger.warning(f"[YF] {ticker} failed: {e}")
        return None

def get_dxy() -> float | None:
    # DX-Y.NYB is delisted — use futures contract then ETF as fallback
    return _safe_last("DX=F") or _safe_last("UUP")

def get_gold_price() -> float | None:
    return _safe_last("GC=F")

def get_sp500_return() -> float | None:
    return _safe_return("^GSPC")

def get_us10y() -> float | None:
    return _safe_last("^TNX")

def build_market_features() -> dict:
    features = {
        "dxy":            get_dxy(),
        "gold_price":     get_gold_price(),
        "sp500_return_1d":get_sp500_return(),
        "us_10y_yield":   get_us10y(),
    }
    for k, v in features.items():
        logger.info(f"[YF] {k} = {v}")
    return features