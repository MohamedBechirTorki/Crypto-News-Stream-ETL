import yfinance as yf

def get_dxy():
    dxy = yf.Ticker("DX-Y.NYB")
    data = dxy.history(period="1d")
    return float(data["Close"].iloc[-1])

def get_gold_price():
    gold = yf.Ticker("GC=F")
    data = gold.history(period="1d")
    return float(data["Close"].iloc[-1])

def get_sp500_return():
    sp = yf.Ticker("^GSPC")
    data = sp.history(period="2d")
    
    prev = data["Close"].iloc[-2]
    curr = data["Close"].iloc[-1]
    
    return (curr - prev) / prev

def get_us10y():
    tnx = yf.Ticker("^TNX")
    data = tnx.history(period="1d")
    return float(data["Close"].iloc[-1])

def build_market_features() :
    features = {
        "dxy": get_dxy(),
        "gold_price": get_gold_price(),
        "sp500_return_1d": get_sp500_return(),
        "us_10y_yield": get_us10y(),
    }
    return features