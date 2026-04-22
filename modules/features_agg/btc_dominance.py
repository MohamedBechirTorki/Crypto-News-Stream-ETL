import requests

def get_btc_dominance():
    url = "https://api.coingecko.com/api/v3/global"
    data = requests.get(url).json()
    return data["data"]["market_cap_percentage"]["btc"]

