import os
import json
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import feedparser
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# RSS SOURCES (later move to config/sources.yaml)
RSS_FEEDS = {
    "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph": "https://cointelegraph.com/rss",
    "decrypt": "https://decrypt.co/feed",
    "bitcoinist": "https://bitcoinist.com/feed/",
    "cryptoslate": "https://cryptoslate.com/feed/",
    "newsbtc": "https://www.newsbtc.com/feed/",
    "theblock": "https://www.theblock.co/rss.xml",
    "bitcoin_mag": "https://bitcoinmagazine.com/.rss/full/",
    "cryptopanic": "https://cryptopanic.com/news/rss/",
    "coinjournal": "https://coinjournal.net/feed/",
    "investing_crypto": "https://www.investing.com/rss/news_301.rss",
    "seeking_alpha": "https://seekingalpha.com/tag/cryptocurrency.xml",
    "marketwatch": "https://feeds.marketwatch.com/marketwatch/topstories/",
    "cnbc_crypto": "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "forbes_crypto": "https://www.forbes.com/crypto-blockchain/feed/",
    "messari": "https://messari.io/rss",
    "chainalysis": "https://blog.chainalysis.com/rss/",
    "elliptic": "https://www.elliptic.co/blog/rss.xml",
    "defiant": "https://thedefiant.io/feed",
    "coinmetrics": "https://coinmetrics.io/blog/rss/",
    "ark_invest": "https://ark-invest.com/feed/",
    "glassnode": "https://insights.glassnode.com/rss/",
    "santiment": "https://santiment.net/blog/feed",
    "binance_blog": "https://www.binance.com/en/blog/rss",
    "coinbase_blog": "https://blog.coinbase.com/feed",
    "kraken_blog": "https://blog.kraken.com/feed",
    "bitfinex_blog": "https://blog.bitfinex.com/feed/",
    "okx_blog": "https://www.okx.com/feed",
    "bybit_blog": "https://blog.bybit.com/en/feed/",
    "kucoin_blog": "https://www.kucoin.com/blog/rss",
    "ethereum_research": "https://ethresear.ch/latest.rss",
    "solana_blog": "https://solana.com/news/rss.xml",
    "polkadot_blog": "https://polkadot.network/blog/feed/",
    "chainlink_blog": "https://blog.chain.link/rss/",
    "crypto_news": "https://crypto.news/feed/",
    "ambcrypto": "https://ambcrypto.com/feed/",
    "beincrypto": "https://beincrypto.com/feed/",
    "coinspeaker": "https://www.coinspeaker.com/feed/",
    "blockchainreporter": "https://blockchainreporter.net/feed/",
    "cryptodaily": "https://cryptodaily.co.uk/feed",
    "dailycoin": "https://dailycoin.com/feed/",
    "coinmarketcap": "https://coinmarketcap.com/headlines/news/rss/",
    "coingecko_blog": "https://blog.coingecko.com/feed/",
    "sec_press_releases": "https://www.sec.gov/news/pressreleases.rss",
    "sec_litigation": "https://www.sec.gov/litigation/litreleases.rss",
    "cftc_news": "https://www.cftc.gov/PressRoom/PressReleases/rss",
    "eu_commission_finance": "https://ec.europa.eu/commission/presscorner/api/rss?language=en&filter=crypto",
    "bank_of_england": "https://www.bankofengland.co.uk/rss/news",
    "imf_blog": "https://www.imf.org/en/News/rss",
    "federal_reserve": "https://www.federalreserve.gov/feeds/press_all.xml",
    "blackrock_newsroom": "https://www.blackrock.com/us/individual/rss/press-releases",
    "fidelity_news": "https://www.fidelity.com/news/rss",
    "grayscale_news": "https://www.grayscale.com/rss.xml",
    "ark_etf_updates": "https://ark-funds.com/feed/",
    "vaneck_news": "https://www.vaneck.com/us/en/rss/",
    "cme_group": "https://www.cmegroup.com/rss/news.rss",
    "ice_markets": "https://ir.theice.com/rss/news-releases.xml",
    "coindesk_markets": "https://www.coindesk.com/arc/outboundfeeds/rss/category/markets/",
    "cert_usa": "https://www.cisa.gov/cybersecurity-advisories/all.xml",
    "mitre_attacks": "https://attack.mitre.org/resources/rss/",
    "rekt_news": "https://rekt.news/rss/",
    "hackernews_security": "https://feeds.feedburner.com/TheHackersNews",
    "ap_news_business": "https://apnews.com/rss/business",
    "ft_markets": "https://www.ft.com/?format=rss",
    "aave_blog": "https://aave.com/blog/rss.xml",
    "uniswap_blog": "https://blog.uniswap.org/rss.xml",
    "curve_blog": "https://curve.fi/blog/rss.xml",
    "defillama_blog": "https://defillama.com/blog/rss.xml",
    "l2beat_updates": "https://l2beat.com/rss.xml",
    "coinglass_blog": "https://www.coinglass.com/rss",
    "deribit_insights": "https://insights.deribit.com/feed/",
}

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_RAW_DIR = os.path.join(REPO_ROOT, "data", "raw")
DATA_RAW_PATH = os.path.join(REPO_ROOT, "data", "raw", "raw.json")


def fetch_rss_feed(source_name: str, url: str, limit: int = 50):
    try:
        response = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code != 200:
            return []

        feed = feedparser.parse(response.content)

        articles = []
        for entry in feed.entries[:limit]:
            articles.append({
                "title": entry.get("title", ""),
                "content": entry.get("summary", ""),
                "url": entry.get("link", ""),
                "source": source_name,
                "raw_published_at": entry.get("published") or entry.get("updated"),
            })

        return articles

    except Exception as e:
        logger.error(f"{source_name} failed: {e}")
        return []

def fetch_all(limit=50):
    all_articles = []

    def worker(item):
        name, url = item
        return fetch_rss_feed(name, url, limit)

    with ThreadPoolExecutor(max_workers=6) as executor:
        results = executor.map(worker, RSS_FEEDS.items())

    for res in results:
        all_articles.extend(res)

    return all_articles


def save_raw(articles):
    os.makedirs(os.path.dirname(DATA_RAW_PATH), exist_ok=True)

    with open(DATA_RAW_PATH, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    return DATA_RAW_PATH

def run_fetch():
    raw = fetch_all()
    path = save_raw(raw)

    logger.info(f"Saved {len(raw)} raw articles → {path}")
    return path

if __name__ == "__main__":
    run_fetch()