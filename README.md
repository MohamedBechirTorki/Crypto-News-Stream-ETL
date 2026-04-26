# Crypto News Stream ETL

A production-grade crypto news intelligence pipeline that collects, filters, scores, and stores news articles every hour using Apache Airflow. Built to generate labeled training data for a BTC price prediction model.

---

## Architecture

```
fetch_rss_data → clean_data → noise_filter → enrich_data → window_features_aggregation → save_to_storage
```

Each stage runs as an Airflow PythonOperator on a 1-hour schedule.

---

## What It Does

- Fetches articles from 55+ RSS sources every hour
- Filters noise using a local LLM (Llama 3 via Ollama)
- Scores financial sentiment using FinBERT
- Classifies event types and asset mentions with rule-based logic
- Aggregates window-level features (sentiment, impact, macro, price)
- Stores structured features in PostgreSQL for model training

---

## Requirements

- Python 3.10
- Docker + Docker Compose
- [Ollama](https://ollama.com) (local LLM inference)
- 8GB RAM minimum
- No GPU required

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/mohamedbechirtorki/crypto-news-stream-etl.git
cd crypto-news-stream-etl
```

### 2. Install Ollama and pull Llama 3

```bash
# Install Ollama
curl -fsSL https://ollama.com/install.sh | sh

# Pull the model used for noise filtering
ollama pull llama3

# Verify it works
ollama run llama3 "is this crypto news or noise: Bitcoin hits $80k"
```

Ollama must be running before starting the pipeline:

```bash
ollama serve
```

### 3. Configure environment variables

Copy the example env file and fill in your values:

```bash
cp .env.example .env
```

Edit `.env`:

```
PG_HOST=localhost
PG_PORT=5432
PG_DB=crypto_etl
PG_USER=postgres
PG_PASSWORD=your_password_here
```

### 4. Start services with Docker Compose

```bash
docker-compose up --build -d
```

This starts:
- Apache Airflow (webserver + scheduler)
- PostgreSQL

### 5. Access Airflow UI

Open [http://localhost:8080](http://localhost:8080)

Default credentials:
```
username: airflow
password: airflow
```

Enable the `crypto_news_pipeline` DAG to start the pipeline.

---

## Project Structure

```
crypto-news-stream-etl/
├── dags/
│   └── crypto_pipeline.py        # Airflow DAG definition
├── modules/
│   ├── fetcher.py                 # RSS ingestion
│   ├── cleaner.py                 # Normalization + dedup
│   ├── noise_filter.py            # LLM noise detection (Llama 3)
│   ├── enricher.py                # FinBERT sentiment + rule-based scoring
│   ├── storage.py                 # PostgreSQL storage
│   └── features_agg/
│       ├── pipeline.py            # Window aggregation orchestrator
│       ├── window_computer.py     # Feature computation
│       ├── market_features.py     # Binance price + technical indicators
│       ├── yahoo_finance.py       # Macro features (DXY, gold, SP500)
│       └── btc_dominance.py       # BTC dominance from CoinGecko
├── data/
│   ├── raw/                       # Raw RSS output
│   ├── cleaned/                   # Normalized articles
│   ├── filtred/                   # After noise gate
│   ├── enriched/                  # Scored articles
│   └── features/                  # Window feature vectors + history
├── .env                           # Environment variables (not committed)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Feature Output (per 1-hour window)

Each pipeline run produces one row in `window_features` table:

| Category | Features |
|---|---|
| Volume | `article_count`, `total_raw`, `noise_ratio` |
| Sentiment | `avg_sentiment`, `max_sentiment`, `sentiment_std`, `sentiment_momentum` |
| Impact | `avg_impact`, `max_impact`, `high_impact_count`, `net_impact_sentiment` |
| Events | `has_regulation`, `has_hack`, `has_macro`, `has_institutional`, ... |
| Assets | `btc_mentions`, `eth_mentions`, `etf_mentions`, ... |
| Price | `btc_open`, `btc_close`, `rsi_14`, `macd`, `bb_position` |
| Macro | `dxy`, `gold_price`, `sp500_return_1d`, `us_10y_yield` |
| Market | `btc_dominance`, `fear_greed_index`, `etf_net_flow_24h` |

---

## RSS Sources

| Tier | Sources |
|---|---|
| Tier 1 — Regulatory | SEC, CFTC, Federal Reserve, IMF |
| Tier 2 — Institutional | Chainalysis, Glassnode, Messari, ARK Invest |
| Tier 3 — Journalism | CoinDesk, The Block, Bitcoin Magazine |
| Tier 4 — Aggregators | CryptoPanic, CoinTelegraph, Decrypt |

---

## Models Used

| Model | Purpose | Size |
|---|---|---|
| Llama 3 (via Ollama) | Noise detection | ~5GB |
| ProsusAI/FinBERT | Financial sentiment scoring | ~500MB |

---

## Notes

- Ollama must be running separately from Docker (`ollama serve`)
- FinBERT runs on CPU — no GPU required
- Pipeline is idempotent — safe to rerun any window
- `ON CONFLICT DO UPDATE` prevents duplicate rows in PostgreSQL
