import os
import json
import time
import logging
import requests
from typing import List, Dict

# -----------------------------
# LOGGING CONFIG
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------
# PATHS
# -----------------------------
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

INPUT_PATH = os.path.join(REPO_ROOT, "data", "cleaned", "data.json")

# versioned output (prevents overwrite)
timestamp = int(time.time())
OUTPUT_PATH = os.path.join(
    REPO_ROOT,
    "data",
    "enriched",
    f"data_{timestamp}.json"
)

# -----------------------------
# OLLAMA CONFIG
# -----------------------------
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "mistral"

BATCH_SIZE = 5
MAX_RETRIES = 3
RETRY_DELAY = 2


# -----------------------------
# DEFAULT FALLBACK (SCHEMA SAFE)
# -----------------------------
def default_fallback() -> Dict:
    return {
        "is_bitcoin_relevant": False,
        "indirect_btc_impact": False,
        "is_noise": True,
        "asset_mentioned": "other",
        "event_type": "other",
        "event_direction": "neutral",
        "novelty": 1,
        "affected_timeframe": "short",
        "headline_sentiment": 0.0,
        "event_sentiment": 0.0,
        "impact_score": 0,
        "confidence": 0.0,
        "magnitude": 1,
        "reason": "fallback"
    }


# -----------------------------
# PROMPT BUILDER
# -----------------------------
def build_prompt(batch: List[Dict]) -> str:
    instructions = """
You are a Bitcoin market analyst. Score this news article for BTC price prediction.
Return ONLY valid JSON — no explanation outside the JSON.

Return a JSON ARRAY (one object per article).

Structure:
{
  "is_bitcoin_relevant": bool,
  "indirect_btc_impact": bool,
  "is_noise": bool,
  "asset_mentioned": "BTC|ETH|macro|regulation|other",
  "event_type": "regulation|institutional|on-chain|macro|technical|sentiment|hack|other",
  "event_direction": "bullish|bearish|neutral",
  "novelty": 1|2|3,
  "affected_timeframe": "short|mid|long",
  "headline_sentiment": float,
  "event_sentiment": float,
  "impact_score": 0|1|2|3,
  "confidence": float,
  "magnitude": 1|2|3|4|5,
  "reason": "string"
}

Rules:
- Short squeeze = bullish
- ETH-only → irrelevant + noise
- Roundups → noise
- If noise → impact=0 and sentiments=0
"""

    articles_text = ""
    for i, a in enumerate(batch):
        articles_text += f"""
Article {i+1}:
Title: {a.get("title", "")}
Content: {a.get("content", "")[:300]}
"""

    return instructions + "\n" + articles_text


# -----------------------------
# CALL LLM
# -----------------------------
def call_llm(prompt: str) -> str:
    response = requests.post(
        OLLAMA_URL,
        json={
            "model": MODEL,
            "prompt": prompt,
            "stream": False,
        },
        timeout=60
    )

    response.raise_for_status()
    return response.json()["response"]


# -----------------------------
# SAFE PARSER (PARTIAL RECOVERY)
# -----------------------------
def safe_parse(response_text: str, batch_size: int) -> List[Dict]:
    try:
        data = json.loads(response_text)

        if not isinstance(data, list):
            raise ValueError("Not a list")

        fixed = []
        for i in range(batch_size):
            if i < len(data) and isinstance(data[i], dict):
                fixed.append(data[i])
            else:
                fixed.append(default_fallback())

        return fixed

    except Exception as e:
        logger.warning(f"Parse failed: {e}")
        return [default_fallback() for _ in range(batch_size)]


# -----------------------------
# BATCH SUMMARY LOGGING
# -----------------------------
def log_batch_summary(results: List[Dict]):
    avg_impact = sum(r["impact_score"] for r in results) / len(results)
    avg_conf = sum(r["confidence"] for r in results) / len(results)
    noise_ratio = sum(1 for r in results if r["is_noise"]) / len(results)

    logger.info(
        f"[BATCH] avg_impact={avg_impact:.2f} | "
        f"avg_conf={avg_conf:.2f} | "
        f"noise_ratio={noise_ratio:.2f}"
    )


# -----------------------------
# SAVE PARTIAL
# -----------------------------
def save_partial(enriched: List[Dict]):
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)


# -----------------------------
# PROCESS BATCH WITH RETRY
# -----------------------------
def process_batch(batch: List[Dict]) -> List[Dict]:
    prompt = build_prompt(batch)

    for attempt in range(MAX_RETRIES):
        try:
            raw = call_llm(prompt)

            logger.debug(f"RAW LLM OUTPUT:\n{raw[:1000]}")

            parsed = safe_parse(raw, len(batch))
            return parsed

        except Exception as e:
            logger.warning(f"Retry {attempt+1}/{MAX_RETRIES} failed: {e}")
            time.sleep(RETRY_DELAY)

    logger.error("Batch failed → fallback used")
    return [default_fallback() for _ in batch]


# -----------------------------
# MAIN PIPELINE
# -----------------------------
def run_enrichment():
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        articles = json.load(f)

    enriched = []

    for i in range(0, len(articles), BATCH_SIZE):
        batch = articles[i:i + BATCH_SIZE]

        logger.info(f"Processing batch {i//BATCH_SIZE + 1}")

        results = process_batch(batch)
        log_batch_summary(results)

        for article, llm_out in zip(batch, results):
            article.update(llm_out)

            logger.info(
                f"[ENRICHED] "
                f"title='{article['title'][:60]}...' | "
                f"impact={article['impact_score']} | "
                f"event_sent={article['event_sentiment']:.2f} | "
                f"headline_sent={article['headline_sentiment']:.2f} | "
                f"conf={article['confidence']:.2f} | "
                f"noise={article['is_noise']}"
            )

            enriched.append(article)

        # SAVE AFTER EACH BATCH (crash-safe)
        save_partial(enriched)

    logger.info(f"Saved {len(enriched)} articles → {OUTPUT_PATH}")
    return OUTPUT_PATH


# -----------------------------
# ENTRYPOINT
# -----------------------------
if __name__ == "__main__":
    run_enrichment()