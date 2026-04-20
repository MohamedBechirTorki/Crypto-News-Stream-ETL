import os
import json
import re
import time
import logging
import requests
from typing import List, Dict
import re

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
        "is_noise": True,
        "reason": "fallback"
    }


# -----------------------------
# PROMPT BUILDER
# -----------------------------
def build_prompt(batch: List[Dict]) -> str:
    instructions = """
You are a financial news classifier.

Return ONLY valid JSON in this format:

{
  "results": [
    {"is_noise": true, "reason": "short phrase"},
    {"is_noise": false, "reason": "short phrase"}
  ]
}

STRICT RULES:

1. Mark is_noise = true if:
- Speculation or prediction ("will", "could", "might", "expected")
- Opinions or analysis without new facts
- Listicles ("top", "best", "X to watch")
- PR, sponsored, or promotional content
- Generic summaries ("what happened today")

2. Mark is_noise = false ONLY if:
- A confirmed event already happened (buy, sell, hack, regulation, partnership)
- On-chain activity (large transfer, whale movement)
- Official announcements (ETF approval, company action)
- Measurable market data (liquidations, inflows, volume spike)

3. PRIORITY:
If ANY speculation or listicle is present → is_noise = true
EVEN if it mentions real concepts (ETF, BTC, etc.)

4. Keep reason under 6 words.

5. Output ONLY the JSON array. No text.
"""

    articles_text = ""
    for i, a in enumerate(batch):
        articles_text += f"""
{i}:
title: {a.get("title","")}
content: {a.get("content","")[:250]}
"""

    return instructions + "\nARTICLES:\n" + articles_text

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
        match = re.search(r"\{.*\}", response_text, re.DOTALL)
        if match:
            response_text = match.group()
        data = json.loads(response_text)

        results = data.get("results", [])

        fixed = []
        for i in range(batch_size):
            if i < len(results) and isinstance(results[i], dict):
                fixed.append({
                    "is_noise": bool(results[i].get("is_noise", True)),
                    "reason": str(results[i].get("reason", "unknown"))[:30]
                })
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
    noise_ratio = sum(1 for r in results if r["is_noise"]) / len(results)

    logger.info(
        f"[BATCH] noise_ratio={noise_ratio:.2f}"
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

    logger.error(f"Batch failed → NO LLM OUTPUT USED | returning safe defaults")
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
            print(article["title"], "is_noise", article["is_noise"], "reason", article["reason"])  # DEBUG
            logger.info(
                f"[ENRICHED] "
                f"title='{article['title'][:60]}...' | "
                f"noise={article['is_noise']} | "
                f"reason='{article['reason']}'"
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