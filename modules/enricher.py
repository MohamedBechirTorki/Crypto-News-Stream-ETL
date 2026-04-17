import os
import json
import time
import logging
import requests
from typing import List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

INPUT_PATH = os.path.join(REPO_ROOT, "data", "cleaned", "data.json")
OUTPUT_PATH = os.path.join(REPO_ROOT, "data", "enriched", "data.json")

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3.1"

BATCH_SIZE = 5
MAX_RETRIES = 3
RETRY_DELAY = 2


# -----------------------------
# STRICT PROMPT (VERY IMPORTANT)
# -----------------------------
def build_prompt(batch: List[Dict]) -> str:
    instructions = """
You are a financial news classifier for crypto markets.

Your task:
For each article, return STRICT JSON with this schema:

[
  {
    "event_type": "one of [price_movement, regulation, adoption, partnership, hack_exploit, legal, macro, institutional, technology, other]",
    "impact_score": integer (0 to 3),
    "sentiment_score": integer (-2 to 2),
    "confidence": float (0 to 1),
    "is_noise": boolean
  }
]

Rules:
- Output ONLY valid JSON array
- NO explanations
- NO comments
- Keep order SAME as input
- If unsure → low confidence
- If article is generic/analysis → is_noise = true
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
# CALL OLLAMA
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
# SAFE PARSER
# -----------------------------
def safe_parse(response_text: str, batch_size: int) -> List[Dict]:
    try:
        data = json.loads(response_text)

        if not isinstance(data, list):
            raise ValueError("Not a list")

        # ensure same length
        if len(data) != batch_size:
            raise ValueError("Length mismatch")

        return data

    except Exception as e:
        logger.warning(f"Parse failed: {e}")

        # fallback default
        return [
            {
                "event_type": "other",
                "impact_score": 0,
                "sentiment_score": 0,
                "confidence": 0.0,
                "is_noise": True
            }
            for _ in range(batch_size)
        ]


# -----------------------------
# RETRY LOGIC
# -----------------------------
def process_batch(batch: List[Dict]) -> List[Dict]:
    prompt = build_prompt(batch)

    for attempt in range(MAX_RETRIES):
        try:
            raw = call_llm(prompt)
            parsed = safe_parse(raw, len(batch))
            return parsed

        except Exception as e:
            logger.warning(f"Retry {attempt+1}/{MAX_RETRIES} failed: {e}")
            time.sleep(RETRY_DELAY)

    logger.error("Batch failed completely → fallback")
    return [
        {
            "event_type": "other",
            "impact_score": 0,
            "sentiment_score": 0,
            "confidence": 0.0,
            "is_noise": True
        }
        for _ in batch
    ]


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

        for article, llm_out in zip(batch, results):
            article.update(llm_out)
            enriched.append(article)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(enriched)} enriched articles → {OUTPUT_PATH}")
    return OUTPUT_PATH


if __name__ == "__main__":
    run_enrichment()