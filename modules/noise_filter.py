import os
import json
import re
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

timestamp = int(time.time())
OUTPUT_PATH = os.path.join(
    REPO_ROOT,
    "data",
    "filtred",
    "data.json"
)

print("with llama3")
# -----------------------------
# OLLAMA CONFIG
# -----------------------------
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3"
BATCH_SIZE = 1
MAX_RETRIES = 3
RETRY_DELAY = 2

# -----------------------------
# FUNCTIONS (No prompt changes)
# -----------------------------
def default_fallback() -> Dict:
    return {"is_noise": True, "reason": "fallback"}

def build_prompt(batch: List[Dict]) -> str:
    # Instructions omitted for brevity but kept exactly as your original
    instructions = """You are a crypto market news classification engine.

Your task is to determine whether a news article is "noise" or "signal" for cryptocurrency markets.

Return ONLY valid JSON in this format:

{
  "results": [
    {
      "is_noise": true,
      "reason": "short phrase",
      "category": "regulation|macro|hack|adoption|social|exchange|tech|whale|etf|tax|geopolitical|stablecoin|onchain|unknown"
    }
  ]
}

------------------------------------------------------------
DEFINITIONS
------------------------------------------------------------

Noise = information that does NOT directly represent a confirmed market-impacting event.

Signal = factual, confirmed, market-relevant event that can directly impact price, liquidity, or risk.

------------------------------------------------------------
NEWS CATEGORIES (CRITICAL FOR DECISION)
------------------------------------------------------------

1. regulation → laws, SEC, bans, compliance, ETF approval/rejection
2. macro → inflation, CPI, Fed, interest rates, economy
3. hack → exchange hacks, exploits, rug pulls, stolen funds
4. adoption → companies/governments using crypto
5. social → influencers, celebrities, tweets, hype
6. exchange → listings, delistings, liquidity changes
7. tech → forks, upgrades, protocol changes
8. whale → large on-chain transfers, accumulation/distribution
9. etf → ETF filings, approvals, inflows
10. tax → tax policy or reporting rules
11. stablecoin → depegs, stablecoin stress events
12. geopolitical → war, sanctions, crises affecting markets
13. onchain → analytics reports, blockchain metrics insights
14. unknown → cannot classify

------------------------------------------------------------
NOISE RULES (is_noise = true if ANY apply)
------------------------------------------------------------

Mark NOISE if the article contains:
- speculation (will, could, might, expected)
- predictions or forecasts
- opinions or analysis without new factual event
- listicles (top, best, x to watch, ranking articles)
- PR / marketing / sponsored content
- generic summaries of market state
- technical analysis without new event
- sentiment-only content ("investors are optimistic")
- social hype without real on-chain or official event

IMPORTANT:
If ANY speculation exists → ALWAYS noise = true

------------------------------------------------------------
SIGNAL RULES (is_noise = false ONLY if ALL apply)
------------------------------------------------------------

Mark SIGNAL only if:

- a real-world event already happened or is officially confirmed
- has measurable or verifiable impact (price, volume, flow, regulation, hack, listing)
- source is credible (exchange, government, company, on-chain data, major media)

Examples:
- ETF approved/rejected
- hack confirmed
- exchange listing/delisting
- whale movement confirmed
- regulatory action announced
- company bought/accepted crypto
- stablecoin depeg confirmed

------------------------------------------------------------
DECISION PRIORITY (VERY IMPORTANT)

1. If speculative OR listicle OR opinion → NOISE (always)
2. If mixed (fact + speculation) → NOISE
3. If confirmed event → SIGNAL
4. If unclear → NOISE

------------------------------------------------------------
REASON RULES

- max 6 words
- must explain decision briefly
- no punctuation needed

------------------------------------------------------------
OUTPUT RULES

- Return ONLY JSON
- No explanations outside JSON
- One object per article""" 
    articles_text = ""
    for i, a in enumerate(batch):
        articles_text += f"\n{i}:\ntitle: {a.get('title','')}\ncontent: {a.get('content','')[:250]}\n"
    return instructions + "\nARTICLES:\n" + articles_text

def call_llm(prompt: str) -> str:
    response = requests.post(
        OLLAMA_URL,
        json={
            "model": MODEL,
            "prompt": prompt,
            "stream": False,
            "format": "json"
        },
        timeout=60
    )
    response.raise_for_status()
    return response.json()["response"]

def safe_parse(response_text: str, batch_size: int) -> List[Dict]:
    try:
        # 1. extract JSON block more safely
        match = re.search(r"\{[\s\S]*\}", response_text)
        if not match:
            raise ValueError("No JSON found")

        cleaned = match.group()

        # 2. fix common LLM mistakes
        cleaned = cleaned.replace("'", '"')

        data = json.loads(cleaned)

        results = data.get("results", [])

        fixed = []
        for i in range(batch_size):
            if i < len(results):
                r = results[i]
                fixed.append({
                    "is_noise": bool(r.get("is_noise", True)),
                    "reason": str(r.get("reason", "unknown"))[:30],
                    "category": r.get("category", "unknown")
                })
            else:
                fixed.append(default_fallback())

        return fixed

    except Exception as e:
        logger.error("RAW RESPONSE WAS:")
        logger.error(response_text)
        logger.error(f"PARSE ERROR: {e}")
        return [default_fallback() for _ in range(batch_size)]

def process_batch(batch: List[Dict]) -> List[Dict]:
    prompt = build_prompt(batch)
    for attempt in range(MAX_RETRIES):
        try:
            raw = call_llm(prompt)
            return safe_parse(raw, len(batch))
        except Exception as e:
            logger.warning(f"Retry {attempt+1}/{MAX_RETRIES} failed: {e}")
            time.sleep(RETRY_DELAY)
    return [default_fallback() for _ in batch]

def save_partial(enriched: List[Dict]):
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

# -----------------------------
# UPDATED MAIN PIPELINE
# -----------------------------
def run_filter():
    if not os.path.exists(INPUT_PATH):
        logger.error(f"Input file not found at {INPUT_PATH}")
        return

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        articles = json.load(f)

    enriched = []
    total_articles = len(articles)

    for i in range(0, total_articles, BATCH_SIZE):
        batch = articles[i:i + BATCH_SIZE]
        logger.info(f"Processing batch {i//BATCH_SIZE + 1} (Articles {i} to {min(i+BATCH_SIZE, total_articles)})")

        results = process_batch(batch)

        for article, llm_out in zip(batch, results):
            # Update article dictionary with LLM results
            article.update(llm_out)
            
            # --- DISPLAY INFO ---
            print("-" * 50)
            print(f"TITLE:   {article.get('title', 'N/A')}")
            # Show first 150 chars of content for readability
            content_snippet = article.get('content', 'N/A')[:150].replace('\n', ' ')
            print(f"CONTENT: {content_snippet}...")
            print(f"RESULT:  {'[NOISE]' if article['is_noise'] else '[SIGNAL]'}")
            print(f"REASON:  {article['reason']}")
            print("-" * 50)

            enriched.append(article)

        # Save after each batch for safety
        save_partial(enriched)

    logger.info(f"Process complete. Saved {len(enriched)} articles to: {OUTPUT_PATH}")
    return OUTPUT_PATH

if __name__ == "__main__":
    run_filter()