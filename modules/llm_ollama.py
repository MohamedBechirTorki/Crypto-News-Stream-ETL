import requests
import logging
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OllamaClient:
    def __init__(
        self,
        model: str = "llama3.1",
        host: str = "http://host.docker.internal:11434",
        timeout: int = 60,
    ):
        self.model = model
        self.url = f"{host}/api/generate"
        self.timeout = timeout

    def ask(self, prompt: str) -> str:
        """Single prompt inference"""
        try:
            payload = {
                "model": self.model,
                "prompt": prompt,
                "stream": False
            }

            response = requests.post(self.url, json=payload, timeout=self.timeout)
            response.raise_for_status()

            return response.json().get("response", "")

        except requests.exceptions.RequestException as e:
            logger.error(f"Ollama request failed: {e}")
            return ""

    def ask_json(self, prompt: str) -> Dict[str, Any]:
        """Ask and try to parse JSON response"""
        raw = self.ask(prompt)

        try:
            import json
            return json.loads(raw)
        except Exception:
            logger.warning("Failed to parse JSON from model output")
            return {"raw": raw}

    def batch_ask(self, prompts: List[str]) -> List[str]:
        """Sequential batch processing (safe for local Ollama)"""
        results = []

        for i, p in enumerate(prompts):
            logger.info(f"Processing {i+1}/{len(prompts)}")
            results.append(self.ask(p))

        return results