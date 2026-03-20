"""
llm/ollama_llm.py
Lead owns this file.

Ollama local LLM implementation — no API key, runs entirely on your machine.
Install Ollama: https://ollama.com
Then pull a model: ollama pull llama3  (or codellama, mistral, etc.)

Good models for SQL generation (local):
  - llama3           (balanced, good general SQL)
  - codellama        (trained on code, best SQL quality locally)
  - mistral          (fast, decent SQL)
  - llama3.1:8b      (good quality, runs on 8GB RAM)

Ollama runs an HTTP server on localhost:11434.
No external calls — perfect for offline dev or when you want zero API costs.
"""
from typing import Any

import requests

from core.interfaces import BaseLLM
from core.config.settings import get_settings


class OllamaLLM(BaseLLM):
    """
    Ollama local LLM via its HTTP API.

    Ollama exposes two endpoints we use:
      POST /api/generate  — single prompt (non-streaming)
      POST /api/chat      — multi-turn conversation (non-streaming)

    Both are called with stream=False so we get a single JSON response.
    """

    def __init__(self):
        s = get_settings()
        self.base_url = s.OLLAMA_BASE_URL.rstrip("/")
        self.model = s.OLLAMA_MODEL
        self.timeout = 120   # local models can be slow — give them 2 minutes

        # Verify Ollama is reachable at startup
        self._verify_connection()

    def _verify_connection(self) -> None:
        """Check that the Ollama server is running and the model is available."""
        try:
            resp = requests.get(f"{self.base_url}/api/tags", timeout=5)
            resp.raise_for_status()
            available = [m["name"] for m in resp.json().get("models", [])]

            # Check if configured model is pulled (match by prefix before ":")
            model_base = self.model.split(":")[0]
            if not any(model_base in name for name in available):
                raise RuntimeError(
                    f"Ollama model '{self.model}' is not pulled. "
                    f"Run: ollama pull {self.model}\n"
                    f"Available models: {available or '(none)'}"
                )
        except requests.ConnectionError:
            raise RuntimeError(
                f"Cannot reach Ollama at {self.base_url}. "
                "Make sure Ollama is running: ollama serve"
            )

    def generate(self, prompt: str, **kwargs: Any) -> str:
        """Send a single prompt, return the model's text response."""
        response = requests.post(
            f"{self.base_url}/api/generate",
            json={
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": kwargs.get("temperature", 0.1),
                    "num_predict": kwargs.get("max_tokens", 1024),
                },
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json().get("response", "").strip()

    def generate_with_history(self, messages: list[dict], **kwargs: Any) -> str:
        """
        Send a conversation history, return the assistant's reply.

        Ollama's /api/chat accepts the same message format as OpenAI:
        [{"role": "system"|"user"|"assistant", "content": "..."}]
        No conversion needed.
        """
        response = requests.post(
            f"{self.base_url}/api/chat",
            json={
                "model": self.model,
                "messages": messages,
                "stream": False,
                "options": {
                    "temperature": kwargs.get("temperature", 0.1),
                    "num_predict": kwargs.get("max_tokens", 1024),
                },
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json().get("message", {}).get("content", "").strip()

    @property
    def provider_name(self) -> str:
        return "ollama"