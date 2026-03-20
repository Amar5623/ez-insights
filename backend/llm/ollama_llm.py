from typing import Any
import requests
from core.interfaces import BaseLLM
from core.config.settings import get_settings


class OllamaLLM(BaseLLM):
    def __init__(self):
        s = get_settings()
        self.base_url = s.OLLAMA_BASE_URL
        self.model = s.OLLAMA_MODEL

    def generate(self, prompt: str, **kwargs: Any) -> str:
        response = requests.post(
            f"{self.base_url}/api/generate",
            json={"model": self.model, "prompt": prompt, "stream": False},
        )
        response.raise_for_status()
        return response.json()["response"]

    def generate_with_history(self, messages: list[dict], **kwargs: Any) -> str:
        response = requests.post(
            f"{self.base_url}/api/chat",
            json={"model": self.model, "messages": messages, "stream": False},
        )
        response.raise_for_status()
        return response.json()["message"]["content"]

    @property
    def provider_name(self) -> str:
        return "ollama"
