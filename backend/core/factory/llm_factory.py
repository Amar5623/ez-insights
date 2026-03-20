"""
core/factory/llm_factory.py
Lead owns this file.

Reads LLM_PROVIDER from .env and returns the correct BaseLLM instance.
To add a new provider: implement BaseLLM, add a case here.
"""
from core.interfaces import BaseLLM
from core.config.settings import get_settings


def create_llm() -> BaseLLM:
    """
    Returns the correct LLM instance based on LLM_PROVIDER in .env.

    Options:
      groq   → GroqLLM   (default — fast, free, OpenAI-compatible API)
      gemini → GeminiLLM (Google, 1500 req/day free on Flash)
      ollama → OllamaLLM (local, no API key, needs Ollama running)
    """
    provider = get_settings().LLM_PROVIDER.lower()

    if provider == "groq":
        from llm.groq_llm import GroqLLM
        return GroqLLM()

    if provider == "gemini":
        from llm.gemini_llm import GeminiLLM
        return GeminiLLM()

    if provider == "ollama":
        from llm.ollama_llm import OllamaLLM
        return OllamaLLM()

    raise ValueError(
        f"Unknown LLM_PROVIDER='{provider}'. "
        "Valid options: groq | gemini | ollama"
    )