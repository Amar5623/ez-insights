from core.interfaces import BaseLLM
from core.config.settings import get_settings


def create_llm() -> BaseLLM:
    """
    Returns the correct LLM instance based on LLM_PROVIDER in .env.
    To add a new provider: implement BaseLLM, add a case here.
    """
    provider = get_settings().LLM_PROVIDER.lower()

    if provider == "openai":
        from llm.openai_llm import OpenAILLM
        return OpenAILLM()

    if provider == "gemini":
        from llm.gemini_llm import GeminiLLM
        return GeminiLLM()

    if provider == "ollama":
        from llm.ollama_llm import OllamaLLM
        return OllamaLLM()

    raise ValueError(
        f"Unknown LLM_PROVIDER='{provider}'. "
        "Valid options: openai | gemini | ollama"
    )
