from core.interfaces import BaseEmbedder
from core.config.settings import get_settings


def create_embedder() -> BaseEmbedder:
    """
    Returns the correct embedder based on EMBEDDER_PROVIDER in .env.
    To add a new embedder: implement BaseEmbedder, add a case here.
    """
    provider = get_settings().EMBEDDER_PROVIDER.lower()

    if provider == "openai":
        from rag.embedders.openai_embedder import OpenAIEmbedder
        return OpenAIEmbedder()

    if provider == "cohere":
        from rag.embedders.cohere_embedder import CohereEmbedder
        return CohereEmbedder()

    raise ValueError(
        f"Unknown EMBEDDER_PROVIDER='{provider}'. "
        "Valid options: openai | cohere"
    )
