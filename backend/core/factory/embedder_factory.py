"""
core/factory/embedder_factory.py
Lead owns this file.

Reads EMBEDDER_PROVIDER from .env and returns the correct BaseEmbedder instance.
To add a new embedder: implement BaseEmbedder, add a case here.
"""
from core.interfaces import BaseEmbedder
from core.config.settings import get_settings


def create_embedder() -> BaseEmbedder:
    """
    Returns the correct embedder based on EMBEDDER_PROVIDER in .env.

    Options:
      nomic  → NomicEmbedder  (default — local via Ollama, free, 768 dims)
      cohere → CohereEmbedder (cloud, 1024 dims, needs COHERE_API_KEY)
      gemma  → EmbeddingGemmaEmbedder (Hugging Face, 768 dims, needs HF_TOKEN)
    """
    provider = get_settings().EMBEDDER_PROVIDER.lower()

    if provider == "nomic":
        from rag.embedders.nomic_embedder import NomicEmbedder
        return NomicEmbedder()

    if provider == "cohere":
        from rag.embedders.cohere_embedder import CohereEmbedder
        return CohereEmbedder()

    if provider == "gemma":
        from rag.embedders.gemma_embedder import EmbeddingGemmaEmbedder
        return EmbeddingGemmaEmbedder()

    raise ValueError(
        f"Unknown EMBEDDER_PROVIDER='{provider}'. "
        "Valid options: nomic | cohere | gemma"
    )