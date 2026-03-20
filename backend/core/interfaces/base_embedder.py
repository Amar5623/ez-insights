"""
core/interfaces/base_embedder.py
Lead owns this file — do not modify without team discussion.

Contract every embedding implementation must fulfil.
To add a new embedder (e.g. Cohere, local sentence-transformers):
  1. Create rag/embedders/<name>_embedder.py and subclass BaseEmbedder
  2. Implement all abstract methods
  3. Register in core/factory/embedder_factory.py
  4. Add env vars to settings.py and .env.example

NEVER import a concrete embedder in your module.
Always: from core.interfaces import BaseEmbedder
"""
from abc import ABC, abstractmethod


class BaseEmbedder(ABC):
    """
    Abstract base class for all text embedders.

    Used by:
      - SchemaRetriever (rag/) to embed schema chunks at startup
      - VectorSearchStrategy (strategies/) to embed user questions at query time

    Both callers receive an injected BaseEmbedder instance — they never
    know whether it's OpenAI, Cohere, or anything else.

    Implementation contract:
      - embed()       must return a list[float] of length == self.dimensions
      - embed_batch() must return list[list[float]], same length as input list
      - Both methods must be deterministic for the same input text
        (don't add random noise).
      - Do not cache results internally — the caller decides caching strategy.
    """

    @abstractmethod
    def embed(self, text: str) -> list[float]:
        """
        Embed a single string into a dense float vector.

        Args:
            text: The input string to embed. May be a schema column description,
                  a table name + column list, or a user question.

        Returns:
            A list of floats with length == self.dimensions.
            Example: [0.012, -0.834, 0.221, ...]

        Raises:
            RuntimeError: If the API call fails.
            ValueError:   If text is empty.
        """
        ...

    @abstractmethod
    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """
        Embed a list of strings in one API call (where the provider supports it).

        Args:
            texts: List of strings to embed. At schema indexing time this can
                   contain dozens of table/column descriptions.

        Returns:
            A list of float vectors, same length and order as `texts`.
            Each inner list has length == self.dimensions.

        Raises:
            RuntimeError: If the API call fails.
            ValueError:   If texts is empty.

        Note:
            Prefer this over calling embed() in a loop — most providers offer
            a batch endpoint that is faster and cheaper.
        """
        ...

    @property
    @abstractmethod
    def dimensions(self) -> int:
        """
        The number of dimensions in each vector this embedder produces.

        This value is used by FAISSVectorStore to initialise its index.
        It must be a positive integer and must never change for a given model.

        Examples:
            OpenAI text-embedding-3-small → 1536
            Cohere embed-english-v3.0     → 1024
        """
        ...

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """
        Human-readable provider identifier.

        Returns:
            Lowercase string matching EMBEDDER_PROVIDER env var.
            Examples: 'openai', 'cohere'
        """
        ...