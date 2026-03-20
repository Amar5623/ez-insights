from abc import ABC, abstractmethod


class BaseEmbedder(ABC):
    """
    Abstract base class for all embedding providers.
    Implement this to add a new embedder — register in embedder_factory.py.
    """

    @abstractmethod
    def embed(self, text: str) -> list[float]:
        """Embed a single string, return a float vector."""
        ...

    @abstractmethod
    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed a list of strings, return a list of float vectors."""
        ...

    @property
    @abstractmethod
    def dimensions(self) -> int:
        """Vector dimensions produced by this embedder."""
        ...

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Human-readable provider name e.g. 'openai', 'cohere'."""
        ...
