from abc import ABC, abstractmethod


class BaseVectorStore(ABC):
    """
    Abstract base class for vector stores.
    Used by the RAG layer to store and retrieve schema embeddings.
    Register implementations in vector_store_factory.py.
    """

    @abstractmethod
    def upsert(self, id: str, vector: list[float], metadata: dict) -> None:
        """Insert or update a vector with associated metadata."""
        ...

    @abstractmethod
    def search(
        self, query_vector: list[float], top_k: int = 5
    ) -> list[dict]:
        """
        Find the top_k most similar vectors.
        Returns list of { id, score, metadata } dicts.
        """
        ...

    @abstractmethod
    def delete(self, id: str) -> None:
        """Remove a vector by id."""
        ...

    @abstractmethod
    def clear(self) -> None:
        """Wipe all vectors — used when re-indexing schema."""
        ...

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """e.g. 'faiss', 'pinecone'"""
        ...
