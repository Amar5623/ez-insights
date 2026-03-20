"""
core/interfaces/base_vector_store.py
Lead owns this file — do not modify without team discussion.

Contract every vector store implementation must fulfil.
To add a new store (e.g. Weaviate, Qdrant):
  1. Create rag/vector_stores/<n>_store.py, subclass BaseVectorStore
  2. Implement every @abstractmethod
  3. Register in core/factory/vector_store_factory.py
  4. Add env vars to settings.py and .env.example

Used by:
  - SchemaRetriever  (rag/)          — upsert schema chunks at startup
  - VectorSearchStrategy (strategies/) — search at query time

Both callers receive an injected BaseVectorStore — they never know if
it is FAISS (local) or Pinecone (cloud).
"""
from abc import ABC, abstractmethod


class BaseVectorStore(ABC):
    """
    Abstract base class for vector stores.

    Implementations:
      rag/vector_stores/faiss_store.py    → FAISSVectorStore    (Lead)
      rag/vector_stores/pinecone_store.py → PineconeVectorStore (Lead)

    Lifecycle:
        store = create_vector_store()       # factory call at startup
        store.clear()                       # wipe old schema embeddings
        store.upsert(id, vector, metadata)  # index each schema chunk
        # ... at query time ...
        hits = store.search(query_vector, top_k=5)

    Vector format:
        All vectors are list[float]. The dimensionality must match the
        embedder that produces them (see BaseEmbedder.dimensions).
        FAISSVectorStore initialises its index with the embedder's dimension.

    Metadata format:
        Each vector carries arbitrary metadata as a plain dict.
        SchemaRetriever stores at minimum:
            {
                "table": "products",
                "columns": "id, name, price, category",
                "text": "products table: id int, name varchar, price decimal"
            }
        The search() result returns this metadata alongside each hit.
    """

    @abstractmethod
    def upsert(self, id: str, vector: list[float], metadata: dict) -> None:
        """
        Insert or update a vector with associated metadata.

        If a vector with the given id already exists, overwrite it.
        This is used during schema re-indexing to refresh stale embeddings.

        Args:
            id:       Unique string identifier for this vector.
                      SchemaRetriever uses table/collection name as id,
                      e.g. "products", "orders", "users".

            vector:   Dense float embedding, length == embedder.dimensions.

            metadata: Arbitrary dict stored alongside the vector.
                      Must be JSON-serialisable (no ObjectIds, no datetimes).
                      Retrieved verbatim from search() results.

        Raises:
            RuntimeError: If the upsert fails (e.g. Pinecone API error,
                          FAISS index dimension mismatch).
            ValueError:   If vector length doesn't match the index dimension.
        """
        ...

    @abstractmethod
    def search(
        self, query_vector: list[float], top_k: int = 5
    ) -> list[dict]:
        """
        Find the top_k most similar vectors to query_vector.

        This is called at query time — latency matters. Keep it fast.

        Args:
            query_vector: Embedded user question, same dimension as stored vectors.
            top_k:        Number of nearest neighbours to return.
                          Default 5 — SchemaRetriever passes this from its
                          own top_k setting.

        Returns:
            List of result dicts, sorted by similarity descending (best first):
            [
                {
                    "id":       "products",
                    "score":    0.92,        # cosine similarity or L2 distance
                    "metadata": {
                        "table":   "products",
                        "columns": "id, name, price",
                        "text":    "products table: ..."
                    }
                },
                ...
            ]
            Returns [] if the index is empty or no neighbours found.
            Never returns None.

        Score convention:
            Higher is better (cosine similarity). FAISS uses L2 distance
            internally — convert to similarity before returning so callers
            always see the same convention regardless of backend.

        Raises:
            RuntimeError: If the search fails.
            ValueError:   If query_vector has wrong dimensions.
        """
        ...

    @abstractmethod
    def delete(self, id: str) -> None:
        """
        Remove a vector by id.

        Used when a table/collection is dropped from the schema and we want
        to remove its embedding from the index.

        Args:
            id: The id originally passed to upsert().

        Raises:
            KeyError:     If id does not exist in the store.
            RuntimeError: On backend failure.
        """
        ...

    @abstractmethod
    def clear(self) -> None:
        """
        Delete all vectors from the store.

        Called by SchemaRetriever before re-indexing the full schema.
        After clear(), the store must behave as if it was just initialised.

        Raises:
            RuntimeError: On backend failure.
        """
        ...

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """
        Human-readable store identifier.

        Returns:
            Lowercase string matching VECTOR_STORE env var.
            Examples: 'faiss', 'pinecone'
        """
        ...