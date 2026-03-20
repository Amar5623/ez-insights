"""
rag/vector_stores/pinecone_store.py
Lead owns this file.

Pinecone cloud vector store — production-grade hosted option.
Use this when deploying to production with a large schema (100+ tables)
where FAISS's in-memory approach becomes a concern.

For development, FAISS is recommended — it needs no account or network call.
Switch to Pinecone by setting VECTOR_STORE=pinecone in .env.

Setup (one-time):
  1. Create a free account at https://app.pinecone.io
  2. Create an index with the correct dimensions:
       - 1536 if EMBEDDER_PROVIDER=openai (text-embedding-3-small)
       - 1024 if EMBEDDER_PROVIDER=cohere (embed-english-v3.0)
     Metric: cosine
  3. Copy your API key and index name into .env

Install: pip install pinecone-client
"""
import logging

from core.interfaces import BaseVectorStore
from core.config.settings import get_settings

logger = logging.getLogger("nlsql.pinecone")


class PineconeVectorStore(BaseVectorStore):
    """
    Pinecone vector store via the official pinecone-client SDK.

    Every operation is a real HTTP call to Pinecone's API.
    Latency is ~50-200ms per call depending on region — acceptable for
    schema retrieval at startup, fine for per-query search.

    Namespace:
        All vectors are stored in the default namespace ("").
        If you want to support multiple databases or tenants in one index,
        extend this to accept a namespace parameter.
    """

    def __init__(self):
        from pinecone import Pinecone, ServerlessSpec
        s = get_settings()

        if not s.PINECONE_API_KEY:
            raise ValueError(
                "PINECONE_API_KEY is not set. "
                "Get a free key at https://app.pinecone.io and add it to .env"
            )
        if not s.PINECONE_INDEX:
            raise ValueError(
                "PINECONE_INDEX is not set. "
                "Create an index at https://app.pinecone.io and set its name in .env"
            )

        self._pc = Pinecone(api_key=s.PINECONE_API_KEY)
        self._index_name = s.PINECONE_INDEX

        # Verify index exists — fail fast with a clear message
        existing = [idx.name for idx in self._pc.list_indexes()]
        if self._index_name not in existing:
            raise RuntimeError(
                f"Pinecone index '{self._index_name}' does not exist. "
                f"Existing indexes: {existing or '(none)'}. "
                f"Create it at https://app.pinecone.io with the correct "
                f"dimensions for your embedder (1536 for OpenAI, 1024 for Cohere)."
            )

        self._index = self._pc.Index(self._index_name)

        # Log index stats at startup for visibility
        try:
            stats = self._index.describe_index_stats()
            logger.info(
                f"[pinecone] Connected to index '{self._index_name}' — "
                f"{stats.total_vector_count} vectors, "
                f"dim={stats.dimension}"
            )
        except Exception as e:
            logger.warning(f"[pinecone] Could not fetch index stats: {e}")

    # ── Public interface ──────────────────────────────────────────────────────

    def upsert(self, id: str, vector: list[float], metadata: dict) -> None:
        """
        Insert or update a vector. Pinecone upsert is idempotent by id.

        Args:
            id:       Unique string key, e.g. table name "products"
            vector:   Float embedding list
            metadata: Dict to store alongside — must be JSON-serialisable
                      and values must be str, int, float, bool, or list of str.
                      Pinecone does not support nested dicts in metadata.
        """
        if not id:
            raise ValueError("id cannot be empty")
        if not vector:
            raise ValueError("vector cannot be empty")

        # Flatten metadata values that Pinecone can't handle
        safe_metadata = self._sanitise_metadata(metadata)

        try:
            self._index.upsert(
                vectors=[{
                    "id": id,
                    "values": vector,
                    "metadata": safe_metadata,
                }]
            )
            logger.debug(f"[pinecone] Upserted id='{id}'")

        except Exception as e:
            raise RuntimeError(f"[pinecone] upsert failed for id='{id}': {e}") from e

    def search(
        self, query_vector: list[float], top_k: int = 5
    ) -> list[dict]:
        """
        Find the top_k most similar vectors by cosine similarity.

        Returns:
            List of dicts sorted by score descending:
            [{"id": "products", "score": 0.92, "metadata": {...}}, ...]
        """
        if not query_vector:
            raise ValueError("query_vector cannot be empty")

        try:
            result = self._index.query(
                vector=query_vector,
                top_k=top_k,
                include_metadata=True,
            )

            return [
                {
                    "id": match.id,
                    "score": float(match.score),
                    "metadata": dict(match.metadata) if match.metadata else {},
                }
                for match in result.matches
            ]

        except Exception as e:
            raise RuntimeError(f"[pinecone] search failed: {e}") from e

    def delete(self, id: str) -> None:
        """
        Remove a vector by id.

        Note: Pinecone delete is fire-and-forget — it does not raise if the
        id doesn't exist. We do a pre-check to match the BaseVectorStore
        contract (KeyError if not found).
        """
        if not id:
            raise ValueError("id cannot be empty")

        try:
            # Pinecone fetch to check existence
            result = self._index.fetch(ids=[id])
            if id not in result.vectors:
                raise KeyError(f"[pinecone] id '{id}' not found in index")

            self._index.delete(ids=[id])
            logger.info(f"[pinecone] Deleted id='{id}'")

        except KeyError:
            raise
        except Exception as e:
            raise RuntimeError(f"[pinecone] delete failed for id='{id}': {e}") from e

    def clear(self) -> None:
        """
        Delete all vectors from the index.

        Uses delete_all on the default namespace — does not delete the index
        itself, just empties it so schema can be re-indexed fresh.
        """
        try:
            self._index.delete(delete_all=True)
            logger.info(f"[pinecone] Cleared all vectors from index '{self._index_name}'")

        except Exception as e:
            raise RuntimeError(f"[pinecone] clear failed: {e}") from e

    @property
    def provider_name(self) -> str:
        return "pinecone"

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _sanitise_metadata(self, metadata: dict) -> dict:
        """
        Flatten metadata to Pinecone-compatible types.

        Pinecone metadata values must be: str, int, float, bool, or list[str].
        Nested dicts and other types are converted to strings so we don't crash
        on upsert. The metadata is opaque to us anyway — we store and retrieve it.
        """
        safe = {}
        for k, v in metadata.items():
            if isinstance(v, (str, int, float, bool)):
                safe[k] = v
            elif isinstance(v, list):
                # Convert list items to strings if they aren't already
                safe[k] = [str(item) for item in v]
            else:
                # Dicts, None, etc. — stringify
                safe[k] = str(v)
        return safe