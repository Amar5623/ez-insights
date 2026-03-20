"""
rag/vector_stores/faiss_store.py
Lead owns this file.

Local FAISS vector store — no external service, no account, no API calls.
Vectors live in memory during the session and are persisted to disk between
restarts so the schema doesn't need to be re-embedded every time you start up.

How it works:
  - FAISS IndexFlatIP stores raw float vectors and does brute-force inner product
    search. With L2-normalised vectors, inner product == cosine similarity.
  - We maintain two parallel lists: _ids (string names) and _metadata (dicts)
    that map 1:1 with FAISS index positions.
  - On upsert: if id exists → remove old entry, add new one at the end.
  - On delete: remove entry, shift everything left by rebuilding.
  - Persistence: index + metadata are saved to disk after every write so a
    restart doesn't require re-embedding the entire schema.

Install: pip install faiss-cpu numpy
"""
import json
import os
import logging
from typing import Optional

import numpy as np

from core.interfaces import BaseVectorStore
from core.config.settings import get_settings

logger = logging.getLogger("nlsql.faiss")


class FAISSVectorStore(BaseVectorStore):
    """
    In-memory FAISS vector store with disk persistence.

    Files written to FAISS_INDEX_PATH:
      index.faiss   — the raw FAISS binary index
      metadata.json — parallel list of {id, metadata} dicts

    Both files are written together atomically (metadata first, then index)
    so a crash mid-write leaves the old files intact.
    """

    def __init__(self):
        import faiss
        self._faiss = faiss

        s = get_settings()
        self._index_dir = s.FAISS_INDEX_PATH
        self._index_file = os.path.join(self._index_dir, "index.faiss")
        self._meta_file = os.path.join(self._index_dir, "metadata.json")

        os.makedirs(self._index_dir, exist_ok=True)

        # In-memory state — parallel structures
        self._index: Optional[object] = None   # faiss.IndexFlatIP
        self._ids: list[str] = []              # e.g. ["products", "orders"]
        self._metadata: list[dict] = []        # e.g. [{"table": "products", ...}]
        self._dim: Optional[int] = None        # set on first upsert

        # Try to load persisted index from disk
        self._load_from_disk()

    # ── Public interface ──────────────────────────────────────────────────────

    def upsert(self, id: str, vector: list[float], metadata: dict) -> None:
        """
        Insert or update a vector. If id exists, the old entry is replaced.

        Args:
            id:       Unique string key, e.g. table name "products"
            vector:   Float embedding from the embedder
            metadata: Dict stored alongside the vector, returned by search()
        """
        vec = self._normalise(vector)
        dim = vec.shape[1]

        # Initialise index on first call
        if self._index is None:
            self._dim = dim
            self._index = self._faiss.IndexFlatIP(dim)
            logger.info(f"[faiss] Initialised index with dim={dim}")
        elif dim != self._dim:
            raise ValueError(
                f"Vector dimension mismatch: index expects {self._dim}, got {dim}. "
                "This happens when you switch embedder models without clearing the index. "
                "Run store.clear() first, then re-index."
            )

        # If id already exists — remove it first
        if id in self._ids:
            self._remove_by_id(id)
            logger.debug(f"[faiss] Updated existing entry: {id}")
        else:
            logger.debug(f"[faiss] Inserted new entry: {id}")

        # Add to FAISS and parallel lists
        self._index.add(vec)
        self._ids.append(id)
        self._metadata.append(metadata)

        self._save_to_disk()

    def search(
        self, query_vector: list[float], top_k: int = 5
    ) -> list[dict]:
        """
        Find the top_k most similar vectors. Returns [] if index is empty.

        Returns:
            List of dicts sorted by score descending:
            [{"id": "products", "score": 0.92, "metadata": {...}}, ...]
        """
        if self._index is None or self._index.ntotal == 0:
            logger.warning("[faiss] search() called on empty index — returning []")
            return []

        vec = self._normalise(query_vector)

        if vec.shape[1] != self._dim:
            raise ValueError(
                f"Query vector dimension {vec.shape[1]} doesn't match "
                f"index dimension {self._dim}."
            )

        # Cap top_k at actual index size to avoid FAISS errors
        k = min(top_k, self._index.ntotal)
        scores, indices = self._index.search(vec, k)

        results = []
        for rank, idx in enumerate(indices[0]):
            if idx < 0:
                # FAISS returns -1 for padding when fewer results than top_k
                continue
            results.append({
                "id": self._ids[idx],
                "score": float(scores[0][rank]),
                "metadata": self._metadata[idx],
            })

        return results

    def delete(self, id: str) -> None:
        """Remove a vector by id. Raises KeyError if not found."""
        if id not in self._ids:
            raise KeyError(f"[faiss] id '{id}' not found in index")

        self._remove_by_id(id)
        logger.info(f"[faiss] Deleted entry: {id}")
        self._save_to_disk()

    def clear(self) -> None:
        """
        Wipe all vectors. Called by SchemaRetriever before re-indexing.
        Also deletes the persisted files on disk.
        """
        self._index = None
        self._ids = []
        self._metadata = []
        self._dim = None

        # Remove persisted files so a restart doesn't reload stale data
        for path in [self._index_file, self._meta_file]:
            if os.path.exists(path):
                os.remove(path)

        logger.info("[faiss] Index cleared")

    @property
    def provider_name(self) -> str:
        return "faiss"

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _normalise(self, vector: list[float]) -> np.ndarray:
        """Convert to float32 numpy array and L2-normalise in place."""
        vec = np.array([vector], dtype="float32")
        self._faiss.normalize_L2(vec)   # makes inner product == cosine similarity
        return vec

    def _remove_by_id(self, id: str) -> None:
        """
        Remove an entry by id and rebuild the FAISS index without it.

        FAISS IndexFlatIP does not support in-place deletion, so we:
          1. Find the position of the id in our parallel lists
          2. Remove it from _ids and _metadata
          3. Rebuild the index from the remaining vectors

        We store vectors as normalised float32 arrays — we re-normalise all
        stored vectors during rebuild. Because we already normalised on insert,
        normalising again is a no-op (idempotent), so this is safe.

        Note: we keep a separate _vectors list in memory to enable rebuild
        without needing to re-call the embedder API.
        """
        idx = self._ids.index(id)
        self._ids.pop(idx)
        self._metadata.pop(idx)

        # We need to rebuild the FAISS index without the deleted vector.
        # Since FAISS flat index stores raw floats, we can extract them.
        if self._index.ntotal == 0 or len(self._ids) == 0:
            # Nothing left after removal
            self._index = self._faiss.IndexFlatIP(self._dim)
            return

        # Extract all vectors currently in the index
        all_vectors = self._index.reconstruct_n(0, self._index.ntotal)

        # Remove the vector at the deleted index position
        remaining = np.delete(all_vectors, idx, axis=0)

        # Rebuild the index with remaining vectors
        new_index = self._faiss.IndexFlatIP(self._dim)
        if len(remaining) > 0:
            new_index.add(remaining)

        self._index = new_index

    def _save_to_disk(self) -> None:
        """Persist index and metadata to disk for restart recovery."""
        try:
            # Write metadata first — if we crash between the two writes,
            # the old index file is still valid for the old metadata.
            meta_payload = [
                {"id": self._ids[i], "metadata": self._metadata[i]}
                for i in range(len(self._ids))
            ]
            with open(self._meta_file, "w") as f:
                json.dump({"dim": self._dim, "entries": meta_payload}, f)

            # Write FAISS index
            self._faiss.write_index(self._index, self._index_file)
            logger.debug(f"[faiss] Saved {len(self._ids)} vectors to disk")

        except Exception as e:
            logger.warning(f"[faiss] Failed to persist index to disk: {e}")
            # Don't raise — in-memory state is still valid

    def _load_from_disk(self) -> None:
        """Load a previously persisted index on startup if it exists."""
        if not os.path.exists(self._index_file) or not os.path.exists(self._meta_file):
            logger.info("[faiss] No persisted index found — starting fresh")
            return

        try:
            with open(self._meta_file) as f:
                payload = json.load(f)

            self._dim = payload["dim"]
            entries = payload["entries"]
            self._ids = [e["id"] for e in entries]
            self._metadata = [e["metadata"] for e in entries]

            self._index = self._faiss.read_index(self._index_file)
            logger.info(
                f"[faiss] Loaded {len(self._ids)} vectors from disk "
                f"(dim={self._dim})"
            )

        except Exception as e:
            logger.warning(
                f"[faiss] Failed to load persisted index: {e}. Starting fresh."
            )
            # Reset to clean state — SchemaRetriever will re-index
            self._index = None
            self._ids = []
            self._metadata = []
            self._dim = None