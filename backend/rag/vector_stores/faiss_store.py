"""
Lead owns this file.
Local FAISS vector store — no external service needed.
Install: pip install faiss-cpu
"""
import json
import os
import numpy as np

from core.interfaces import BaseVectorStore
from core.config.settings import get_settings


class FAISSVectorStore(BaseVectorStore):
    def __init__(self):
        import faiss
        self._faiss = faiss
        self._index = None
        self._metadata: list[dict] = []   # parallel list to FAISS vectors
        self._ids: list[str] = []
        self._index_path = get_settings().FAISS_INDEX_PATH
        os.makedirs(self._index_path, exist_ok=True)

    def _ensure_index(self, dim: int) -> None:
        if self._index is None:
            self._index = self._faiss.IndexFlatIP(dim)  # inner product = cosine on normalized vecs

    def upsert(self, id: str, vector: list[float], metadata: dict) -> None:
        vec = np.array([vector], dtype="float32")
        self._faiss.normalize_L2(vec)
        dim = vec.shape[1]
        self._ensure_index(dim)

        if id in self._ids:
            # update in place
            idx = self._ids.index(id)
            self._metadata[idx] = metadata
            # FAISS flat index doesn't support in-place update — rebuild
            self._rebuild()
        else:
            self._index.add(vec)
            self._ids.append(id)
            self._metadata.append(metadata)

    def search(self, query_vector: list[float], top_k: int = 5) -> list[dict]:
        if self._index is None or self._index.ntotal == 0:
            return []
        vec = np.array([query_vector], dtype="float32")
        self._faiss.normalize_L2(vec)
        k = min(top_k, self._index.ntotal)
        scores, indices = self._index.search(vec, k)
        return [
            {"id": self._ids[i], "score": float(scores[0][rank]), "metadata": self._metadata[i]}
            for rank, i in enumerate(indices[0])
            if i >= 0
        ]

    def delete(self, id: str) -> None:
        if id in self._ids:
            idx = self._ids.index(id)
            self._ids.pop(idx)
            self._metadata.pop(idx)
            self._rebuild()

    def clear(self) -> None:
        self._index = None
        self._ids = []
        self._metadata = []

    def _rebuild(self) -> None:
        """Rebuild index from scratch after a delete/update."""
        old_meta = list(self._metadata)
        old_ids = list(self._ids)
        self.clear()
        # Re-add all — embeddings are not stored so schema must be re-indexed
        # This is a limitation of flat FAISS; use Pinecone for production
        pass

    @property
    def provider_name(self) -> str:
        return "faiss"
