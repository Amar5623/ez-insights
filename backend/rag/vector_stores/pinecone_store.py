"""
Lead owns this file.
Pinecone vector store — production-grade, hosted.
Install: pip install pinecone-client
"""
from core.interfaces import BaseVectorStore
from core.config.settings import get_settings


class PineconeVectorStore(BaseVectorStore):
    def __init__(self):
        from pinecone import Pinecone
        s = get_settings()
        pc = Pinecone(api_key=s.PINECONE_API_KEY)
        self._index = pc.Index(s.PINECONE_INDEX)

    def upsert(self, id: str, vector: list[float], metadata: dict) -> None:
        self._index.upsert(vectors=[{"id": id, "values": vector, "metadata": metadata}])

    def search(self, query_vector: list[float], top_k: int = 5) -> list[dict]:
        result = self._index.query(vector=query_vector, top_k=top_k, include_metadata=True)
        return [
            {"id": m.id, "score": m.score, "metadata": m.metadata}
            for m in result.matches
        ]

    def delete(self, id: str) -> None:
        self._index.delete(ids=[id])

    def clear(self) -> None:
        self._index.delete(delete_all=True)

    @property
    def provider_name(self) -> str:
        return "pinecone"
