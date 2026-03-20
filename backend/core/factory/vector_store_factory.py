from core.interfaces import BaseVectorStore
from core.config.settings import get_settings


def create_vector_store() -> BaseVectorStore:
    """
    Returns the correct vector store based on VECTOR_STORE in .env.
    To add a new store: implement BaseVectorStore, add a case here.
    """
    provider = get_settings().VECTOR_STORE.lower()

    if provider == "faiss":
        from rag.vector_stores.faiss_store import FAISSVectorStore
        return FAISSVectorStore()

    if provider == "pinecone":
        from rag.vector_stores.pinecone_store import PineconeVectorStore
        return PineconeVectorStore()

    raise ValueError(
        f"Unknown VECTOR_STORE='{provider}'. "
        "Valid options: faiss | pinecone"
    )
