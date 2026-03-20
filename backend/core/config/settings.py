from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # ── LLM ──────────────────────────────────────────────
    LLM_PROVIDER: str = "openai"          # openai | gemini | ollama
    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-4o-mini"
    GEMINI_API_KEY: str = ""
    GEMINI_MODEL: str = "gemini-1.5-flash"
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3"

    # ── Embedder ─────────────────────────────────────────
    EMBEDDER_PROVIDER: str = "openai"     # openai | cohere
    COHERE_API_KEY: str = ""
    OPENAI_EMBEDDING_MODEL: str = "text-embedding-3-small"

    # ── Database ─────────────────────────────────────────
    DB_TYPE: str = "mysql"                # mysql | mongo

    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = ""
    MYSQL_DATABASE: str = "nlsql_db"

    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DATABASE: str = "nlsql_db"

    # ── Vector store ─────────────────────────────────────
    VECTOR_STORE: str = "faiss"           # faiss | pinecone
    PINECONE_API_KEY: str = ""
    PINECONE_INDEX: str = "nlsql"
    FAISS_INDEX_PATH: str = "./data/faiss_index"

    # ── Strategy ─────────────────────────────────────────
    STRATEGY: str = "auto"                # auto | sql | fuzzy | vector | combined
    MAX_RETRIES: int = 3
    MAX_RESULT_ROWS: int = 20

    # ── API ──────────────────────────────────────────────
    API_KEY: str = "change-me-in-env"
    CORS_ORIGINS: list[str] = ["http://localhost:5173"]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    """Call this anywhere: from core.config.settings import get_settings"""
    return Settings()
