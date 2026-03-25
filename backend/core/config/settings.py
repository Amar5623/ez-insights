"""
core/config/settings.py
Lead owns this file.

Single source of truth for every configuration value in the system.
All modules read config through get_settings() — never os.environ directly.

Usage:
    from core.config.settings import get_settings
    s = get_settings()
    print(s.LLM_PROVIDER)    # 'groq'

get_settings() is lru_cache'd — reads and parses .env exactly once per process.
"""
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    # ── LLM ──────────────────────────────────────────────────────────────────
    LLM_PROVIDER: str = "groq"            # groq | gemini | ollama

    # Groq (default) — free tier at https://console.groq.com
    GROQ_API_KEY: str = ""
    GROQ_MODEL: str = "llama-3.3-70b-versatile"
    # Other good Groq models:
    #   llama-3.1-8b-instant   — fastest
    #   mixtral-8x7b-32768     — large context, good for big schemas

    # Google Gemini — free tier at https://aistudio.google.com
    GEMINI_API_KEY: str = ""
    GEMINI_MODEL: str = "gemini-1.5-flash"

    # Ollama (local) — no key, install from https://ollama.com
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3"

    # ── Embedder ─────────────────────────────────────────────────────────────
    # Converts schema text and questions into vectors for RAG similarity search.
    # nomic runs locally through Ollama — same server as the LLM, no extra setup.
    EMBEDDER_PROVIDER: str = "nomic"      # nomic | cohere

    # nomic-embed-text (default) — local via Ollama, free, 768 dims
    # Pull with: ollama pull nomic-embed-text
    NOMIC_MODEL: str = "nomic-embed-text"

    # Cohere (cloud alternative) — needs API key, 1024 dims
    COHERE_API_KEY: str = ""
    COHERE_EMBEDDING_MODEL: str = "embed-english-v3.0"

    # ── Database ─────────────────────────────────────────────────────────────
    DB_TYPE: str = "mysql"                # mysql | mongo

    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = ""
    MYSQL_DATABASE: str = "nlsql_db"
    MYSQL_POOL_SIZE: int = 5

    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DATABASE: str = "nlsql_db"
    MONGO_SAMPLE_SIZE: int = 100

    # ── Vector store ─────────────────────────────────────────────────────────
    VECTOR_STORE: str = "faiss"           # faiss | pinecone

    FAISS_INDEX_PATH: str = "./data/faiss_index"

    PINECONE_API_KEY: str = ""
    PINECONE_INDEX: str = "nlsql"

    # ── Query strategy ───────────────────────────────────────────────────────
    STRATEGY: str = "auto"               # auto | sql | fuzzy | vector | combined
    MAX_RETRIES: int = 3
    MAX_RESULT_ROWS: int = 100

    # ── API / server ─────────────────────────────────────────────────────────
    API_KEY: str = "change-me-in-env"
    CORS_ORIGINS: list[str] = ["http://localhost:5173"]

    INTENT_CLASSIFIER_ENABLED: bool = True
    INTENT_LLM_FALLBACK: bool = True

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    """
    Return the singleton Settings instance (parsed once, cached forever).
    In tests, call get_settings.cache_clear() after monkeypatching env vars.
    """
    return Settings()