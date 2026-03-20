"""
core/config/settings.py
Lead owns this file.

Single source of truth for every configuration value in the system.
All modules read config through get_settings() — never os.environ directly.

Rules:
  - Adding a new env var? Add it here with a sensible default AND in .env.example.
  - Changing a field name? Update .env.example and tell the team in your PR.
  - Never hardcode secrets anywhere else. If you need a key, add it here.

Usage:
    from core.config.settings import get_settings

    s = get_settings()
    print(s.LLM_PROVIDER)    # 'openai'
    print(s.MYSQL_HOST)      # 'localhost'

get_settings() is lru_cache'd — it reads and parses .env exactly once per
process. Do not call Settings() directly in application code.
"""
from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    All environment variables, typed and with defaults.

    Loaded from the .env file in the backend/ directory (or from the real
    environment, which takes precedence). See core/config/.env.example for
    documentation of every variable.

    Pydantic-settings coerces types automatically:
      - MYSQL_PORT="3306" in .env → int 3306 here
      - CORS_ORIGINS='["http://localhost:5173"]' → list[str] here
    """

    # ── LLM ──────────────────────────────────────────────────────────────────
    # Which LLM provider to use. Swap without touching code.
    LLM_PROVIDER: str = "openai"          # openai | gemini | ollama

    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-4o-mini"     # any ChatCompletion-compatible model

    GEMINI_API_KEY: str = ""
    GEMINI_MODEL: str = "gemini-1.5-flash"

    OLLAMA_BASE_URL: str = "http://localhost:11434"   # Ollama HTTP API base
    OLLAMA_MODEL: str = "llama3"

    # ── Embedder ─────────────────────────────────────────────────────────────
    EMBEDDER_PROVIDER: str = "openai"     # openai | cohere

    OPENAI_EMBEDDING_MODEL: str = "text-embedding-3-small"   # 1536 dims

    COHERE_API_KEY: str = ""
    COHERE_EMBEDDING_MODEL: str = "embed-english-v3.0"       # 1024 dims

    # ── Database ─────────────────────────────────────────────────────────────
    DB_TYPE: str = "mysql"                # mysql | mongo

    # MySQL — used when DB_TYPE=mysql
    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = ""
    MYSQL_DATABASE: str = "nlsql_db"
    MYSQL_POOL_SIZE: int = 5              # number of pooled connections

    # MongoDB — used when DB_TYPE=mongo
    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DATABASE: str = "nlsql_db"
    MONGO_SAMPLE_SIZE: int = 100          # docs to sample per collection for schema inference

    # ── Vector store ─────────────────────────────────────────────────────────
    VECTOR_STORE: str = "faiss"           # faiss | pinecone

    FAISS_INDEX_PATH: str = "./data/faiss_index"   # local file path for FAISS persistence

    PINECONE_API_KEY: str = ""
    PINECONE_INDEX: str = "nlsql"         # name of the Pinecone index

    # ── Query strategy ───────────────────────────────────────────────────────
    STRATEGY: str = "auto"               # auto | sql | fuzzy | vector | combined
    #   auto      → StrategyRouter classifies the question and picks one
    #   sql       → always SQLFilterStrategy (exact WHERE queries)
    #   fuzzy     → always FuzzyMatchStrategy (Levenshtein distance)
    #   vector    → always VectorSearchStrategy (semantic similarity)
    #   combined  → always CombinedStrategy (runs all three, merges)

    MAX_RETRIES: int = 3                  # max LLM retry attempts on bad SQL
    MAX_RESULT_ROWS: int = 100            # cap rows returned to the frontend

    # ── API / server ─────────────────────────────────────────────────────────
    API_KEY: str = "change-me-in-env"    # X-API-Key header value
    CORS_ORIGINS: list[str] = ["http://localhost:5173"]   # allowed frontend origins

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # Extra fields in .env are silently ignored (don't crash on unknown vars)
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    """
    Return the singleton Settings instance (parsed once, cached forever).

    Use this everywhere instead of Settings() directly.
    The lru_cache means .env is read exactly once per process — safe and fast.

    In tests, call get_settings.cache_clear() after monkeypatching env vars
    to force a re-parse.

    Example:
        from core.config.settings import get_settings
        s = get_settings()
        print(s.DB_TYPE)    # 'mysql'
    """
    return Settings()