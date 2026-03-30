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
    OLLAMA_MODEL: str = "phi3"

    # ── Embedder ─────────────────────────────────────────────────────────────
    # Converts schema text and questions into vectors for RAG similarity search.
    # nomic runs locally through Ollama — same server as the LLM, no extra setup.
    EMBEDDER_PROVIDER: str = "nomic"      # nomic | cohere | gemma

    # nomic-embed-text (default) — local via Ollama, free, 768 dims
    # Pull with: ollama pull nomic-embed-text
    NOMIC_MODEL: str = "nomic-embed-text"

    # Cohere (cloud alternative) — needs API key, 1024 dims
    COHERE_API_KEY: str = ""
    COHERE_EMBEDDING_MODEL: str = "embed-english-v3.0"

    # EmbeddingGemma 300M — local via HuggingFace sentence-transformers, 768 dims
    # Requires: pip install sentence-transformers
    # Requires: accept license at huggingface.co/google/embeddinggemma-300m
    HF_TOKEN: str = ""   # HuggingFace token for gated model download

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

    # ── Row limits — three separate concerns ─────────────────────────────────
    #
    # These three settings serve completely different purposes and must NOT
    # be conflated. Changing one should never affect the others.
    #
    # MAX_DB_FETCH_ROWS: hard cap on rows the DB query returns.
    #   Applied as a Python slice after strategy.execute() — acts as a safety
    #   net so a rogue query never returns 100k rows into memory.
    #   Strategies also use this in their own LIMIT clauses.
    #
    # MAX_ROWS_FOR_LLM: how many rows are injected into the answer-generation
    #   prompt sent to the LLM. Must be much smaller than MAX_DB_FETCH_ROWS —
    #   100 rows of raw data in a prompt is 5000–8000 tokens; the LLM cannot
    #   reason well over that volume and it is expensive.
    #   The full result set is still returned to the frontend.
    #
    # PAGE_SIZE: UI-level setting — how many rows the frontend shows per page
    #   before the "Show more" button. Backend never uses this directly;
    #   it is exposed here so it can be changed without a frontend redeploy.
    #   (Frontend reads it from the QueryResponse or uses the constant directly.)
    #
    MAX_DB_FETCH_ROWS: int = 100   # rows fetched from DB (was MAX_RESULT_ROWS)
    MAX_ROWS_FOR_LLM: int = 10     # rows sent to LLM answer-generation prompt
    PAGE_SIZE: int = 10            # rows shown per page in the UI

    # Backward-compat alias — Dev 2's strategy files reference MAX_RESULT_ROWS.
    # Do NOT remove until all strategy files are updated to MAX_DB_FETCH_ROWS.
    @property
    def MAX_RESULT_ROWS(self) -> int:
        return self.MAX_DB_FETCH_ROWS

    # ── API / server ─────────────────────────────────────────────────────────
    API_KEY: str = "change-me-in-env"
    CORS_ORIGINS: list[str] = ["http://localhost:5173"]

    INTENT_CLASSIFIER_ENABLED: bool = True
    INTENT_LLM_FALLBACK: bool = True

    # ── Sensitive Data Masking───────────────────────────────────────────────────────────────
    SENSITIVE_COLUMNS_EXTRA: list[str] = []
    # session
    APP_MONGO_URI: str = "mongodb+srv://..."   # same cluster, different DB is fine
    APP_MONGO_DB_NAME: str = "ez_insights_data"

    # ── Logging ─────────────────────────────────────────────────────────────────
    # Place this section right before the `class Config:` block.
    #
    # LOG_LEVEL controls verbosity:
    #   INFO  → one line per pipeline stage (always on, for monitoring)
    #   DEBUG → full prompts, raw LLM output, per-column scrub decisions
    #           WARNING: DEBUG logs contain full SQL queries and answer text.
    #           Never enable in production without log access controls.
 
    LOG_LEVEL: str = "INFO"          # INFO | DEBUG | WARNING | ERROR

    # ── Sensitive data masking ────────────────────────────────────────────────────
    # SENSITIVE_TABLES: tables the schema inspector will skip (unless a safe view
    #   named vw_<tablename> exists). Configurable per client — no hardcoding needed.
    #
    # SENSITIVE_COLUMNS_EXTRA: additional column names to block, on top of the
    #   built-in list in data_scrubber.py and schema_inspector/mysql.py.
    #
    # SCRUB_EMAILS: whether to redact email-looking values in query results.
    #   Set False if your UI legitimately shows email addresses.
    
    SENSITIVE_TABLES: list[str] = ["customers", "payments"]   # override per client
    SENSITIVE_COLUMNS_EXTRA: list[str] = []                   # already existed, keep it
    SCRUB_EMAILS: bool = True

    # ── Client config ─────────────────────────────────────────────────────────────
    # Path to the client-specific configuration bundle.
    # Set this to the folder for the current client deployment.
    # Example: CLIENT_CONFIG_PATH=./client-configs/classicmodels
    CLIENT_CONFIG_PATH: str = "./client-configs/classicmodels"
 
      
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