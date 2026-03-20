# TASKS.md — Team task breakdown

> Read this fully before touching any code.
> Every task maps to a folder. Stay in your folder.
> If you need something from another module, use the interface — never import directly.

---

## LEAD (you) — core/ + llm/ + rag/

### Phase 1 — Do this first, unblocks everyone else

- [ ] `core/interfaces/base_llm.py` — abstract LLM class
- [ ] `core/interfaces/base_embedder.py` — abstract embedder class
- [ ] `core/interfaces/base_db_adapter.py` — abstract DB adapter class
- [ ] `core/interfaces/base_strategy.py` — abstract strategy class
- [ ] `core/interfaces/base_vector_store.py` — abstract vector store class
- [ ] `core/config/settings.py` — load all env vars, expose typed settings object
- [ ] `core/config/.env.example` — document every env var with description

### Phase 2 — Your feature work

- [ ] `core/factory/llm_factory.py` — reads `LLM_PROVIDER`, returns correct LLM instance
- [ ] `core/factory/embedder_factory.py` — reads `EMBEDDER_PROVIDER`, returns embedder
- [ ] `core/factory/db_factory.py` — reads `DB_TYPE`, returns correct adapter
- [ ] `core/factory/strategy_factory.py` — reads `STRATEGY`, returns strategy
- [ ] `core/factory/vector_store_factory.py` — reads `VECTOR_STORE`, returns store
- [ ] `llm/base_llm.py` — re-export for convenience
- [ ] `llm/openai_llm.py` — OpenAI ChatCompletion implementation
- [ ] `llm/gemini_llm.py` — Google Gemini implementation
- [ ] `llm/ollama_llm.py` — local Ollama implementation
- [ ] `rag/schema_retriever.py` — fetch relevant schema chunks via vector similarity
- [ ] `rag/prompt_builder.py` — build final prompt from question + schema context
- [ ] `rag/embedders/openai_embedder.py` — embed via OpenAI
- [ ] `rag/embedders/cohere_embedder.py` — embed via Cohere
- [ ] `backend/main.py` — FastAPI app init, wire all factories, include routers

### Phase 3 — Integration + review

- [ ] Review all PRs from Dev 1, Dev 2, Dev 3
- [ ] `tests/integration/test_full_pipeline.py` — end-to-end query test
- [ ] `docker-compose.yml` — MySQL + Mongo + backend + frontend services
- [ ] `backend/requirements.txt` — pin all dependencies

---

## DEV 1 — adapters/

> Branch: `feat/db-adapters`
> You implement the DB layer. Your only contract is `core/interfaces/base_db_adapter.py`.
> Do not import anything from `strategies/`, `llm/`, or `rag/`.

### What you're building

The system needs to connect to MySQL and MongoDB, execute queries, and
inspect the schema (tables/collections + columns/fields). Each DB gets its
own adapter class that implements `BaseDBAdapter`.

### Tasks

- [ ] Read `core/interfaces/base_db_adapter.py` before writing a single line
- [ ] `adapters/mysql_adapter.py`
  - implement `connect()`, `disconnect()`, `execute_query(sql, params)`, `fetch_schema()`
  - use `PyMySQL` for connection
  - return rows as `list[dict]`
  - handle connection errors with clear messages
- [ ] `adapters/mongo_adapter.py`
  - implement same interface for MongoDB using `pymongo`
  - `execute_query()` accepts a filter dict, not SQL string
  - `fetch_schema()` samples documents to infer field structure
- [ ] `adapters/schema_inspector/mysql.py`
  - use `SHOW TABLES` + `DESCRIBE <table>` to extract full schema
  - return structured dict: `{table_name: [{column, type, nullable}]}`
- [ ] `adapters/schema_inspector/mongo.py`
  - sample top 100 docs per collection to infer field names + types
  - return structured dict: `{collection_name: [{field, inferred_type}]}`
- [ ] `adapters/connection_pool.py`
  - simple connection pool wrapper for MySQL (use `DBUtils` or manual pool)
  - context manager support: `with pool.get_connection() as conn:`
- [ ] `tests/unit/test_mysql_adapter.py` — mock connection, test execute + schema
- [ ] `tests/unit/test_mongo_adapter.py` — mock pymongo, test execute + schema

### Definition of done

- Both adapters pass their unit tests
- `db_factory.py` (lead's file) can instantiate either with zero code change
- No hardcoded connection strings anywhere — all from settings

---

## DEV 2 — strategies/

> Branch: `feat/strategies`
> You build the query strategy engine. Your contracts are:
> `core/interfaces/base_strategy.py` and `core/interfaces/base_db_adapter.py`.
> Never import from `adapters/` directly — you receive an adapter instance via DI.

### What you're building

The system detects what kind of query the user is asking and routes it to
the right strategy. Each strategy produces a query (SQL or Mongo filter),
executes it via the adapter, and returns results.

### Tasks

- [ ] Read `core/interfaces/base_strategy.py` and `base_db_adapter.py` first
- [ ] `strategies/sql_filter.py`
  - handles exact/structured queries: `WHERE price > 20`, date ranges, booleans
  - receives generated SQL from LLM, cleans + parameterizes it
  - calls `adapter.execute_query(sql, params)`
- [ ] `strategies/fuzzy_match.py`
  - handles typo-tolerant text search
  - uses Levenshtein distance (install `python-Levenshtein`)
  - applies `LOWER()` normalization before comparison
  - returns top N results ranked by similarity score
- [ ] `strategies/vector_search.py`
  - handles semantic/conceptual queries
  - receives embedded query vector, searches vector store
  - returns matching rows with similarity scores
- [ ] `strategies/combined.py`
  - orchestrates sql + fuzzy + vector together
  - merges + deduplicates results
  - ranks by combined score
- [ ] `strategies/router.py`
  - `detect_strategy(question: str) -> StrategyType`
  - uses keywords + LLM classification to pick strategy
  - falls back to `combined` when uncertain
- [ ] `strategies/sql_validator.py`
  - parses SQL using `sqlparse`
  - checks for dangerous operations: DROP, DELETE, TRUNCATE, etc.
  - returns `(is_valid: bool, error: str | None)`
- [ ] `strategies/retry_handler.py`
  - wraps strategy execution in retry loop (max 3 attempts)
  - on failure: appends error to history, re-prompts LLM with error context
  - raises `MaxRetriesExceeded` after 3 failures
- [ ] `tests/unit/test_router.py` — test strategy detection logic
- [ ] `tests/unit/test_sql_validator.py` — test valid + dangerous SQL cases
- [ ] `tests/unit/test_retry_handler.py` — mock failures, test retry count

### Definition of done

- Router correctly identifies strategy type for 10 sample questions
- SQL validator blocks dangerous queries
- Retry handler gives up after 3 and raises the right exception

---

## DEV 3 — api/ + frontend/

> Branch: `feat/api-ui`
> You own the full surface layer. Backend: FastAPI routes + middleware.
> Frontend: React + Vite. You never touch core/, llm/, rag/, adapters/, strategies/.
> The backend gives you a `QueryService` — you call it, you don't build it.

### What you're building

The REST API that the frontend talks to, and the React UI that users interact with.

### Backend tasks

- [ ] `api/routes/query.py`
  - `POST /api/query` — accepts `{question: str, db_type: str}`
  - calls `QueryService.run(question)` (lead provides this)
  - returns `{sql: str, results: list[dict], strategy_used: str, answer: str}`
- [ ] `api/routes/history.py`
  - `GET /api/history` — returns last N queries + results
  - `DELETE /api/history/{id}` — delete a history entry
- [ ] `api/routes/health.py`
  - `GET /api/health` — returns DB connection status + LLM provider name
- [ ] `api/middleware/auth.py`
  - simple API key auth via `X-API-Key` header
  - reads valid keys from settings
  - returns 401 if missing or invalid
- [ ] `api/middleware/logging.py`
  - log every request: method, path, status, duration
  - use Python `logging` module, structured output
- [ ] `api/schemas.py`
  - Pydantic models for all request + response bodies
  - `QueryRequest`, `QueryResponse`, `HistoryItem`

### Frontend tasks

- [ ] Vite + React + TypeScript project init (already scaffolded)
- [ ] `src/services/api.ts` — all fetch calls in one place, typed
- [ ] `src/types/index.ts` — TypeScript types matching backend Pydantic models
- [ ] `src/components/ChatInput.tsx` — textarea + submit, handles loading state
- [ ] `src/components/ResultTable.tsx` — renders query results as a table
- [ ] `src/components/QueryHistory.tsx` — sidebar list of past queries
- [ ] `src/components/StrategyBadge.tsx` — pill showing which strategy was used
- [ ] `src/components/SqlPreview.tsx` — collapsible raw SQL display
- [ ] `src/hooks/useQuery.ts` — custom hook: send question, get back result + loading + error
- [ ] `src/hooks/useHistory.ts` — fetch + manage query history
- [ ] `src/pages/Home.tsx` — main page layout wiring all components
- [ ] `.env.example` — `VITE_API_URL=http://localhost:8000`

### Definition of done

- `POST /api/query` returns correct shape (test with curl or Postman)
- UI sends a question and displays result table + SQL + strategy badge
- History loads on page refresh
- Auth middleware rejects requests without valid API key
