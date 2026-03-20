"""
strategies/vector_search.py
Dev 2 owns this file.

Handles semantic / conceptual queries using vector similarity search
for both MySQL and MongoDB.

How it fits in the system:
    At startup (main.py):
        - SchemaRetriever embeds every table/collection and stores
          vectors in the vector store (Lead's code)

    At query time (this file):
        1. Embed the user's question → query_vector
        2. Search vector store for the most similar schema chunks
        3. Extract which tables/collections matched
        4. Build a DB query targeting those tables/collections
        5. Execute via adapter → return rows + similarity scores

Flow:
    execute(question, generated_query)
        ├── embedder.embed(question)          → query_vector
        ├── vector_store.search(query_vector) → top schema matches
        ├── _extract_entities()               → table/collection names
        ├── _build_query()                    → SQL or Mongo filter
        ├── validator.validate()              → safety check
        ├── adapter.execute_query()           → rows from DB
        └── StrategyResult with scores        → back to QueryService

Note:
    vector_store and embedder are injected by the factory (Lead wires this).
    Never instantiate them here — just use self.vector_store and self.embedder.
"""

from __future__ import annotations

import re
from typing import Any

from core.interfaces import BaseDBAdapter, BaseStrategy, StrategyResult
from core.config.settings import get_settings
from strategies.sql_validator import get_validator


# ─── Keyword signals for can_handle() ────────────────────────────────────────

# Positive signals — abstract, conceptual, semantic language
_VECTOR_POSITIVE_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\babout\b",                        # "books about loneliness"
        r"\brelated\s+to\b",                 # "products related to fitness"
        r"\bsimilar\s+(to|items?|products?)", # "similar to this one"
        r"\blike\s+\w+",                     # "something like Dune"
        r"\bsomething\b",                    # "show me something inspiring"
        r"\banything\b",                     # "anything about space"
        r"\binspir\w+\b",                    # "inspiring", "inspirational"
        r"\btheme\w*\b",                     # "theme of loss"
        r"\bconcept\w*\b",                   # "concept of justice"
        r"\bfeeling\b",                      # "a feeling of hope"
        r"\bmood\b",                         # "a dark mood"
        r"\bvibe\b",                         # "cozy vibe"
        r"\bmeaning\w*\b",                   # "meaning of life"
        r"\bphilosoph\w+\b",                 # "philosophical questions"
        r"\bemotional\w*\b",                 # "emotional stories"
        r"\bsemantic\w*\b",                  # "semantic search"
        r"\bconceptual\w*\b",                # "conceptual match"
        r"\brecommend\w*\b",                 # "recommend me something"
        r"\bsugg\w+\b",                      # "suggest", "suggestions"
        r"\bdiscover\w*\b",                  # "discover new books"
        r"\bexplore\w*\b",                   # "explore topics about"
    ]
]

# Negative signals — structured / exact queries that belong to sql_filter
_VECTOR_NEGATIVE_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bwhere\b",
        r"\bprice\s*[<>=]",
        r"\bgreater\s+than\b",
        r"\bless\s+than\b",
        r"\bbetween\b",
        r"\bin\s+stock\b",
        r"\bcount\b",
        r"\btotal\b",
        r"\bsum\b",
        r"\border\s+by\b",
        r"\b\d{4}\b",                        # year like 2024
    ]
]

# Default number of vector store results to retrieve
_DEFAULT_TOP_K = 10

# Fallback SQL when we know the table but have no specific filter
_FALLBACK_SQL_TEMPLATE = "SELECT * FROM `{table}` LIMIT {limit}"

# Fallback Mongo filter when we know the collection but have no specific filter
_FALLBACK_MONGO_TEMPLATE: dict = {}


class VectorSearchStrategy(BaseStrategy):
    """
    Executes semantic / conceptual queries against MySQL or MongoDB
    using vector similarity search over the schema index.

    How it works:
        The schema is pre-embedded at startup by SchemaRetriever (Lead's code).
        Each table/collection is stored as a vector in the vector store.

        At query time:
            1. The user's question is embedded into the same vector space.
            2. The vector store finds the most similar schema chunks —
               i.e. the tables/collections most relevant to the question.
            3. We build a query targeting those tables/collections.
            4. The adapter executes the query and returns matching rows.

    MySQL  — builds a SELECT * FROM <matched_table> query, optionally
             enhanced by any SQL hint from generated_query.
    MongoDB — builds a filter dict targeting the matched collection,
              optionally enhanced by any filter hint from generated_query.

    Dependency injection:
        vector_store and embedder are passed in via __init__ by the factory.
        Never import or instantiate concrete embedder/store classes here.
    """

    def __init__(
        self,
        adapter: BaseDBAdapter,
        vector_store=None,
        embedder=None,
        top_k: int = _DEFAULT_TOP_K,
    ):
        """
        Args:
            adapter:     Injected DB adapter (MySQL or Mongo).
            vector_store: Injected BaseVectorStore — FAISS or Pinecone.
            embedder:    Injected BaseEmbedder — OpenAI or Cohere.
            top_k:       How many schema matches to retrieve from the
                         vector store. Default 10.
        """
        super().__init__(adapter)
        self.vector_store = vector_store
        self.embedder = embedder
        self.top_k = top_k
        self._settings = get_settings()

    # ── Public interface ──────────────────────────────────────────────────────

    def execute(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Embed the question, find relevant schema, query the DB, return rows.

        Args:
            question:        Original natural language question from user.
            generated_query: SQL string (MySQL) or filter dict (MongoDB)
                             from the LLM — used as a hint if available,
                             but the vector search drives the table/collection
                             selection.

        Returns:
            StrategyResult with rows and similarity scores in metadata.

        Raises:
            RuntimeError: If embedder or vector_store are not injected.
            ValueError:   If no relevant schema entity can be found.
            RuntimeError: If adapter execution fails.
        """
        # ── Guard: ensure dependencies are injected ──────────────────────────
        if self.embedder is None:
            raise RuntimeError(
                "VectorSearchStrategy requires an embedder. "
                "Make sure it is injected via the factory."
            )
        if self.vector_store is None:
            raise RuntimeError(
                "VectorSearchStrategy requires a vector_store. "
                "Make sure it is injected via the factory."
            )

        db = self.adapter.db_type.lower()

        if db == "mysql":
            return self._execute_mysql(question, generated_query)

        if db in ("mongo", "mongodb"):
            return self._execute_mongo(question, generated_query)

        raise ValueError(
            f"VectorSearchStrategy does not support db_type='{db}'. "
            "Valid options: 'mysql' | 'mongo'"
        )

    def can_handle(self, question: str) -> bool:
        """
        Return True if the question is abstract / conceptual / semantic
        and does NOT look like a structured filter query.

        Logic:
            1. At least one positive pattern must match (about, related to,
               inspiring, similar, theme, recommend, etc.)
            2. No negative pattern must match (price >, where, count, etc.)
        """
        if not question or not question.strip():
            return False

        has_positive = any(p.search(question) for p in _VECTOR_POSITIVE_PATTERNS)
        if not has_positive:
            return False

        has_negative = any(p.search(question) for p in _VECTOR_NEGATIVE_PATTERNS)
        if has_negative:
            return False

        return True

    @property
    def strategy_name(self) -> str:
        return "vector"

    # ── Shared: embed question + search vector store ──────────────────────────

    def _search_schema(self, question: str) -> list[dict]:
        """
        Embed the question and search the vector store for the most
        relevant schema chunks.

        Returns:
            List of search result dicts, each shaped as:
            {
                "id":       "products",        ← table/collection name
                "score":    0.92,              ← cosine similarity (higher = better)
                "metadata": {
                    "entity":      "products",
                    "schema_text": "Table: products — columns: id, name, price"
                }
            }
            Sorted by score descending (best match first).
            Returns [] if vector store is empty.
        """
        query_vector = self.embedder.embed(question)
        results = self.vector_store.search(query_vector, top_k=self.top_k)
        return results  # already sorted descending by score

    def _extract_top_entity(self, search_results: list[dict]) -> str | None:
        """
        Extract the best-matching table/collection name from vector search results.

        Returns the 'id' field of the top result, or None if results are empty.

        Example:
            results = [{"id": "products", "score": 0.92, ...}, ...]
            → returns "products"
        """
        if not search_results:
            return None
        return search_results[0]["id"]

    def _build_score_metadata(
        self, search_results: list[dict], question: str
    ) -> dict:
        """
        Build the metadata dict included in StrategyResult.
        Contains similarity scores for all matched schema entities.

        Example output:
            {
                "question": "books about loneliness",
                "top_matches": [
                    {"entity": "books", "score": 0.91},
                    {"entity": "authors", "score": 0.74},
                ],
            }
        """
        return {
            "question": question,
            "top_matches": [
                {
                    "entity": r["id"],
                    "score": round(r["score"], 4),
                }
                for r in search_results
            ],
        }

    # ── MySQL execution path ──────────────────────────────────────────────────

    def _execute_mysql(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Vector search pipeline for MySQL.

        Steps:
            1. Embed question → search vector store → find best table
            2. Build SQL targeting that table:
               a. If generated_query is a valid SQL string → use it as-is
                  (LLM already produced a good query, vector search just
                   confirmed the right table)
               b. Otherwise → build a simple SELECT * FROM <table> fallback
            3. Validate SQL with MySQLValidator
            4. Execute via adapter
            5. Return StrategyResult with similarity scores in metadata
        """
        # ── 1. Vector search ─────────────────────────────────────────────────
        search_results = self._search_schema(question)
        top_entity = self._extract_top_entity(search_results)

        if not top_entity:
            raise ValueError(
                "VectorSearchStrategy found no relevant schema entities. "
                "Make sure the schema has been indexed at startup."
            )

        # ── 2. Build SQL ──────────────────────────────────────────────────────
        sql = self._resolve_mysql_query(generated_query, top_entity)

        # ── 3. Validate ───────────────────────────────────────────────────────
        validator = get_validator("mysql")
        is_valid, error = validator.validate(sql)
        if not is_valid:
            # LLM query was dangerous — fall back to safe SELECT *
            sql = _FALLBACK_SQL_TEMPLATE.format(
                table=top_entity,
                limit=self._settings.MAX_RESULT_ROWS,
            )

        sql = sql.rstrip("; \t\n")

        # ── 4. Execute ────────────────────────────────────────────────────────
        try:
            rows = self.adapter.execute_query(sql, None)
        except Exception as exc:
            raise RuntimeError(
                f"VectorSearchStrategy failed to execute MySQL query.\n"
                f"SQL  : {sql}\n"
                f"Error: {exc}"
            ) from exc

        rows = rows[: self._settings.MAX_RESULT_ROWS]

        # ── 5. Return ─────────────────────────────────────────────────────────
        metadata = self._build_score_metadata(search_results, question)
        metadata["sql_used"] = sql

        return StrategyResult(
            rows=rows,
            query_used=sql,
            strategy_name=self.strategy_name,
            row_count=len(rows),
            metadata=metadata,
        )

    def _resolve_mysql_query(
        self, generated_query: Any, top_entity: str
    ) -> str:
        """
        Decide which SQL to use.

        Priority:
            1. If generated_query is a non-empty SQL string → use it.
               The LLM already produced a query; vector search just
               confirmed we're looking at the right table.
            2. Otherwise → build a simple SELECT * FROM <top_entity>.

        This makes vector search work even when generated_query is None
        (e.g. when called directly from CombinedStrategy).
        """
        if isinstance(generated_query, str) and generated_query.strip():
            return generated_query.strip()

        # Fallback — safe SELECT * with row cap
        return _FALLBACK_SQL_TEMPLATE.format(
            table=top_entity,
            limit=self._settings.MAX_RESULT_ROWS,
        )

    # ── MongoDB execution path ────────────────────────────────────────────────

    def _execute_mongo(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Vector search pipeline for MongoDB.

        Steps:
            1. Embed question → search vector store → find best collection
            2. Build Mongo filter targeting that collection:
               a. If generated_query is a valid dict → use it (LLM hint)
               b. Otherwise → use empty filter {} (returns all docs)
            3. Validate filter with MongoValidator
            4. Execute via adapter
            5. Return StrategyResult with similarity scores in metadata
        """
        # ── 1. Vector search ─────────────────────────────────────────────────
        search_results = self._search_schema(question)
        top_entity = self._extract_top_entity(search_results)

        if not top_entity:
            raise ValueError(
                "VectorSearchStrategy found no relevant schema entities. "
                "Make sure the schema has been indexed at startup."
            )

        # ── 2. Build Mongo filter ─────────────────────────────────────────────
        mongo_filter = self._resolve_mongo_filter(generated_query, top_entity)

        # ── 3. Validate ───────────────────────────────────────────────────────
        validator = get_validator("mongo")
        is_valid, error = validator.validate(mongo_filter)
        if not is_valid:
            # LLM filter was dangerous — fall back to empty filter
            mongo_filter = dict(_FALLBACK_MONGO_TEMPLATE)

        # ── 4. Execute ────────────────────────────────────────────────────────
        try:
            rows = self.adapter.execute_query(mongo_filter, None)
        except Exception as exc:
            raise RuntimeError(
                f"VectorSearchStrategy failed to execute Mongo filter.\n"
                f"Filter: {mongo_filter}\n"
                f"Error : {exc}"
            ) from exc

        rows = rows[: self._settings.MAX_RESULT_ROWS]

        # ── 5. Return ─────────────────────────────────────────────────────────
        metadata = self._build_score_metadata(search_results, question)
        metadata["filter_used"] = str(mongo_filter)
        metadata["collection"] = top_entity

        return StrategyResult(
            rows=rows,
            query_used=str(mongo_filter),
            strategy_name=self.strategy_name,
            row_count=len(rows),
            metadata=metadata,
        )

    def _resolve_mongo_filter(
        self, generated_query: Any, top_entity: str
    ) -> dict:
        """
        Decide which Mongo filter to use.

        Priority:
            1. If generated_query is a non-empty dict → use it.
               The LLM already produced a filter; vector search just
               confirmed the right collection.
            2. Otherwise → use empty filter {} (returns all documents
               from the matched collection, capped at MAX_RESULT_ROWS).

        Args:
            generated_query: LLM output — may be dict, list, str, or None.
            top_entity:      Best-matched collection name from vector search.

        Returns:
            A dict safe to pass to adapter.execute_query().
        """
        if isinstance(generated_query, dict) and generated_query:
            return generated_query

        # Fallback — empty filter = return all documents
        return dict(_FALLBACK_MONGO_TEMPLATE)