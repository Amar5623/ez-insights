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

──────────────────────────────────────────────────────────────────────────────
MYSQL vs MONGODB — what this strategy does differently
──────────────────────────────────────────────────────────────────────────────

  MySQL:
    generated_query is a str or None
    _execute_mysql():
      • Vector-searches schema → finds the best matching table
      • Uses generated_query as-is if it's a valid SQL string
      • Falls back to "SELECT * FROM <table> LIMIT N" if not
      • Validates with MySQLValidator
      • Executes via adapter.execute_query(sql_string, None)

  MongoDB:
    generated_query is a dict or None
    _execute_mongo():
      • Vector-searches schema → finds the best matching collection
      • _resolve_mongo_query() builds the final query dict:
          - If generated_query is a valid dict: uses its filter/pipeline
            but OVERRIDES its collection with the vector-matched collection
            (vector search is the authoritative source for collection name)
          - If generated_query is missing or invalid: falls back to
            {"collection": top_entity, "filter": {}, "limit": N}
      • Validates with MongoValidator
      • Falls back to safe empty-filter query if validation fails
      • Executes via adapter.execute_query(query_dict, None)

  CRITICAL BUG THAT WAS FIXED:
    The old fallback was:
        _FALLBACK_MONGO_TEMPLATE: dict = {}   ← empty dict, no "collection" key!

    MongoAdapter.execute_query() always requires a "collection" key and
    raises ValueError without it. So every time the fallback was triggered,
    the strategy crashed.

    The fix: the fallback is now built dynamically in _resolve_mongo_query()
    using the vector-matched collection name (top_entity). There is no longer
    a static empty-dict template.
──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
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

# ── MySQL: fallback SQL when we know the table but have no specific filter ────
_FALLBACK_SQL_TEMPLATE = "SELECT * FROM `{table}` LIMIT {limit}"

# ── MongoDB: there is NO static fallback dict anymore ────────────────────────
# The old `_FALLBACK_MONGO_TEMPLATE: dict = {}` was BROKEN — it had no
# "collection" key, so MongoAdapter.execute_query() always raised ValueError.
# The fallback is now built dynamically in _resolve_mongo_query() using the
# vector-matched collection name. See that method for details.


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
    MongoDB — builds a query dict targeting the matched collection, using
              generated_query's filter/pipeline if available, always setting
              the collection name from the vector search result.

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
            adapter:      Injected DB adapter (MySQL or Mongo).
            vector_store: Injected BaseVectorStore — FAISS or Pinecone.
            embedder:     Injected BaseEmbedder — Nomic or Cohere.
            top_k:        How many schema matches to retrieve from the
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
            generated_query: SQL string (MySQL) or query dict (MongoDB)
                             from the LLM — used as a filter hint if available,
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

        # ── MySQL path ────────────────────────────────────────────────────────
        if db in ("mysql", "postgres"):
            return self._execute_mysql(question, generated_query)

        # ── MongoDB path ──────────────────────────────────────────────────────
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

        Works the same for MySQL and MongoDB (question text is DB-agnostic).
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
        SHARED: Embed the question and search the vector store for the most
        relevant schema chunks.

        Returns search result dicts sorted by similarity descending.
        Same implementation for MySQL and MongoDB.
        """
        query_vector = self.embedder.embed(question)
        results = self.vector_store.search(query_vector, top_k=self.top_k)
        return results  # already sorted descending by score

    def _extract_top_entity(self, search_results: list[dict]) -> str | None:
        """
        SHARED: Extract the best-matching table/collection name from vector
        search results. Returns the 'id' field of the top result, or None.
        """
        if not search_results:
            return None
        return search_results[0]["id"]

    def _build_score_metadata(
        self, search_results: list[dict], question: str
    ) -> dict:
        """
        SHARED: Build the metadata dict included in StrategyResult.
        Contains similarity scores for all matched schema entities.
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

    # ══════════════════════════════════════════════════════════════════════════
    # MYSQL EXECUTION PATH
    # All methods in this section operate on SQL strings only.
    # They are never called when DB_TYPE=mongo.
    # ══════════════════════════════════════════════════════════════════════════

    def _execute_mysql(self, question: str, generated_query: Any) -> StrategyResult:
        """
        MYSQL-SPECIFIC: Vector search pipeline for MySQL.

        Steps:
            1. Embed question → search vector store → find best table
            2. Build SQL targeting that table:
               a. If generated_query is a valid SQL string → use it as-is
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
                "[MySQL] VectorSearchStrategy found no relevant schema entities. "
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
                f"[MySQL] VectorSearchStrategy failed to execute query.\n"
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
        MYSQL-SPECIFIC: Decide which SQL to use.

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

    # ══════════════════════════════════════════════════════════════════════════
    # MONGODB EXECUTION PATH
    # All methods in this section operate on query dicts only.
    # They are never called when DB_TYPE=mysql.
    # ══════════════════════════════════════════════════════════════════════════

    def _execute_mongo(self, question: str, generated_query: Any) -> StrategyResult:
        """
        MONGO-SPECIFIC: Vector search pipeline for MongoDB.

        Steps:
            1. Embed question → search vector store → find best collection
            2. Build complete Mongo query dict via _resolve_mongo_query():
               - Always sets "collection" to the vector-matched entity
               - Uses filter/pipeline from generated_query if available
               - Falls back to empty filter if generated_query is invalid
            3. Validate with MongoValidator
               - If invalid, fall back to safe empty-filter query
            4. Execute via adapter
            5. Return StrategyResult with similarity scores in metadata

        PREVIOUS BUG:
            The fallback was `dict(_FALLBACK_MONGO_TEMPLATE)` where
            _FALLBACK_MONGO_TEMPLATE = {} — an empty dict with no "collection"
            key. MongoAdapter.execute_query() requires "collection" and
            crashed every time the fallback was used.

        FIX:
            The fallback is now built in _resolve_mongo_query() dynamically
            using top_entity as the collection name.
        """
        # ── 1. Vector search ─────────────────────────────────────────────────
        search_results = self._search_schema(question)
        top_entity = self._extract_top_entity(search_results)

        if not top_entity:
            raise ValueError(
                "[MongoDB] VectorSearchStrategy found no relevant schema entities. "
                "Make sure the schema has been indexed at startup."
            )

        # ── 2. Build complete Mongo query dict ────────────────────────────────
        mongo_query = self._resolve_mongo_query(generated_query, top_entity)

        # ── 3. Validate ───────────────────────────────────────────────────────
        validator = get_validator("mongo")
        is_valid, error = validator.validate(mongo_query)
        if not is_valid:
            # LLM filter was dangerous — fall back to safe empty-filter query
            # for the vector-matched collection
            mongo_query = {
                "collection": top_entity,
                "filter": {},
                "limit": self._settings.MAX_RESULT_ROWS,
            }

        # ── 4. Execute ────────────────────────────────────────────────────────
        try:
            rows = self.adapter.execute_query(mongo_query, None)
        except Exception as exc:
            raise RuntimeError(
                f"[MongoDB] VectorSearchStrategy failed to execute query.\n"
                f"Query : {json.dumps(mongo_query, ensure_ascii=False, default=str)}\n"
                f"Error : {exc}"
            ) from exc

        rows = rows[: self._settings.MAX_RESULT_ROWS]

        # ── 5. Return ─────────────────────────────────────────────────────────
        metadata = self._build_score_metadata(search_results, question)
        metadata["filter_used"] = json.dumps(
            mongo_query, ensure_ascii=False, default=str
        )
        metadata["collection"] = top_entity

        return StrategyResult(
            rows=rows,
            query_used=json.dumps(mongo_query, ensure_ascii=False, indent=2),
            strategy_name=self.strategy_name,
            row_count=len(rows),
            metadata=metadata,
        )

    def _resolve_mongo_query(
        self, generated_query: Any, top_entity: str
    ) -> dict:
        """
        MONGO-SPECIFIC: Build the final Mongo query dict, ensuring the
        "collection" key is always set to the vector-matched entity.

        The vector search result is the authoritative source for the collection
        name — it is always set from top_entity, even if generated_query also
        has a "collection" key (the LLM may guess the wrong collection).

        Priority order:
          1. If generated_query is a valid dict with a "filter" or "pipeline" key:
             → use its filter/pipeline, set collection = top_entity
          2. Otherwise (generated_query is None, a string, or an incomplete dict):
             → fallback: empty filter on top_entity collection

        NOTE ON PREVIOUS BUG:
            The old `_FALLBACK_MONGO_TEMPLATE: dict = {}` approach returned an
            empty dict with no "collection" key. The adapter always crashed.
            This method always produces a dict with a "collection" key.

        Args:
            generated_query: LLM output after JSON parsing — may be dict, str,
                             or None. Only dicts with filter/pipeline are used.
            top_entity:      Collection name from vector search.

        Returns:
            A complete query dict ready for adapter.execute_query().
        """
        if isinstance(generated_query, dict) and generated_query:
            query = {}
            # Vector search is authoritative on collection name
            query["collection"] = top_entity
            # Carry over filter or pipeline from the LLM's query
            if "pipeline" in generated_query:
                query["pipeline"] = generated_query["pipeline"]
            elif "filter" in generated_query:
                query["filter"] = generated_query["filter"]
            else:
                # Dict exists but has neither filter nor pipeline — empty filter
                query["filter"] = {}
            query["limit"] = generated_query.get("limit", self._settings.MAX_RESULT_ROWS)
            return query

        # Fallback — empty filter on vector-matched collection
        # "Give me all documents from the most relevant collection"
        return {
            "collection": top_entity,
            "filter": {},
            "limit": self._settings.MAX_RESULT_ROWS,
        }