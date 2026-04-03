"""
strategies/combined.py
Dev 2 owns this file.

Orchestrates SQLFilterStrategy + FuzzyMatchStrategy + VectorSearchStrategy
together, merges their results, deduplicates, and re-ranks by a combined score.

Used for complex queries that have multiple signals at once, e.g.:
    "sci-fi books about loneliness under $15 by an author named Asimov"
     ↑ sql_filter (price < 15)   ↑ vector (about loneliness)  ↑ fuzzy (Asimov)

Also used as the fallback when the router is uncertain — it handles anything.

How it fits in the system:
    factory/strategy_factory.py creates this when STRATEGY=combined or STRATEGY=auto
    and the router cannot confidently pick one strategy.

Flow:
    execute(question, generated_query)
        ├── _run_safe(SQLFilterStrategy, ...)   → sql_result   (may be empty)
        ├── _run_safe(FuzzyMatchStrategy, ...)  → fuzzy_result (may be empty)
        ├── _run_safe(VectorSearchStrategy, ...) → vector_result (may be empty)
        ├── _merge_results([sql_result, fuzzy_result, vector_result])
        │       ├── deduplicate by primary key (_find_pk_value)
        │       └── boost rows that appear in multiple strategy results
        ├── _rank_rows(merged)                  → sorted by boost score desc
        ├── cap at MAX_RESULT_ROWS
        └── StrategyResult with per-strategy metadata
"""

from __future__ import annotations

import json
import logging
from typing import Any

from core.interfaces import BaseDBAdapter, BaseStrategy, StrategyResult
from core.config.settings import get_settings
from strategies.sql_filter import SQLFilterStrategy
from strategies.fuzzy_match import FuzzyMatchStrategy
from strategies.vector_search import VectorSearchStrategy


# ─── Primary key candidate column names (checked in order) ───────────────────
# We try these column names to find a unique identifier for deduplication.
# If none match, we fall back to a hash of the full row dict.
_PK_CANDIDATES: list[str] = [
    "id", "_id", "uuid", "pk",
    "product_id", "order_id", "user_id", "book_id", "item_id",
]

# Score boost given to a row for each additional strategy that returned it.
# A row returned by all 3 strategies gets boost = 2 * _MULTI_MATCH_BOOST.
_MULTI_MATCH_BOOST = 1.0


class CombinedStrategy(BaseStrategy):
    """
    Runs all three sub-strategies in parallel (safe — failures are caught),
    merges results, deduplicates by primary key, and re-ranks rows that
    appear in multiple strategy results.

    Sub-strategy failures are non-fatal:
        If SQLFilterStrategy raises (e.g. LLM produced bad SQL), we catch
        the error, record it in metadata, and continue with the other two.
        The combined result is still useful even with one strategy failing.

    Dependency injection:
        vector_store and embedder are passed in from the factory (Lead wires
        this). They are forwarded to VectorSearchStrategy and FuzzyMatchStrategy
        at execute() time — not at __init__ time, so the same CombinedStrategy
        instance works regardless of which sub-strategies need them.
    """

    def __init__(
        self,
        adapter: BaseDBAdapter,
        vector_store=None,
        embedder=None,
    ):
        super().__init__(adapter)
        self.vector_store = vector_store
        self.embedder = embedder
        self._settings = get_settings()

    # ── Public interface ──────────────────────────────────────────────────────

    def execute(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Run all sub-strategies, merge results, rank, and return.

        Args:
            question:        Original natural language question.
            generated_query: SQL string (MySQL) or filter dict (MongoDB)
                             from the LLM — passed to all sub-strategies.

        Returns:
            StrategyResult with merged rows ranked by combined score.
            Never raises — sub-strategy failures are recorded in metadata.
        """
        # ── 1. Run all three sub-strategies safely ───────────────────────────
        sql_result, sql_error = self._run_safe(
            SQLFilterStrategy(self.adapter),
            question,
            generated_query,
            label="sql_filter",
        )

        fuzzy_result, fuzzy_error = self._run_safe(
            FuzzyMatchStrategy(self.adapter),
            question,
            generated_query,
            label="fuzzy",
        )

        vector_result, vector_error = self._run_safe(
            VectorSearchStrategy(
                self.adapter,
                vector_store=self.vector_store,
                embedder=self.embedder,
            ),
            question,
            generated_query,
            label="vector",
        )

        # ── 2. Collect non-empty results ─────────────────────────────────────
        sub_results: list[StrategyResult] = [
            r for r in [sql_result, fuzzy_result, vector_result]
            if r is not None and r.rows
        ]

        # ── 3. Merge + deduplicate + rank ────────────────────────────────────
        merged_rows = self._merge_results(sub_results)

        # ── 4. Cap at MAX_RESULT_ROWS ────────────────────────────────────────
        merged_rows = merged_rows[: self._settings.MAX_RESULT_ROWS]

        # ── 5. Build metadata ────────────────────────────────────────────────
        metadata = self._build_metadata(
            sql_result=sql_result,
            fuzzy_result=fuzzy_result,
            vector_result=vector_result,
            sql_error=sql_error,
            fuzzy_error=fuzzy_error,
            vector_error=vector_error,
            question=question,
        )

        return StrategyResult(
            rows=merged_rows,
            query_used=self._summarise_queries(
                sql_result, fuzzy_result, vector_result
            ),
            strategy_name=self.strategy_name,
            row_count=len(merged_rows),
            metadata=metadata,
        )

    def can_handle(self, question: str) -> bool:
        """Always True — CombinedStrategy is the universal fallback."""
        return True

    @property
    def strategy_name(self) -> str:
        return "combined"

    # ── Safe sub-strategy runner ──────────────────────────────────────────────

    def _run_safe(
        self,
        strategy: BaseStrategy,
        question: str,
        generated_query: Any,
        label: str,
    ) -> tuple[StrategyResult | None, str | None]:
        """
        Execute a sub-strategy and catch any exception.

        Returns:
            (StrategyResult, None)   — success
            (None, error_message)    — failure, error recorded

        This is what makes CombinedStrategy resilient — if SQLFilterStrategy
        fails because the LLM produced bad SQL, we still get fuzzy + vector
        results rather than crashing entirely.
        """
        try:
            result = strategy.execute(question, generated_query)
            logging.getLogger(__name__).info(
            f"[COMBINED] {label} → rows={result.row_count} | query={str(result.query_used)[:100]}"
        )
            return result, None
        except Exception as exc:
            logging.getLogger(__name__).error(f"[COMBINED] {label} failed: {exc}")
            return None, f"{label} failed: {exc}"

    # ── Merge + deduplication ─────────────────────────────────────────────────

    def _merge_results(
        self, sub_results: list[StrategyResult]
    ) -> list[dict]:
        """
        Merge rows from multiple strategy results, deduplicate by primary key,
        and boost rows that appear in more than one result.

        Algorithm:
            1. For each strategy result, iterate over its rows.
            2. Compute a unique key for each row (_find_pk_value).
            3. If the key was already seen → increment its boost score.
            4. If new → add to seen dict with boost score = 0.
            5. Sort all unique rows by boost score descending.

        Boost scoring:
            - Row seen in 1 strategy  → boost = 0
            - Row seen in 2 strategies → boost = 1.0
            - Row seen in 3 strategies → boost = 2.0

        Rows with higher boost scores appear first — they are the most
        confidently relevant results across multiple search methods.

        Returns:
            List of unique row dicts, sorted by boost score descending.
        """
        # seen: pk_key → {"row": dict, "boost": float, "order": int}
        seen: dict[Any, dict] = {}
        insertion_order = 0

        for result in sub_results:
            for row in result.rows:
                pk = self._find_pk_value(row)

                if pk in seen:
                    # Row already seen from another strategy → boost it
                    seen[pk]["boost"] += _MULTI_MATCH_BOOST
                else:
                    seen[pk] = {
                        "row": row,
                        "boost": 0.0,
                        "order": insertion_order,
                    }
                    insertion_order += 1

        # Sort: boost descending, then insertion order ascending (stable)
        sorted_entries = sorted(
            seen.values(),
            key=lambda e: (-e["boost"], e["order"]),
        )

        return [entry["row"] for entry in sorted_entries]

    def _find_pk_value(self, row: dict) -> Any:
        """
        Find the primary key value of a row for deduplication.

        Checks _PK_CANDIDATES in order. If none found, falls back to
        a hash of the entire row dict (converted to a sorted JSON string
        so it is stable across different dict orderings).

        Returns:
            The primary key value (any hashable type), or a string hash
            of the full row if no PK column is found.
        """
        for pk_col in _PK_CANDIDATES:
            if pk_col in row:
                return row[pk_col]

        # Fallback — hash the full row as a stable JSON string
        try:
            return json.dumps(row, sort_keys=True, default=str)
        except Exception:
            return str(row)

    # ── Ranking ───────────────────────────────────────────────────────────────

    def _rank_rows(self, rows: list[dict]) -> list[dict]:
        """
        Rows are already sorted by _merge_results(). This method exists
        as an explicit hook for future ranking enhancements (e.g. adding
        vector similarity scores to the boost calculation).

        Currently a no-op passthrough — returns rows unchanged.
        """
        return rows

    # ── Query summary ─────────────────────────────────────────────────────────

    def _summarise_queries(
        self,
        sql_result: StrategyResult | None,
        fuzzy_result: StrategyResult | None,
        vector_result: StrategyResult | None,
    ) -> str:
        """
        Build a human-readable summary of which queries were executed
        by each sub-strategy.

        This is stored in StrategyResult.query_used and shown in the
        SqlPreview component in the frontend.

        Example output:
            sql_filter: SELECT * FROM products WHERE price < 15
            fuzzy: SELECT * FROM products WHERE author = 'Asimov'
            vector: {'category': 'Sci-Fi'}
        """
        parts: list[str] = []

        if sql_result:
            parts.append(f"sql_filter: {sql_result.query_used}")
        if fuzzy_result:
            parts.append(f"fuzzy: {fuzzy_result.query_used}")
        if vector_result:
            parts.append(f"vector: {vector_result.query_used}")

        return "\n".join(parts) if parts else "no queries executed"

    # ── Metadata builder ──────────────────────────────────────────────────────

    def _build_metadata(
        self,
        sql_result: StrategyResult | None,
        fuzzy_result: StrategyResult | None,
        vector_result: StrategyResult | None,
        sql_error: str | None,
        fuzzy_error: str | None,
        vector_error: str | None,
        question: str,
    ) -> dict:
        """
        Build the metadata dict for the combined StrategyResult.

        Includes:
            - Per-strategy row counts and errors
            - Total rows before and after deduplication
            - Which strategies succeeded vs failed
            - The original question

        Example:
            {
                "question": "sci-fi books about loneliness under $15",
                "sub_results": {
                    "sql_filter": {"rows": 5, "error": None},
                    "fuzzy":      {"rows": 3, "error": None},
                    "vector":     {"rows": 8, "error": None},
                },
                "strategies_run": ["sql_filter", "fuzzy", "vector"],
                "strategies_failed": [],
            }
        """
        def _count(r: StrategyResult | None) -> int:
            return r.row_count if r is not None else 0

        strategies_run = []
        strategies_failed = []

        for label, result, error in [
            ("sql_filter", sql_result, sql_error),
            ("fuzzy",      fuzzy_result, fuzzy_error),
            ("vector",     vector_result, vector_error),
        ]:
            if error:
                strategies_failed.append(label)
            else:
                strategies_run.append(label)

        return {
            "question": question,
            "sub_results": {
                "sql_filter": {
                    "rows": _count(sql_result),
                    "error": sql_error,
                },
                "fuzzy": {
                    "rows": _count(fuzzy_result),
                    "error": fuzzy_error,
                },
                "vector": {
                    "rows": _count(vector_result),
                    "error": vector_error,
                },
            },
            "strategies_run": strategies_run,
            "strategies_failed": strategies_failed,
        }