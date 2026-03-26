"""
services/query_service.py
Lead owns this file.

Central orchestration layer — the single point that wires together:
    LLM  →  SchemaRetriever  →  PromptBuilder  →  Strategy  →  DataScrubber

The service is DB-agnostic by design. It does NOT import MySQLAdapter or
MongoAdapter directly — it receives an injected BaseDBAdapter via __init__.

──────────────────────────────────────────────────────────────────────────────
MYSQL vs MONGODB — what this file does differently per DB type
──────────────────────────────────────────────────────────────────────────────

  MySQL:
    - LLM returns a plain SQL string
      e.g. "SELECT * FROM products WHERE price > 100 LIMIT 20"
    - _parse_query() returns the string as-is
    - strategy.execute() receives a str

  MongoDB:
    - LLM returns a JSON string (see MONGO_GENERATION_TEMPLATE in prompt_builder)
      e.g. '{"collection": "products", "filter": {"price": {"$gt": 100}}, "limit": 20}'
      or   '{"collection": "sales", "pipeline": [...], "limit": 5}'
    - _parse_query() calls _parse_mongo_json() to convert the string to a dict
    - strategy.execute() receives a dict
    - On parse failure, the retry loop re-prompts the LLM with the error

This split happens ONCE in _parse_query(). Every strategy and adapter
downstream receives the correct type and doesn't have to worry about parsing.
──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from core.interfaces import BaseLLM, BaseDBAdapter, BaseStrategy
from core.config.settings import get_settings
from rag.schema_retriever import SchemaRetriever
from rag.prompt_builder import PromptBuilder
from services.data_scrubber import scrub_rows
from strategies.retry_handler import AttemptRecord

logger = logging.getLogger("nlsql.query_service")


# ─── Custom exception ─────────────────────────────────────────────────────────

class MaxRetriesExceeded(Exception):
    """Raised when all retry attempts for query generation/execution are exhausted."""


# ─── Response dataclass ───────────────────────────────────────────────────────

@dataclass
class QueryResponse:
    """
    The result returned by QueryService.run() and sent to the API layer.

    Fields mirror api/schemas.py QueryResponse exactly so FastAPI can
    serialise this dataclass directly via the response_model.

    sql:          For MySQL — the SQL string.
                  For MongoDB — the JSON query dict stringified.
    strategy_used: Short name of the strategy that produced the result.
    error:        None on success. Error message on failure (the route
                  turns this into an HTTP 500).
    """
    question: str
    sql: str
    results: list[dict]
    row_count: int
    strategy_used: str
    answer: str
    error: str | None = field(default=None)


# ─── Service ──────────────────────────────────────────────────────────────────

class QueryService:
    """
    Orchestrates the full question → answer pipeline.

    Constructed once at startup (main.py lifespan) with all dependencies
    injected. The same instance handles every request at runtime.

    Args:
        llm:       Language model — generates queries and natural language answers.
        adapter:   Database adapter — MySQL or MongoDB (injected by factory).
        strategy:  Query strategy — SQL, Fuzzy, Vector, Combined, or Router.
        retriever: SchemaRetriever — embeds and retrieves relevant schema chunks.
    """

    def __init__(
        self,
        llm: BaseLLM,
        adapter: BaseDBAdapter,
        strategy: BaseStrategy,
        retriever: SchemaRetriever,
    ):
        self.llm = llm
        self.adapter = adapter
        self.strategy = strategy
        self.retriever = retriever
        self._prompt_builder = PromptBuilder(adapter)
        self._settings = get_settings()

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self, question: str, context: list[dict] | None = None) -> QueryResponse:
        """
        Execute the full pipeline for a natural language question.

        Flow:
            1. Retrieve relevant schema chunks (vector similarity)
            2. Build a schema-aware LLM prompt
            3. LLM generates a query (SQL string for MySQL, JSON dict for MongoDB)
            4. Parse the LLM output into the correct type (_parse_query)
            5. Strategy executes the query against the DB
            6. On failure: build error-aware retry prompt and try again
            7. Scrub sensitive values from results
            8. LLM generates a human-readable answer from the rows
            9. Return QueryResponse

        Returns:
            QueryResponse with error=None on success, or error message on failure.
            Never raises — all exceptions are caught and returned as error responses.
        """
        try:
            return self._run_pipeline(question, context=context)
        except MaxRetriesExceeded as e:
            logger.warning(f"[QueryService] MaxRetriesExceeded: {e}")
            return QueryResponse(
                question=question,
                sql="",
                results=[],
                row_count=0,
                strategy_used=self.strategy.strategy_name,
                answer="",
                error=(
                    f"Could not generate a valid query after "
                    f"{self._settings.MAX_RETRIES} attempts. "
                    f"Try rephrasing your question. Detail: {e}"
                ),
            )
        except Exception as exc:
            logger.error(
                f"[QueryService] Unhandled error for question={question!r}: {exc}",
                exc_info=True,
            )
            return QueryResponse(
                question=question,
                sql="",
                results=[],
                row_count=0,
                strategy_used="error",
                answer="An internal error occurred. Please try again.",
                error=str(exc),
            )

    # ── Private pipeline ──────────────────────────────────────────────────────

    def _run_pipeline(self, question: str, context: list[dict] | None = None) -> QueryResponse:
        """
        Core pipeline — called by run(). Exceptions propagate to run() handler.
        """
        # ── 1. Retrieve relevant schema chunks ───────────────────────────────
        schema_chunks = self.retriever.retrieve(question)
        logger.debug(
            f"[QueryService] Retrieved {len(schema_chunks)} schema chunk(s) "
            f"for question={question!r}"
        )

        # ── 2–5. Generate query + execute with retry loop ────────────────────
        strategy_result = self._generate_and_execute(question, schema_chunks)

        # ── 6. Scrub sensitive data from results ─────────────────────────────
        scrubbed_rows = scrub_rows(strategy_result.rows)

        # ── 7. Assess result quality for answer prompt ───────────────────────
        quality = self._assess_result_quality(scrubbed_rows)

        # ── 8. Generate natural language answer ──────────────────────────────
        answer_prompt = self._prompt_builder.build_answer_prompt(
            question=question,
            rows=scrubbed_rows,
            row_count=strategy_result.row_count,
            quality=quality,
            sql_query=strategy_result.query_used,
            context=context or [],
        )
        # Use generate_with_history so the ClassicModels Analytics Assistant
        # system prompt is applied as the "system" role.
        answer = self.llm.generate_with_history([
            {"role": "system",  "content": answer_prompt["system"]},
            {"role": "user",    "content": answer_prompt["user"]},
        ])
        
        logger.info(
            f"[QueryService] OK | strategy={strategy_result.strategy_name} "
            f"rows={strategy_result.row_count} | question={question!r}"
        )

        # ── 9. Return ────────────────────────────────────────────────────────
        return QueryResponse(
            question=question,
            sql=strategy_result.query_used,
            results=scrubbed_rows,
            row_count=strategy_result.row_count,
            strategy_used=strategy_result.strategy_name,
            answer=answer,
            error=None,
        )

    def _generate_and_execute(self, question: str, schema_chunks: list[dict]):
        """
        LLM query generation + strategy execution with retry loop.

        On each attempt:
            1. Build a schema-aware prompt (includes error history on retries)
            2. Ask LLM to generate a query
            3. Parse the LLM output into the correct type for this DB
            4. Pass to strategy.execute()
            5. On failure: record the error and retry with updated prompt

        Raises:
            MaxRetriesExceeded: After MAX_RETRIES failures.
        """
        attempt_history: list[AttemptRecord] = []
        last_exc: Exception | None = None

        for attempt in range(1, self._settings.MAX_RETRIES + 1):

            # ── Build schema-aware prompt (includes errors from prior attempts)
            # Returns {"system": ..., "user": ...} — passed as separate roles.
            prompt = self._prompt_builder.build_query_prompt(
                question=question,
                schema_chunks=schema_chunks,
                attempt_history=attempt_history or None,
            )
 
            # ── LLM generates raw query text ──────────────────────────────────
            # Use generate_with_history so the system prompt is injected as the
            # "system" role, giving the LLM its full behavioural instruction set.
            raw_query_text = self.llm.generate_with_history([
                {"role": "system",  "content": prompt["system"]},
                {"role": "user",    "content": prompt["user"]},
            ])
            logger.debug(
                f"[QueryService] Attempt {attempt}/{self._settings.MAX_RETRIES} "
                f"— raw LLM output: {raw_query_text[:200]!r}"
            )

            # ── Parse LLM output → correct type for this DB ───────────────────
            # MySQL  → str (SQL)   |   MongoDB → dict (JSON parsed to dict)
            try:
                generated_query = self._parse_query(raw_query_text)
            except ValueError as parse_exc:
                # JSON parse failure for MongoDB counts as a failed attempt
                last_exc = parse_exc
                attempt_history.append(
                    AttemptRecord(
                        attempt_number=attempt,
                        query_used=raw_query_text,
                        error=str(parse_exc),
                    )
                )
                logger.warning(
                    f"[QueryService] Attempt {attempt} parse error: {parse_exc}"
                )
                if attempt == self._settings.MAX_RETRIES:
                    raise MaxRetriesExceeded(str(parse_exc)) from parse_exc
                continue

            # ── Execute strategy ──────────────────────────────────────────────
            try:
                result = self.strategy.execute(question, generated_query)
                logger.debug(
                    f"[QueryService] Attempt {attempt} succeeded — "
                    f"strategy={result.strategy_name} rows={result.row_count}"
                )
                return result
            except Exception as exc:
                last_exc = exc
                attempt_history.append(
                    AttemptRecord(
                        attempt_number=attempt,
                        query_used=str(generated_query),
                        error=str(exc),
                    )
                )
                logger.warning(
                    f"[QueryService] Attempt {attempt} execution error: {exc}"
                )
                if attempt == self._settings.MAX_RETRIES:
                    raise MaxRetriesExceeded(str(exc)) from exc

        # Should never reach here, but satisfy the type checker
        raise MaxRetriesExceeded(str(last_exc))

    # ── Query parsing — the MySQL / MongoDB split ─────────────────────────────

    def _parse_query(self, raw: str) -> Any:
        """
        Parse the LLM's raw text output into the correct query type.

        ┌──────────┬──────────────────────────────────────────────────────────┐
        │  MySQL   │  LLM returns a SQL string → returned as-is (stripped)    │
        │  MongoDB │  LLM returns a JSON string → parsed into a Python dict   │
        └──────────┴──────────────────────────────────────────────────────────┘

        This is the single point where DB type drives behaviour in this file.
        Every downstream component (strategy, adapter) receives the correct type
        and does not need to handle the raw LLM string.

        Raises:
            ValueError: If the MongoDB JSON cannot be parsed into a valid dict.
        """
        # ── MySQL: return the SQL string unchanged ────────────────────────────
        if self.adapter.db_type == "mysql":
            return raw.strip()

        # ── MongoDB: parse JSON string → Python dict ──────────────────────────
        return self._parse_mongo_json(raw)

    def _parse_mongo_json(self, raw: str) -> dict:
        """
        Parse the LLM's MongoDB query JSON string into a Python dict.

        The LLM is instructed to return raw JSON (see MONGO_GENERATION_TEMPLATE
        in prompt_builder.py), but in practice may include:
          - Markdown code fences: ```json ... ```
          - Surrounding explanation text
          - Slightly malformed JSON (trailing commas, single quotes)

        This method is defensive against all common LLM formatting errors.

        Returns:
            A dict with at minimum a "collection" key.
            Either "filter" (simple find) or "pipeline" (aggregation) must also
            be present — the adapter handles both cases.

        Raises:
            ValueError: If the JSON cannot be parsed or is missing required keys.

        MongoDB-specific — not called for MySQL.
        """
        text = raw.strip()

        # ── Strip markdown code fences if present ─────────────────────────────
        # LLM sometimes wraps output in ```json ... ``` despite instructions
        if text.startswith("```"):
            lines = text.splitlines()
            inner_lines = [ln for ln in lines[1:] if ln.strip() != "```"]
            text = "\n".join(inner_lines).strip()

        # ── Extract the outermost { ... } in case the LLM added prose ─────────
        brace_start = text.find("{")
        brace_end = text.rfind("}")
        if brace_start != -1 and brace_end != -1 and brace_end > brace_start:
            text = text[brace_start : brace_end + 1]

        # ── Parse JSON ────────────────────────────────────────────────────────
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"[MongoDB] LLM returned invalid JSON for query.\n"
                f"Parse error : {e}\n"
                f"LLM output  : {raw[:400]!r}\n"
                f"Hint: The LLM must return a raw JSON object starting with "
                f"'{{' and ending with '}}'. Check the prompt template."
            ) from e

        # ── Validate structure ────────────────────────────────────────────────
        if not isinstance(parsed, dict):
            raise ValueError(
                f"[MongoDB] Expected a JSON object (dict), got {type(parsed).__name__}.\n"
                f"LLM output: {raw[:400]!r}"
            )

        if "collection" not in parsed:
            raise ValueError(
                f"[MongoDB] JSON query must have a 'collection' key.\n"
                f"Got keys  : {list(parsed.keys())}\n"
                f"LLM output: {raw[:400]!r}\n"
                f"Hint: The prompt instructs the LLM to always include 'collection'."
            )

        if "filter" not in parsed and "pipeline" not in parsed:
            raise ValueError(
                f"[MongoDB] JSON query must have either 'filter' or 'pipeline' key.\n"
                f"Got keys  : {list(parsed.keys())}\n"
                f"LLM output: {raw[:400]!r}"
            )

        logger.debug(
            f"[MongoDB] Parsed query — collection={parsed['collection']!r}, "
            f"type={'pipeline' if 'pipeline' in parsed else 'filter'}"
        )
        return parsed

    # ── Result quality assessment ─────────────────────────────────────────────

    def _assess_result_quality(self, rows: list[dict]) -> str:
        """
        Inspect result rows and return a quality signal for the answer prompt.

        The PromptBuilder uses this to choose the right instruction for the LLM:
            'ok'            → normal answer generation
            'empty'         → tell user nothing was found, suggest rephrasing
            'all_null'      → data exists but values are all null/empty
            'low_relevance' → rows returned but may not answer the question

        Works the same for MySQL and MongoDB — only the row content matters.
        """
        if not rows:
            return "empty"

        # Check whether every value in every row is null / empty
        all_null = all(
            v is None or v == "" or v == [] or v == {}
            for row in rows
            for v in row.values()
        )
        if all_null:
            return "all_null"

        return "ok"