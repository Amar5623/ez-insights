"""
services/query_service.py

Orchestrates the full NL → Answer pipeline.

PIPELINE FLOW (logged at every step):
    1.  [INTENT]      → classify question (conversational vs DB query)
    2.  [SCHEMA_RAG]  → retrieve relevant schema chunks via vector similarity
    3.  [PROMPT]      → build system+user prompt from question + schema
    4.  [LLM_CALL]    → LLM generates a raw query (SQL string or Mongo JSON)
    5.  [PARSE]       → parse LLM output into correct type for DB
    6.  [STRATEGY]    → execute query via strategy (sql / fuzzy / vector / combined)
    7.  [DB_EXEC]     → strategy calls adapter.execute_query()
    8.  [SCRUB]       → mask sensitive values from results
    9.  [ANSWER]      → LLM generates a human-readable answer from rows
    10. [PIPELINE]    → full summary: strategy, row count, total latency

MySQL  → str (SQL)
MongoDB → dict (JSON parsed from LLM output)

This split happens ONCE in _parse_query(). Every strategy and adapter
downstream receives the correct type and doesn't have to worry about parsing.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from core.interfaces import BaseLLM, BaseDBAdapter, BaseStrategy
from core.config.settings import get_settings
from core.logging_config import get_logger, truncate, log_latency
from rag.schema_retriever import SchemaRetriever
from rag.prompt_builder import PromptBuilder
from services.data_scrubber import scrub_rows
from strategies.retry_handler import AttemptRecord

logger = get_logger(__name__)


# ─── Custom exception ─────────────────────────────────────────────────────────

class MaxRetriesExceeded(Exception):
    """Raised when all retry attempts for query generation/execution are exhausted."""


# ─── Response dataclass ───────────────────────────────────────────────────────

@dataclass
class QueryResponse:
    question: str
    sql: str
    results: list[dict]
    row_count: int
    strategy_used: str
    answer: str
    error: str | None = field(default=None)
    # pagination fields (query_service populates these from settings)
    total_rows: int = field(default=0)
    page_size: int = field(default=10)
    all_results: list[dict] = field(default_factory=list)


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

        logger.info(
            f"[PIPELINE] QueryService initialized | "
            f"llm={llm.provider_name} | "
            f"db={adapter.db_type} | "
            f"strategy={strategy.strategy_name}"
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self, question: str, context: list[dict] | None = None) -> QueryResponse:
        """
        Execute the full pipeline for a natural language question.

        Returns:
            QueryResponse with error=None on success, or error message on failure.
            Never raises — all exceptions are caught and returned as error responses.
        """
        pipeline_start = time.perf_counter()

        logger.info(
            f"[PIPELINE] START | "
            f"question={truncate(question, 120)} | "
            f"context_turns={len(context) if context else 0}"
        )

        try:
            result = self._run_pipeline(question, context=context)

            total_ms = int((time.perf_counter() - pipeline_start) * 1000)
            logger.info(
                f"[PIPELINE] COMPLETE | "
                f"strategy={result.strategy_used} | "
                f"rows={result.row_count} | "
                f"total_latency={total_ms}ms | "
                f"question={truncate(question, 80)}"
            )
            return result

        except MaxRetriesExceeded as e:
            total_ms = int((time.perf_counter() - pipeline_start) * 1000)
            logger.warning(
                f"[PIPELINE] FAILED — MaxRetriesExceeded | "
                f"total_latency={total_ms}ms | error={e}"
            )
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
            total_ms = int((time.perf_counter() - pipeline_start) * 1000)
            logger.error(
                f"[PIPELINE] FAILED — Unhandled exception | "
                f"total_latency={total_ms}ms | "
                f"question={truncate(question, 80)} | "
                f"error={exc}",
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
        """Core pipeline — called by run(). Exceptions propagate to run() handler."""

        # ── 1. Retrieve relevant schema chunks ───────────────────────────────
        t0 = time.perf_counter()
        schema_chunks = self.retriever.retrieve(question)
        schema_ms = int((time.perf_counter() - t0) * 1000)

        logger.info(
            f"[SCHEMA_RAG] Retrieved {len(schema_chunks)} chunk(s) | "
            f"latency={schema_ms}ms"
        )
        for i, chunk in enumerate(schema_chunks):
            logger.debug(
                f"[SCHEMA_RAG] chunk[{i}] | "
                f"id={chunk.get('id', '?')} | "
                f"score={chunk.get('score', 0):.4f} | "
                f"text={truncate(str(chunk.get('metadata', {}).get('text', '')), 100)}"
            )

        # ── 2–5. Generate query + execute with retry loop ────────────────────
        strategy_result = self._generate_and_execute(question, schema_chunks)

        # ── 6. Scrub sensitive data from results ─────────────────────────────
        t0 = time.perf_counter()
        scrubbed_rows = scrub_rows(strategy_result.rows)
        scrub_ms = int((time.perf_counter() - t0) * 1000)
        logger.debug(f"[SCRUB] completed | latency={scrub_ms}ms")

        # ── 7. Assess result quality for answer prompt ───────────────────────
        quality = self._assess_result_quality(scrubbed_rows)
        logger.debug(f"[ANSWER] Result quality assessment: {quality}")

        # ── 8. Generate natural language answer ──────────────────────────────
        answer_prompt = self._prompt_builder.build_answer_prompt(
            question=question,
            rows=scrubbed_rows,
            row_count=strategy_result.row_count,
            quality=quality,
            sql_query=strategy_result.query_used,
            context=context or [],
        )

        logger.debug(
            f"[ANSWER] Sending answer prompt to LLM | "
            f"rows_in_prompt={min(len(scrubbed_rows), self._settings.MAX_ROWS_FOR_LLM)} | "
            f"context_turns={len(context) if context else 0}"
        )

        t0 = time.perf_counter()
        answer = self.llm.generate_with_history([
            {"role": "system", "content": answer_prompt["system"]},
            {"role": "user",   "content": answer_prompt["user"]},
        ])
        answer_ms = int((time.perf_counter() - t0) * 1000)

        logger.info(
            f"[ANSWER] Generated | "
            f"latency={answer_ms}ms | "
            f"answer={truncate(answer, 120)}"
        )
        logger.debug(f"[ANSWER] Full answer:\n{answer}")

        # ── 9. Return ────────────────────────────────────────────────────────
        return QueryResponse(
            question=question,
            sql=strategy_result.query_used,
            results=scrubbed_rows,         # all rows (used for all_results in route)
            all_results=scrubbed_rows,     # same — route slices for first page
            row_count=strategy_result.row_count,
            total_rows=strategy_result.row_count,
            page_size=self._settings.PAGE_SIZE,
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
        max_attempts = self._settings.MAX_RETRIES

        for attempt in range(1, max_attempts + 1):

            logger.info(
                f"[LLM_CALL] Attempt {attempt}/{max_attempts} | "
                f"db_type={self.adapter.db_type}"
            )

            # ── Build schema-aware prompt ─────────────────────────────────────
            t0 = time.perf_counter()
            prompt = self._prompt_builder.build_query_prompt(
                question=question,
                schema_chunks=schema_chunks,
                attempt_history=attempt_history or None,
            )
            prompt_ms = int((time.perf_counter() - t0) * 1000)

            logger.debug(
                f"[PROMPT] Built | latency={prompt_ms}ms | "
                f"has_retry_context={bool(attempt_history)}"
            )
            logger.debug(f"[PROMPT] System:\n{prompt['system']}")
            logger.debug(f"[PROMPT] User:\n{prompt['user']}")

            # ── LLM generates raw query text ──────────────────────────────────
            t0 = time.perf_counter()
            raw_query_text = self.llm.generate_with_history([
                {"role": "system", "content": prompt["system"]},
                {"role": "user",   "content": prompt["user"]},
            ])
            llm_ms = int((time.perf_counter() - t0) * 1000)

            logger.info(
                f"[LLM_CALL] Response received | "
                f"attempt={attempt} | "
                f"latency={llm_ms}ms | "
                f"raw_output={truncate(raw_query_text, 150)}"
            )
            logger.debug(f"[LLM_CALL] Full raw output:\n{raw_query_text}")

            # ── Parse LLM output → correct type for this DB ───────────────────
            try:
                t0 = time.perf_counter()
                generated_query = self._parse_query(raw_query_text)
                parse_ms = int((time.perf_counter() - t0) * 1000)

                logger.info(
                    f"[PARSE] Success | "
                    f"attempt={attempt} | "
                    f"db_type={self.adapter.db_type} | "
                    f"query_type={type(generated_query).__name__} | "
                    f"latency={parse_ms}ms | "
                    f"parsed={truncate(str(generated_query), 150)}"
                )

            except ValueError as parse_exc:
                last_exc = parse_exc
                attempt_history.append(
                    AttemptRecord(
                        attempt_number=attempt,
                        query_used=raw_query_text,
                        error=str(parse_exc),
                    )
                )
                logger.warning(
                    f"[PARSE] FAILED | "
                    f"attempt={attempt}/{max_attempts} | "
                    f"error={parse_exc}"
                )
                if attempt == max_attempts:
                    raise MaxRetriesExceeded(str(parse_exc)) from parse_exc
                continue

            # ── Strategy execution ─────────────────────────────────────────────
            try:
                t0 = time.perf_counter()
                logger.info(
                    f"[STRATEGY] Executing | "
                    f"strategy={self.strategy.strategy_name} | "
                    f"attempt={attempt}"
                )

                strategy_result = self.strategy.execute(question, generated_query)
                exec_ms = int((time.perf_counter() - t0) * 1000)

                logger.info(
                    f"[STRATEGY] Success | "
                    f"strategy={strategy_result.strategy_name} | "
                    f"rows={strategy_result.row_count} | "
                    f"latency={exec_ms}ms | "
                    f"query={truncate(str(strategy_result.query_used), 120)}"
                )
                if strategy_result.metadata:
                    logger.debug(
                        f"[STRATEGY] Metadata: {strategy_result.metadata}"
                    )

                return strategy_result

            except Exception as exec_exc:
                last_exc = exec_exc
                attempt_history.append(
                    AttemptRecord(
                        attempt_number=attempt,
                        query_used=str(generated_query),
                        error=str(exec_exc),
                    )
                )
                logger.warning(
                    f"[STRATEGY] FAILED | "
                    f"attempt={attempt}/{max_attempts} | "
                    f"strategy={self.strategy.strategy_name} | "
                    f"error={exec_exc}"
                )
                if attempt == max_attempts:
                    raise MaxRetriesExceeded(str(exec_exc)) from exec_exc

        # Should never reach here — loop always raises or returns
        raise MaxRetriesExceeded(str(last_exc))

    def _parse_query(self, raw_text: str) -> str | dict:
        """
        Parse LLM output into the correct type for the current DB.

        MySQL  → returns str (the SQL string, stripped of markdown fences)
        MongoDB → returns dict (JSON parsed from LLM output)

        Raises:
            ValueError: If MongoDB output cannot be parsed as valid JSON.
        """
        db_type = self.adapter.db_type

        if db_type == "mysql":
            return self._strip_sql(raw_text)

        if db_type == "mongo":
            return self._parse_mongo_json(raw_text)

        raise ValueError(
            f"_parse_query: unsupported db_type='{db_type}'. "
            "Expected 'mysql' or 'mongo'."
        )

    @staticmethod
    def _strip_sql(raw: str) -> str:
        """Strip markdown code fences and whitespace from LLM SQL output."""
        sql = raw.strip()
        # Remove ```sql ... ``` or ``` ... ``` fences
        if sql.startswith("```"):
            lines = sql.split("\n")
            # Drop first line (```sql or ```) and last line (```)
            inner = lines[1:] if lines[-1].strip() == "```" else lines[1:]
            sql = "\n".join(
                line for line in inner if line.strip() != "```"
            ).strip()
        return sql

    @staticmethod
    def _parse_mongo_json(raw: str) -> dict:
        """
        Extract a JSON dict from LLM output.

        The LLM often wraps JSON in markdown fences or adds commentary.
        We strip fences then attempt to extract the outermost { ... } block.

        Raises:
            ValueError: With a descriptive message if JSON cannot be parsed.
        """
        text = raw.strip()

        # Strip markdown fences
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(
                line for line in lines[1:]
                if line.strip() not in ("```", "```json")
            ).strip()

        # Find the outermost { ... } block
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError(
                f"LLM output did not contain a JSON object. "
                f"Got: {repr(text[:200])}"
            )
        json_str = text[start:end + 1]

        try:
            parsed = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"LLM output contained invalid JSON: {exc}. "
                f"Raw snippet: {repr(json_str[:300])}"
            ) from exc

        if not isinstance(parsed, dict):
            raise ValueError(
                f"Expected JSON object (dict), got {type(parsed).__name__}. "
                f"Value: {repr(parsed)}"
            )

        if "collection" not in parsed:
            raise ValueError(
                f"MongoDB query JSON must contain 'collection' key. "
                f"Got keys: {list(parsed.keys())}"
            )

        return parsed

    def _assess_result_quality(self, rows: list[dict]) -> str:
        """
        Classify result quality for the answer prompt.

        Returns one of: 'empty', 'small', 'large'
        The answer prompt uses this to adjust LLM behaviour:
          - 'empty'  → LLM says "no results found"
          - 'small'  → LLM formats all rows as a table
          - 'large'  → LLM summarizes and shows only MAX_ROWS_FOR_LLM rows
        """
        count = len(rows)
        if count == 0:
            return "empty"
        if count <= self._settings.MAX_ROWS_FOR_LLM:
            return "small"
        return "large"