"""
services/query_service.py

Orchestrates the full NL → Answer pipeline.

KEY CHANGES FROM PREVIOUS VERSION:

  1. Context is now passed all the way through to SQL generation.
     Previously: context was only used for the answer prompt (step 8).
     Now: context is also passed to build_query_prompt (step 3) so the LLM
     knows what it was querying in previous turns.

  2. PAGINATION intent gets a separate fast path:
     - Schema retrieval uses the PREVIOUS question (not "show more") so FAISS
       returns the same schema chunks as the previous query.
     - pagination_offset is calculated from the previous turn's answer text.
     - The prompt builder receives is_pagination=True and builds a dedicated
       "take this SQL and add LIMIT/OFFSET" prompt instead of the normal one.
     - Quality is set to "pagination" so the answer explains which rows are shown.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any

from core.interfaces import BaseLLM, BaseDBAdapter, BaseStrategy
from core.config.settings import get_settings
from core.logging_config import get_logger, truncate, log_latency
from rag.schema_retriever import SchemaRetriever
from rag.prompt_builder import PromptBuilder
from services.data_scrubber import scrub_rows
from services.intent_classifier import IntentType
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
    total_rows: int = field(default=0)
    page_size: int = field(default=10)
    all_results: list[dict] = field(default_factory=list)


# ─── Service ──────────────────────────────────────────────────────────────────

class QueryService:

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

    def run(
        self,
        question: str,
        context: list[dict] | None = None,
        intent: IntentType = IntentType.DB_QUERY,
    ) -> QueryResponse:
        """
        Execute the full pipeline for a natural language question.

        Args:
            question: The user's natural language question.
            context:  Prior turns: [{"question": str, "sql": str, "answer": str}, ...]
            intent:   Pre-classified intent. Defaults to DB_QUERY.
                      Pass IntentType.PAGINATION when the user typed "show more".
        """
        pipeline_start = time.perf_counter()
        is_pagination = (intent == IntentType.PAGINATION)

        logger.info(
            f"[PIPELINE] START | "
            f"question={truncate(question, 120)} | "
            f"context_turns={len(context) if context else 0} | "
            f"intent={intent.value}"
        )

        try:
            result = self._run_pipeline(
                question,
                context=context,
                is_pagination=is_pagination,
            )

            total_ms = int((time.perf_counter() - pipeline_start) * 1000)
            logger.info(
                f"[PIPELINE] COMPLETE | "
                f"strategy={result.strategy_used} | "
                f"rows={result.row_count} | "
                f"total_latency={total_ms}ms"
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
                f"total_latency={total_ms}ms | error={exc}",
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

    def _run_pipeline(
        self,
        question: str,
        context: list[dict] | None = None,
        is_pagination: bool = False,
    ) -> QueryResponse:

        ctx = context or []

        # ── 1. Schema retrieval ───────────────────────────────────────────────
        # PAGINATION FIX: Use the PREVIOUS question for schema retrieval,
        # not "show more" / "next". "show more" has no semantic relationship
        # to any table, so FAISS would return random chunks. The previous
        # question ("show me top customers") will return the correct chunks.
        if is_pagination and ctx:
            retrieval_question = self._get_previous_question(ctx)
            logger.info(
                f"[SCHEMA_RAG] Pagination mode — using previous question for retrieval | "
                f"retrieval_question={truncate(retrieval_question, 80)}"
            )
        else:
            retrieval_question = question

        t0 = time.perf_counter()
        schema_chunks = self.retriever.retrieve(retrieval_question)
        schema_ms = int((time.perf_counter() - t0) * 1000)

        logger.info(
            f"[SCHEMA_RAG] Retrieved {len(schema_chunks)} chunk(s) | "
            f"latency={schema_ms}ms"
        )

        # ── 2. Calculate pagination offset ────────────────────────────────────
        # How many rows have already been shown? Extract from the previous
        # answer text ("Showing 10 of 47 results") or fall back to PAGE_SIZE.
        pagination_offset = 0
        if is_pagination and ctx:
            pagination_offset = self._extract_pagination_offset(ctx)
            logger.info(
                f"[PAGINATION] offset={pagination_offset} | "
                f"next_page={pagination_offset}–{pagination_offset + self._settings.PAGE_SIZE}"
            )

        # ── 3–5. Generate query + execute ────────────────────────────────────
        strategy_result = self._generate_and_execute(
            question=question,
            schema_chunks=schema_chunks,
            context=ctx,
            is_pagination=is_pagination,
            pagination_offset=pagination_offset,
        )

        # ── 6. Scrub sensitive data ───────────────────────────────────────────
        t0 = time.perf_counter()
        scrubbed_rows = scrub_rows(strategy_result.rows)
        scrub_ms = int((time.perf_counter() - t0) * 1000)
        logger.debug(f"[SCRUB] completed | latency={scrub_ms}ms")

        # ── 7. Assess result quality ──────────────────────────────────────────
        if is_pagination:
            quality = "pagination"
        else:
            quality = self._assess_result_quality(scrubbed_rows)
        logger.debug(f"[ANSWER] quality={quality}")

        # ── 8. Generate NL answer ─────────────────────────────────────────────
        answer_prompt = self._prompt_builder.build_answer_prompt(
            question=question,
            rows=scrubbed_rows,
            row_count=strategy_result.row_count,
            quality=quality,
            sql_query=strategy_result.query_used,
            context=ctx,
            pagination_offset=pagination_offset,
        )

        t0 = time.perf_counter()
        answer = self.llm.generate_with_history([
            {"role": "system", "content": answer_prompt["system"]},
            {"role": "user",   "content": answer_prompt["user"]},
        ])
        answer_ms = int((time.perf_counter() - t0) * 1000)

        logger.info(
            f"[ANSWER] Generated | latency={answer_ms}ms | "
            f"answer={truncate(answer, 120)}"
        )

        # ── 9. Return ─────────────────────────────────────────────────────────
        return QueryResponse(
            question=question,
            sql=strategy_result.query_used,
            results=scrubbed_rows,
            all_results=scrubbed_rows,
            row_count=strategy_result.row_count,
            total_rows=strategy_result.row_count,
            page_size=self._settings.PAGE_SIZE,
            strategy_used=strategy_result.strategy_name,
            answer=answer,
            error=None,
        )

    def _generate_and_execute(
        self,
        question: str,
        schema_chunks: list[dict],
        context: list[dict] | None = None,
        is_pagination: bool = False,
        pagination_offset: int = 0,
    ):
        """
        LLM query generation + strategy execution with retry loop.

        KEY CHANGE: context, is_pagination, and pagination_offset are now
        passed through to build_query_prompt so the SQL generation prompt
        has full knowledge of prior conversation turns.
        """
        ctx = context or []
        attempt_history: list[AttemptRecord] = []
        last_exc: Exception | None = None
        max_attempts = self._settings.MAX_RETRIES

        for attempt in range(1, max_attempts + 1):

            logger.info(
                f"[LLM_CALL] Attempt {attempt}/{max_attempts} | "
                f"db_type={self.adapter.db_type} | "
                f"is_pagination={is_pagination}"
            )

            t0 = time.perf_counter()
            prompt = self._prompt_builder.build_query_prompt(
                question=question,
                schema_chunks=schema_chunks,
                attempt_history=attempt_history or None,
                context=ctx,                       # ← FIX: context now reaches SQL gen
                is_pagination=is_pagination,        # ← FIX: pagination uses dedicated prompt
                pagination_offset=pagination_offset,
            )
            prompt_ms = int((time.perf_counter() - t0) * 1000)

            logger.debug(
                f"[PROMPT] Built | latency={prompt_ms}ms | "
                f"is_pagination={is_pagination} | "
                f"has_retry_context={bool(attempt_history)}"
            )
            logger.debug(f"[PROMPT] User:\n{prompt['user'][:600]}")

            # LLM generates raw query text
            t0 = time.perf_counter()
            raw_query_text = self.llm.generate_with_history([
                {"role": "system", "content": prompt["system"]},
                {"role": "user",   "content": prompt["user"]},
            ])
            llm_ms = int((time.perf_counter() - t0) * 1000)

            logger.info(
                f"[LLM_CALL] Response | attempt={attempt} | "
                f"latency={llm_ms}ms | "
                f"raw_output={truncate(raw_query_text, 150)}"
            )
            logger.debug(f"[LLM_CALL] Full output:\n{raw_query_text}")

            # Parse LLM output
            try:
                t0 = time.perf_counter()
                generated_query = self._parse_query(raw_query_text)
                parse_ms = int((time.perf_counter() - t0) * 1000)
                logger.info(
                    f"[PARSE] Success | attempt={attempt} | "
                    f"query_type={type(generated_query).__name__} | "
                    f"latency={parse_ms}ms | "
                    f"parsed={truncate(str(generated_query), 150)}"
                )
            except ValueError as parse_exc:
                last_exc = parse_exc
                attempt_history.append(AttemptRecord(attempt, raw_query_text, str(parse_exc)))
                logger.warning(f"[PARSE] FAILED | attempt={attempt} | error={parse_exc}")
                if attempt == max_attempts:
                    raise MaxRetriesExceeded(str(parse_exc)) from parse_exc
                continue

            # Strategy execution
            try:
                t0 = time.perf_counter()
                strategy_result = self.strategy.execute(question, generated_query)
                exec_ms = int((time.perf_counter() - t0) * 1000)
                logger.info(
                    f"[STRATEGY] Success | "
                    f"strategy={strategy_result.strategy_name} | "
                    f"rows={strategy_result.row_count} | "
                    f"latency={exec_ms}ms"
                )
                return strategy_result

            except Exception as exec_exc:
                last_exc = exec_exc
                attempt_history.append(AttemptRecord(attempt, str(generated_query), str(exec_exc)))
                logger.warning(
                    f"[STRATEGY] FAILED | attempt={attempt} | error={exec_exc}"
                )
                if attempt == max_attempts:
                    raise MaxRetriesExceeded(str(exec_exc)) from exec_exc

        raise MaxRetriesExceeded(str(last_exc))

    # ── Parse helpers ─────────────────────────────────────────────────────────

    def _parse_query(self, raw_text: str) -> str | dict:
        db_type = self.adapter.db_type
        if db_type == "mysql":
            return self._strip_sql(raw_text)
        if db_type == "mongo":
            return self._parse_mongo_json(raw_text)
        raise ValueError(f"Unsupported db_type='{db_type}'")

    @staticmethod
    def _strip_sql(raw: str) -> str:
        sql = raw.strip()
        if sql.startswith("```"):
            lines = sql.split("\n")
            inner = lines[1:] if lines[-1].strip() == "```" else lines[1:]
            sql = "\n".join(line for line in inner if line.strip() != "```").strip()
        return sql

    @staticmethod
    def _parse_mongo_json(raw: str) -> dict:
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(
                line for line in lines[1:]
                if line.strip() not in ("```", "```json")
            ).strip()

        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError(
                f"LLM output did not contain a JSON object. Got: {repr(text[:200])}"
            )
        json_str = text[start:end + 1]

        try:
            parsed = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON from LLM: {exc}. Raw: {repr(json_str[:300])}") from exc

        if not isinstance(parsed, dict):
            raise ValueError(f"Expected dict, got {type(parsed).__name__}")
        if "collection" not in parsed:
            raise ValueError(f"MongoDB query missing 'collection' key. Keys: {list(parsed.keys())}")

        return parsed

    def _assess_result_quality(self, rows: list[dict]) -> str:
        count = len(rows)
        if count == 0:
            return "empty"
        if count <= self._settings.MAX_ROWS_FOR_LLM:
            return "small"
        return "large"

    # ── Pagination helpers ────────────────────────────────────────────────────

    def _get_previous_question(self, context: list[dict]) -> str:
        """
        Extract the most recent user question from the conversation context.
        Used as the retrieval query for pagination so FAISS gets the right schema.
        Falls back to a generic query if context is empty or malformed.
        """
        for turn in reversed(context):
            q = turn.get("question", "").strip()
            if q:
                return q
        return "data query"

    def _extract_pagination_offset(self, context: list[dict]) -> int:
        """
        Determine how many rows have already been shown to the user.

        Strategy (in order):
          1. Look for "Showing X of Y" in the most recent answer text.
             The X in "Showing 10 of 47" tells us the current offset.
          2. Fall back to PAGE_SIZE (assume first page was just shown).

        This lets pagination stack correctly:
          - Turn 1: "show customers" → 47 rows, answer says "Showing 10 of 47"
          - Turn 2: "show more" → extract 10 from answer → OFFSET 10
          - Turn 3: "show more" → extract 20 from answer → OFFSET 20
        """
        for turn in reversed(context):
            answer = turn.get("answer", "")
            # Match "Showing X of Y" or "Showing X–Y of Z" (from pagination quality instruction)
            # We want the right-most "shown so far" number
            match = re.search(
                r"Showing\s+(?:rows?\s+)?(\d+)[–\-]?(\d+)?\s+of\s+\d+",
                answer,
                re.IGNORECASE,
            )
            if match:
                # If we have a range (rows 1-10), use the end of the range
                end = match.group(2) or match.group(1)
                try:
                    offset = int(end)
                    logger.debug(
                        f"[PAGINATION] Extracted offset={offset} from answer: "
                        f"{repr(answer[:80])}"
                    )
                    return offset
                except ValueError:
                    pass

        # Fallback: assume one page has been shown
        fallback = self._settings.PAGE_SIZE
        logger.debug(
            f"[PAGINATION] Could not extract offset from context — "
            f"defaulting to PAGE_SIZE={fallback}"
        )
        return fallback