"""
services/query_service.py

Orchestrates the full NL → Answer pipeline.

PAGINATION (how it works now):
    The frontend tracks how many rows the user has seen (displayed_count).
    On "show more", it sends displayed_count in the request body.
    query.py reads it and passes it here as displayed_count.
    This service passes it straight to prompt_builder as pagination_offset.
    prompt_builder builds: "take this SQL, add LIMIT {page_size} OFFSET {displayed_count}".

    No regex scraping. No extracting numbers from LLM answer text.
    The offset is a plain integer that the frontend owns and tracks.

FIX Bug 1 — known_total_rows:
    On pagination calls the frontend also sends back the total_rows value it
    received from the original query's done event. This is the true total (e.g.
    28), not the batch size (10). The service uses it in build_answer_prompt so
    the LLM can write an accurate footer ("Showing rows 11–20 of 28") instead of
    treating the batch count as the total and always concluding "You have seen all
    20 results."

FIX Bug 2 — show_all:
    When show_all=True, the service overrides the page_size cap with
    MAX_RESULT_ROWS so the DB returns every remaining row at once. The quality
    instruction is set to "show_all" which tells the LLM to render all rows with
    an appropriate "all results shown" footer.

SIGNAL HANDLING:
    If the LLM outputs a special signal (__OUT_OF_SCOPE__, __PRIVACY_BLOCK__,
    __CLARIFY__) instead of SQL, _parse_query raises ValueError which feeds
    the retry loop. On max retries, the raw signal text is returned as the
    answer so the user sees a meaningful message instead of a DB error.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field

from core.interfaces import BaseLLM, BaseDBAdapter, BaseStrategy
from core.config.settings import get_settings
from core.logging_config import get_logger, truncate
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


# ─── Special signal prefixes the LLM may output instead of SQL ───────────────

_SPECIAL_SIGNALS = ("__OUT_OF_SCOPE__", "__PRIVACY_BLOCK__", "__CLARIFY__")

def _extract_friendly_message(signal_text: str) -> str:
    """
    Parse the LLM's special signal output into a user-facing message.
    Handles __OUT_OF_SCOPE__, __PRIVACY_BLOCK__, and __CLARIFY__.
    """
    lines = signal_text.splitlines()
    
    # Detect signal type from first line
    first = lines[0].strip() if lines else ""
    
    suggest = ""
    reason = ""
    ambiguity = ""
    options: list[str] = []
    
    for line in lines[1:]:
        line = line.strip()
        if line.startswith("SUGGEST:"):
            suggest = line[len("SUGGEST:"):].strip()
        elif line.startswith("REASON:"):
            reason = line[len("REASON:"):].strip()
        elif line.startswith("AMBIGUITY:"):
            ambiguity = line[len("AMBIGUITY:"):].strip()
        elif line.startswith("- ") and "CLARIFY" in first:
            options.append(line[2:].strip())

    if "__CLARIFY__" in first:
        msg = f"I need a bit more clarity to answer that."
        if ambiguity:
            msg += f" {ambiguity}"
        if options:
            msg += "\n\nDid you mean:\n" + "\n".join(f"- {o}" for o in options)
        return msg

    if "__PRIVACY_BLOCK__" in first:
        base = "That information is protected and can't be retrieved."
        return f"{base} {reason}" if reason else base

    # __OUT_OF_SCOPE__ (default)
    if suggest:
        return f"I can't answer that directly. {suggest}"
    if reason:
        return f"I can't answer that: {reason}"
    return (
        "I couldn't find an answer for that in the database. "
        "Try asking something specific, like 'What are the top-selling products?' "
        "or 'What is the total revenue this month?'"
    )

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
        displayed_count: int = 0,
        known_total_rows: int = 0,
        show_all: bool = False,
    ) -> QueryResponse:
        """
        Execute the full pipeline for a natural language question.

        Args:
            question:          The user's natural language question.
            context:           Prior turns: [{"question": str, "sql": str, "answer": str}, ...]
            intent:            Pre-classified intent. Defaults to DB_QUERY.
                               Pass IntentType.PAGINATION when the user typed "show more".
            displayed_count:   Number of rows already shown to the user in the frontend.
                               Used as OFFSET for pagination. Comes from the request body.
            known_total_rows:  FIX Bug 1. The true total row count from the original
                               query, sent back by the frontend on pagination calls.
                               When non-zero this overrides the batch row_count so the
                               LLM can write an accurate "X of Y" footer.
            show_all:          FIX Bug 2. When True, the PAGE_SIZE cap is removed and
                               all remaining rows are fetched and displayed at once.
        """
        pipeline_start = time.perf_counter()
        is_pagination = (intent == IntentType.PAGINATION)

        logger.info(
            f"[PIPELINE] START | "
            f"question={truncate(question, 120)} | "
            f"context_turns={len(context) if context else 0} | "
            f"intent={intent.value} | "
            f"displayed_count={displayed_count} | "
            f"known_total_rows={known_total_rows} | "
            f"show_all={show_all}"
        )

        try:
            result = self._run_pipeline(
                question,
                context=context,
                is_pagination=is_pagination,
                pagination_offset=displayed_count,
                known_total_rows=known_total_rows,
                show_all=show_all,
            )

            total_ms = int((time.perf_counter() - pipeline_start) * 1000)
            logger.info(
                f"[PIPELINE] COMPLETE | "
                f"strategy={result.strategy_used} | "
                f"rows={result.row_count} | "
                f"total_rows={result.total_rows} | "
                f"total_latency={total_ms}ms"
            )
            return result

        except MaxRetriesExceeded as e:
            total_ms = int((time.perf_counter() - pipeline_start) * 1000)
            logger.warning(
                f"[PIPELINE] FAILED — MaxRetriesExceeded | "
                f"total_latency={total_ms}ms | error={e}"
            )
            # Extract a friendly user-facing message from the signal if present.
            # The LLM emits: __OUT_OF_SCOPE__\nREASON: ...\nSUGGEST: <message>
            friendly_answer = _extract_friendly_message(str(e))
            return QueryResponse(
                question=question,
                sql="",
                results=[],
                row_count=0,
                strategy_used=self.strategy.strategy_name,
                answer=friendly_answer,
                error=None,   # Don't surface a raw error — we have a friendly answer
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
        pagination_offset: int = 0,
        known_total_rows: int = 0,
        show_all: bool = False,
    ) -> QueryResponse:

        ctx = context or []

        # ── 1. Schema retrieval ───────────────────────────────────────────────
        # PAGINATION / SHOW_ALL: Use the PREVIOUS question for schema retrieval,
        # not "show more" / "show all". Those phrases have no semantic relation
        # to any table so FAISS would return random chunks.
        if is_pagination and ctx:
            retrieval_question = self._get_previous_question(ctx)
            logger.info(
                f"[SCHEMA_RAG] Pagination — using previous question for retrieval | "
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

        # FIX Bug 2: when show_all, override page_size to the max cap so the
        # pagination prompt generates "LIMIT {max} OFFSET {offset}" instead of
        # "LIMIT {page_size} OFFSET {offset}".
        effective_page_size = (
            self._settings.MAX_RESULT_ROWS if show_all else self._settings.PAGE_SIZE
        )

        if is_pagination:
            logger.info(
                f"[PAGINATION] offset={pagination_offset} | "
                f"effective_page_size={effective_page_size} | "
                f"show_all={show_all}"
            )

        # ── 2. Generate query + execute ───────────────────────────────────────
        strategy_result = self._generate_and_execute(
            question=question,
            schema_chunks=schema_chunks,
            context=ctx,
            is_pagination=is_pagination,
            pagination_offset=pagination_offset,
            effective_page_size=effective_page_size,
        )

        # ── 3. Scrub sensitive data ───────────────────────────────────────────
        t0 = time.perf_counter()
        scrubbed_rows = scrub_rows(strategy_result.rows)
        scrub_ms = int((time.perf_counter() - t0) * 1000)
        logger.debug(f"[SCRUB] completed | latency={scrub_ms}ms")

        # ── 4. Resolve the true total row count ───────────────────────────────
        # FIX Bug 1:
        #   - Fresh query (is_pagination=False): the DB returns all matching rows,
        #     so strategy_result.row_count IS the true total.
        #   - Pagination (is_pagination=True): the DB returns only the current
        #     batch (e.g. 10 rows). The actual total (e.g. 28) was captured on
        #     the original query's done event and sent back by the frontend as
        #     known_total_rows. Use that; fall back to the batch count only if
        #     the frontend didn't send it (e.g. first-ever call to this code path).
        batch_row_count = strategy_result.row_count
        if is_pagination and known_total_rows > 0:
            true_total = known_total_rows
        else:
            true_total = batch_row_count

        logger.debug(
            f"[TOTAL] batch={batch_row_count} | known_total={known_total_rows} | "
            f"resolved_total={true_total} | is_pagination={is_pagination}"
        )

        # ── 5. Assess result quality ──────────────────────────────────────────
        if show_all and is_pagination:
            # All remaining rows at once — use dedicated quality instruction.
            quality = "show_all"
        elif is_pagination:
            quality = "pagination"
        else:
            quality = self._assess_result_quality(scrubbed_rows)
        logger.debug(f"[ANSWER] quality={quality}")

        # ── 6. Generate NL answer ─────────────────────────────────────────────
        answer_prompt = self._prompt_builder.build_answer_prompt(
            question=question,
            rows=scrubbed_rows,
            row_count=true_total,          # FIX Bug 1: pass true total
            batch_row_count=batch_row_count,
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

        # ── 7. Return ─────────────────────────────────────────────────────────
        return QueryResponse(
            question=question,
            sql=strategy_result.query_used,
            results=scrubbed_rows,
            all_results=scrubbed_rows,
            row_count=batch_row_count,
            total_rows=true_total,         # FIX Bug 1: expose true total upstream
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
        effective_page_size: int | None = None,
    ):
        """
        LLM query generation + strategy execution with retry loop.

        On each attempt:
          1. Build the prompt (pagination-specific or normal).
          2. Call the LLM.
          3. Check for special signals (__OUT_OF_SCOPE__ etc.) before parsing.
          4. Parse the output into SQL or Mongo dict.
          5. Run the strategy.
          6. On failure, record the attempt and retry with error context in prompt.
        """
        ctx = context or []
        attempt_history: list[AttemptRecord] = []
        last_exc: Exception | None = None
        max_attempts = self._settings.MAX_RETRIES
        page_size = effective_page_size if effective_page_size is not None else self._settings.PAGE_SIZE

        for attempt in range(1, max_attempts + 1):

            logger.info(
                f"[LLM_CALL] Attempt {attempt}/{max_attempts} | "
                f"db_type={self.adapter.db_type} | "
                f"is_pagination={is_pagination} | "
                f"effective_page_size={page_size}"
            )

            t0 = time.perf_counter()
            prompt = self._prompt_builder.build_query_prompt(
                question=question,
                schema_chunks=schema_chunks,
                attempt_history=attempt_history or None,
                context=ctx,
                is_pagination=is_pagination,
                pagination_offset=pagination_offset,
                effective_page_size=page_size,
            )

            try:
                raw_output = self.llm.generate_with_history([
                    {"role": "system", "content": prompt["system"]},
                    {"role": "user",   "content": prompt["user"]},
                ])
                llm_ms = int((time.perf_counter() - t0) * 1000)
                logger.info(
                    f"[LLM_CALL] Output received | "
                    f"latency={llm_ms}ms | "
                    f"output={truncate(raw_output, 800)}"
                )
            except Exception as exc:
                logger.warning(f"[LLM_CALL] LLM error on attempt {attempt}: {exc}")
                last_exc = exc
                attempt_history.append(AttemptRecord(
                    attempt_number=attempt,
                    query_used="(LLM call failed)",
                    error=str(exc),
                ))
                continue

            # Check for special signals before trying to parse as SQL
            stripped = raw_output.strip()
            for signal in _SPECIAL_SIGNALS:
                if stripped.startswith(signal):
                    logger.info(f"[LLM_CALL] Special signal detected: {signal}")
                    attempt_history.append(AttemptRecord(
                        attempt_number=attempt,
                        query_used=stripped,
                        error=f"Signal: {signal}",
                    ))
                    last_exc = ValueError(stripped)
                    break
            else:
                # No signal — try to parse and execute
                try:
                    parsed_query = self._parse_query(raw_output)
                    logger.info(f"[PARSED_SQL]\n{parsed_query}")
                    logger.info(f"[PARSED_TYPE] {type(parsed_query)}")
                    t0 = time.perf_counter()
                    print("====== RAW OUTPUT ======")
                    print(raw_output)
                    print("====== PARSED QUERY ======")
                    print(parsed_query)
                    print("====== TYPE ======")
                    print(type(parsed_query))
                    strategy_result = self.strategy.execute(question, parsed_query)
                    exec_ms = int((time.perf_counter() - t0) * 1000)
                    logger.info(
                        f"[STRATEGY] Success on attempt {attempt} | "
                        f"rows={strategy_result.row_count} | "
                        f"latency={exec_ms}ms"
                    )
                    return strategy_result
                except Exception as exc:
                    logger.warning(
                        f"[STRATEGY] Failed on attempt {attempt}: {exc} | "
                        f"query={truncate(raw_output, 80)}"
                    )
                    last_exc = exc
                    attempt_history.append(AttemptRecord(
                        attempt_number=attempt,
                        query_used=raw_output,
                        error=str(exc),
                    ))
                    continue
                
        # All attempts exhausted
        last_signal = str(last_exc) if last_exc else "unknown error"
        # If the last failure was a special signal, return it as the answer
        for signal in _SPECIAL_SIGNALS:
            if last_signal.startswith(signal):
                raise MaxRetriesExceeded(last_signal)
        raise MaxRetriesExceeded(
            f"All {max_attempts} attempts failed. Last error: {last_signal}"
        )

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

        # Remove markdown fences if present
        if sql.startswith("```"):
            lines = sql.split("\n")

            # Remove first line (```sql or ```)
            lines = lines[1:]

            # Remove last line if it's ```
            if lines and lines[-1].strip().startswith("```"):
                lines = lines[:-1]

            sql = "\n".join(lines).strip()

        return sql

    @staticmethod
    def _parse_mongo_json(raw: str) -> dict:
        text = raw.strip()
        if text.startswith(""):
            lines = text.split("\n")
            text = "\n".join(
                line for line in lines[1:]
                if line.strip() not in ("", "json")
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
            raise ValueError(
                f"Invalid JSON from LLM: {exc}. Raw: {repr(json_str[:300])}"
            ) from exc

        if not isinstance(parsed, dict):
            raise ValueError(f"Expected dict, got {type(parsed)._name_}")
        if "collection" not in parsed:
            raise ValueError(
                f"MongoDB query missing 'collection' key. Keys: {list(parsed.keys())}"
            )

        return parsed

    def _get_previous_question(self, context: list[dict]) -> str:
        """Return the most recent user question from context."""
        for turn in reversed(context):
            q = turn.get("question", "").strip()
            if q:
                return q
        return ""

    def _get_last_sql(self, context: list[dict]) -> str:
        """Return the most recent SQL from context."""
        for turn in reversed(context):
            sql = turn.get("sql", "").strip()
            if sql:
                return sql
        return ""

    def _assess_result_quality(self, rows: list[dict]) -> str:
        """
        Assess result quality for fresh (non-pagination) queries.

        Returns a quality label understood by prompt_builder:
          'empty'        — no rows
          'all_null'     — rows present but all values null/empty
          'low_relevance'— rows seem unrelated (future heuristic)
          'small'        — rows fit within one page
          'large'        — rows exceed one page
        """
        if not rows:
            return "empty"

        # Check if all values are null/empty
        all_null = all(
            v is None or v == "" or v == []
            for row in rows
            for v in row.values()
        )
        if all_null:
            return "all_null"

        if len(rows) > self._settings.PAGE_SIZE:
            return "large"

        return "small"