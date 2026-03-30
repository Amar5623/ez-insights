"""
rag/prompt_builder.py

Builds the prompts sent to the LLM for SQL generation and answer generation.

KEY DESIGN DECISIONS:

1. db_context_markdown is NOT injected into the SQL generation user message.
   It lives in sql_system.md (the system prompt) as the BUSINESS CONTEXT section.
   Injecting it again in the user message caused the LLM to see the same domain
   facts (revenue formula, join rules, valid enum values) 2-3 times per call,
   which wastes tokens and can cause the model to oscillate between phrasings.

2. Pagination uses a completely separate, minimal prompt (_PAGINATION_PROMPT_TEMPLATE).
   The previous SQL is pulled from context and handed directly to the LLM.
   The OFFSET is a plain integer (pagination_offset) passed in from the request —
   no regex scraping of answer text needed.
   The LLM's only job is: "take this SQL, replace LIMIT/OFFSET with these numbers."

3. Special signals (__OUT_OF_SCOPE__, __PRIVACY_BLOCK__, __CLARIFY__) are detected
   in query_service before _parse_query() is called, so they never reach the DB.

4. Retry context (attempt_history) is injected into the user message so the LLM
   can see exactly what it tried and what error it got. The schema chunks are also
   present in the retry prompt so the LLM can look up the correct column name.
"""

import json
from core.interfaces import BaseDBAdapter
from core.client_config import get_client_config
from core.logging_config import get_logger, truncate

logger = get_logger(__name__)


# ── SQL generation user message template ──────────────────────────────────────
# NOTE: db_context is intentionally absent here.
# It lives in sql_system.md (system prompt) as the BUSINESS CONTEXT section.
# Adding it here as well caused triple-duplication when combined with the
# enriched FAISS chunks which also contain domain knowledge.

_SQL_USER_TEMPLATE = """## RAG-Retrieved Schema (most relevant tables for this question)
{schema_context}

## Conversation History
{conversation_context}

## Previous Generation Attempts (if any)
{attempt_history}

## Current Question
{question}""".strip()


# ── Pagination-specific prompt ────────────────────────────────────────────────
# Used INSTEAD of the normal template when is_pagination=True.
# Gives the LLM the exact previous SQL and tells it precisely what to change.
# The LLM cannot hallucinate the wrong table — the SQL already specifies it.
# The offset is a plain integer from the frontend — no inference needed.

_PAGINATION_PROMPT_TEMPLATE = """## Task
The user wants to see the next page of results from their previous query.

## Previous SQL (the exact query that was just executed)
{previous_sql}

## Pagination Parameters
- Rows per page: {page_size}
- Rows already shown to the user: {current_offset}
- Next batch starts at row: {next_offset}

## Your Job
Rewrite ONLY the LIMIT and OFFSET in the SQL above.
Output the complete rewritten SQL and nothing else.

Rules:
1. Keep every other part of the SQL identical — same tables, same WHERE, same ORDER BY, same columns.
2. If the SQL already has a LIMIT clause, replace it with: LIMIT {page_size} OFFSET {next_offset}
3. If the SQL has no LIMIT clause, append: LIMIT {page_size} OFFSET {next_offset}
4. Do NOT add, remove, or change any other clause.
5. Output only the SQL — no explanation, no markdown fences, no comments.

## User's message
{question}""".strip()


# ── MongoDB user message template ─────────────────────────────────────────────

_MONGO_USER_TEMPLATE = """## RAG-Retrieved Schema (inferred from sampled documents)
{schema_context}

## Conversation History
{conversation_context}

## Previous Generation Attempts (if any)
{attempt_history}

## Current Question
{question}""".strip()


# ── Answer generation user message template ───────────────────────────────────

_ANSWER_USER_TEMPLATE = """## Conversation Context
{context}

## User Question
{question}

## Query Executed
{sql_query}

## Results ({row_count} row(s) returned)
{results_preview}

## Response Instruction
{quality_instruction}""".strip()


# ── Quality instructions ──────────────────────────────────────────────────────
# These tell the answer LLM exactly how to format its response based on
# how many rows came back and whether this is a pagination turn.

_QUALITY_INSTRUCTIONS: dict[str, str] = {
    "small": (
        "Write a clear, concise natural language answer based on the data above. "
        "Be specific — mention actual values from the results. "
        "Format the data as a markdown table if there are 3+ columns or 3+ rows."
    ),
    "large": (
        "The query returned {total_rows} rows total. "
        "Show the first {page_size} rows as a markdown table. "
        "End with exactly this line (fill in the numbers): "
        "'_Showing 1–{page_size} of {total_rows} results. "
        "Say **show more** to see the next {page_size}._'"
    ),
    "pagination": (
        "The user asked for the next page of results. "
        "These are rows {offset_start}–{offset_end} of {total_rows} total. "
        "Present them as a markdown table. "
        "If there are still more rows after this page, end with exactly: "
        "'_Showing rows {offset_start}–{offset_end} of {total_rows}. "
        "Say **show more** for the next page._' "
        "If this is the last page (offset_end >= total_rows), end with: "
        "'_You have now seen all {total_rows} results._'"
    ),
    "empty": (
        "No results were returned. Tell the user clearly that nothing was found. "
        "Suggest likely reasons and offer a concrete rephrased question they could try."
    ),
    "all_null": (
        "The query returned rows but all values appear empty or null. "
        "Tell the user the data appears to be missing or not yet populated."
    ),
    "low_relevance": (
        "These results may not fully answer the question. Be honest about what "
        "was returned and suggest how to rephrase for a better result."
    ),
}


# ── PromptBuilder ─────────────────────────────────────────────────────────────

class PromptBuilder:

    def __init__(self, adapter: BaseDBAdapter):
        self.adapter = adapter
        cfg = get_client_config()
        logger.info(
            f"[PROMPT] PromptBuilder initialized | "
            f"client='{cfg.company_name}' | db_type={adapter.db_type}"
        )

    # ── SQL / Query generation ─────────────────────────────────────────────────

    def build_query_prompt(
        self,
        question: str,
        schema_chunks: list[dict],
        attempt_history: list = None,
        context: list[dict] | None = None,
        is_pagination: bool = False,
        pagination_offset: int = 0,
    ) -> dict[str, str]:
        """
        Build the SQL or Mongo query generation prompt.

        Args:
            question:          The user's natural language question.
            schema_chunks:     Top-K schema chunks from SchemaRetriever.retrieve().
            attempt_history:   List of AttemptRecord from prior failed attempts.
            context:           Prior conversation turns: [{"question", "sql", "answer"}, ...]
            is_pagination:     True when the user typed "show more" / "next".
            pagination_offset: How many rows the user has already seen (used as OFFSET).
                               This is a plain integer from the frontend — never inferred.

        Returns {"system": ..., "user": ...}
        """
        cfg = get_client_config()
        from core.config.settings import get_settings
        settings = get_settings()

        # ── Pagination path ───────────────────────────────────────────────────
        # Completely different prompt — explicit SQL + LIMIT/OFFSET.
        # Schema chunks not needed: the SQL already encodes the right tables.
        if is_pagination and context:
            previous_sql = self._get_last_sql(context)
            if previous_sql:
                page_size = settings.PAGE_SIZE
                user_content = _PAGINATION_PROMPT_TEMPLATE.format(
                    previous_sql=previous_sql,
                    page_size=page_size,
                    current_offset=pagination_offset,
                    next_offset=pagination_offset + page_size,
                    question=question,
                )
                logger.info(
                    f"[PROMPT] Built PAGINATION prompt | "
                    f"previous_sql={truncate(previous_sql, 80)} | "
                    f"offset={pagination_offset} → {pagination_offset + page_size}"
                )
                return {
                    "system": cfg.sql_system_prompt,
                    "user": user_content,
                }
            else:
                # No previous SQL found — fall through to normal prompt.
                # This can happen if the user's first message is "show more".
                logger.warning(
                    "[PROMPT] Pagination requested but no previous SQL found in context — "
                    "falling through to normal SQL generation prompt"
                )

        # ── Normal path ───────────────────────────────────────────────────────
        schema_context = "\n\n".join(
            chunk.get("schema_text", "") for chunk in schema_chunks
            if chunk.get("schema_text")
        )
        if not schema_context:
            schema_context = "(No relevant schema chunks retrieved)"

        history_text = self._format_history(attempt_history or [])
        conversation_context = self._format_context_for_sql(context or [])

        logger.debug(
            f"[PROMPT] Building query prompt | "
            f"db_type={self.adapter.db_type} | "
            f"schema_chunks={len(schema_chunks)} | "
            f"context_turns={len(context) if context else 0} | "
            f"has_retry_history={bool(attempt_history)}"
        )

        if self.adapter.db_type == "mysql":
            user_content = _SQL_USER_TEMPLATE.format(
                schema_context=schema_context,
                conversation_context=conversation_context,
                attempt_history=history_text,
                question=question,
            )
            return {"system": cfg.sql_system_prompt, "user": user_content}

        # MongoDB path
        user_content = _MONGO_USER_TEMPLATE.format(
            schema_context=schema_context,
            conversation_context=conversation_context,
            attempt_history=history_text,
            question=question,
        )
        return {"system": cfg.sql_system_prompt, "user": user_content}

    # ── NL answer generation ──────────────────────────────────────────────────

    def build_answer_prompt(
        self,
        question: str,
        rows: list[dict],
        row_count: int,
        quality: str = "small",
        sql_query: str = "",
        context: list[dict] | None = None,
        pagination_offset: int = 0,
    ) -> dict[str, str]:
        """
        Build the natural language answer generation prompt.

        Args:
            question:          The original user question.
            rows:              Result rows from the DB (already scrubbed).
            row_count:         Total rows returned by the query.
            quality:           'small' | 'large' | 'pagination' | 'empty' | ...
            sql_query:         The SQL/Mongo query that ran (for transparency).
            context:           Prior conversation turns.
            pagination_offset: Rows already shown before this page (for pagination text).
        """
        from core.config.settings import get_settings
        cfg = get_client_config()
        settings = get_settings()
        max_for_llm = settings.MAX_ROWS_FOR_LLM
        page_size = settings.PAGE_SIZE

        # Limit rows sent to LLM — the full result set can be huge
        preview_rows = rows[:max_for_llm]

        # Strip embedding columns — they are float arrays, useless in prompts
        cleaned = [
            {k: v for k, v in row.items() if "embed" not in k.lower()}
            for row in preview_rows
        ]

        try:
            results_preview = json.dumps(cleaned, indent=2, default=str)
        except Exception:
            results_preview = "\n".join(str(row) for row in cleaned)

        if row_count > max_for_llm:
            results_preview += (
                f"\n\n(Showing {max_for_llm} of {row_count} total rows in this prompt.)"
            )

        # Resolve quality instruction, substituting numeric placeholders
        raw_instruction = _QUALITY_INSTRUCTIONS.get(quality, _QUALITY_INSTRUCTIONS["small"])

        offset_end = pagination_offset + len(preview_rows)

        if any(ph in raw_instruction for ph in ("{total_rows}", "{page_size}", "{offset_start}", "{offset_end}")):
            quality_instruction = raw_instruction.format(
                total_rows=row_count,
                page_size=page_size,
                offset_start=pagination_offset + 1,
                offset_end=offset_end,
            )
        else:
            quality_instruction = raw_instruction

        # Sliding window of recent conversation turns for answer context
        context_text = self._format_context_for_answer(context or [])

        user_content = _ANSWER_USER_TEMPLATE.format(
            context=context_text,
            question=question,
            sql_query=sql_query or "(not available)",
            row_count=row_count,
            results_preview=results_preview or "(no rows returned)",
            quality_instruction=quality_instruction,
        )

        logger.debug(
            f"[PROMPT] Built answer prompt | "
            f"quality={quality} | "
            f"rows_in_prompt={len(preview_rows)}/{row_count} | "
            f"pagination_offset={pagination_offset} | "
            f"context_turns={len(context) if context else 0}"
        )

        return {"system": cfg.answer_system_prompt, "user": user_content}

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _format_history(self, history: list) -> str:
        """Format retry attempt history for injection into the SQL generation prompt."""
        if not history:
            return "None"
        lines = []
        for attempt in history:
            lines.append(
                f"Attempt {attempt.attempt_number}:\n"
                f"  Query tried: {attempt.query_used}\n"
                f"  Error received: {attempt.error}"
            )
        return "\n\n".join(lines)

    def _format_context_for_sql(self, context: list[dict]) -> str:
        """
        Format prior turns for the SQL generation prompt.

        Includes the question AND the SQL so the LLM understands what it
        was querying. The answer is excluded — it's verbose and irrelevant
        to generating the next SQL query.
        """
        if not context:
            return "(First message — no prior conversation turns)"

        lines = [
            "The user has been asking questions in this session. "
            "Here are the prior turns:"
        ]
        for i, turn in enumerate(context[-5:], 1):
            q = turn.get("question", "").strip()
            sql = turn.get("sql", "").strip()
            if q:
                lines.append(f"  Turn {i}: User asked: \"{q}\"")
            if sql:
                lines.append(f"           SQL executed: {sql}")

        lines.append("")
        lines.append(
            "Use this context to understand what the user is referring to. "
            "If the current question is a follow-up, build on the previous SQL."
        )

        return "\n".join(lines)

    def _format_context_for_answer(self, context: list[dict]) -> str:
        """
        Format prior turns for the answer generation prompt.

        Includes Q, SQL, and a truncated answer so the LLM can reference
        prior results without repeating them.
        """
        if not context:
            return "(No prior conversation turns)"

        lines = [
            "Previous conversation turns "
            "(for context only — do not repeat these answers):"
        ]
        for i, turn in enumerate(context[-5:], 1):
            q = turn.get("question", "").strip()
            sql = turn.get("sql", "").strip()
            answer = turn.get("answer", "").strip()
            if len(answer) > 120:
                answer = answer[:120] + "..."
            lines.append(f"  Turn {i}:")
            if q:
                lines.append(f"    Q: {q}")
            if sql:
                lines.append(f"    SQL: {sql}")
            if answer:
                lines.append(f"    A: {answer}")

        return "\n".join(lines)

    def _get_last_sql(self, context: list[dict]) -> str | None:
        """
        Return the most recent non-empty SQL from conversation context.

        Iterates newest-first so the most recent query is always used.
        Returns None if no SQL has been executed yet in this session
        (caller falls back to normal SQL generation prompt).
        """
        for turn in reversed(context):
            sql = turn.get("sql", "").strip()
            if sql:
                return sql
        return None