"""
rag/prompt_builder.py

Builds the prompts sent to the LLM.

KEY CHANGE FROM PREVIOUS VERSION:
    build_query_prompt() now accepts:
      - context: list[dict]  — prior conversation turns (question, sql, answer)
      - is_pagination: bool  — True when the user typed "show more" / "next"

    When context is present, the conversation history (Q + SQL) is injected
    into the SQL generation prompt so the LLM knows what it was doing before.

    When is_pagination=True, a dedicated PAGINATION_PROMPT_TEMPLATE is used
    instead of the normal SQL template. This tells the LLM EXACTLY what to do:
      "Take this SQL, add LIMIT X OFFSET Y, return ONLY the modified SQL."
    The LLM cannot hallucinate a different table because the SQL is explicit.
"""

import json
from core.interfaces import BaseDBAdapter
from core.client_config import get_client_config
from core.logging_config import get_logger, truncate

logger = get_logger(__name__)


# ── SQL generation user message template ─────────────────────────────────────
# CHANGE: added {conversation_context} section between schema and question.
# When there is no prior context, this section says "(First message — no prior turns)".

_SQL_USER_TEMPLATE = """## Static DB Context
{db_context}

## RAG-Retrieved Schema (most relevant tables for this question)
{schema_context}

## Conversation History
{conversation_context}

## Previous Generation Attempts (if any)
{attempt_history}

## Current Question
{question}""".strip()


# ── Pagination-specific prompt template ───────────────────────────────────────
# Used INSTEAD of the normal template when is_pagination=True.
# Extremely explicit: gives the LLM the exact SQL and tells it to paginate.
# No schema chunks needed — the previous SQL already knows the right table.

_PAGINATION_PROMPT_TEMPLATE = """## Task
The user wants to see the next page of results from their previous query.

## Previous SQL (the exact query that was just executed)
{previous_sql}

## Pagination Parameters
- Page size: {page_size} rows per page
- Current offset: {current_offset} rows already shown
- Next offset: {next_offset}

## Your Job
Return ONLY a modified version of the Previous SQL above with:
  LIMIT {page_size} OFFSET {next_offset}

Rules:
1. Keep EVERY other part of the SQL identical — same table, same WHERE clause, same ORDER BY
2. If the original SQL has a LIMIT clause, replace it with LIMIT {page_size} OFFSET {next_offset}
3. If the original SQL has no LIMIT clause, add LIMIT {page_size} OFFSET {next_offset} at the end
4. Do NOT change the table name, columns, filters, or ORDER BY
5. Output ONLY the SQL — no explanation, no markdown fences

## Current question from user
{question}""".strip()


# ── Mongo template (unchanged) ─────────────────────────────────────────────────

_MONGO_USER_TEMPLATE = """## RAG-Retrieved Schema (inferred from sampled documents)
{schema_context}

## Conversation History
{conversation_context}

## Previous Generation Attempts (if any)
{attempt_history}

## Current Question
{question}""".strip()


# ── Answer generation template (unchanged) ────────────────────────────────────

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

_QUALITY_INSTRUCTIONS: dict[str, str] = {
    "small": (
        "Write a clear, concise natural language answer based on the data above. "
        "Be specific — mention actual values from the results. "
        "Format the data as a markdown table if there are 3+ columns or 3+ rows."
    ),
    "large": (
        "The query returned {total_rows} rows total. "
        "Show the first {page_size} rows as a markdown table. "
        "End with exactly this line: "
        "'_Showing {page_size} of {total_rows} results. "
        "Say **show more** to see the next {page_size}._'"
    ),
    "pagination": (
        "The user asked for the next page of results. "
        "These are rows {offset_start}–{offset_end} of {total_rows} total. "
        "Present them as a markdown table. "
        "If there are more rows, end with: "
        "'_Showing rows {offset_start}–{offset_end} of {total_rows}. "
        "Say **show more** for the next page._' "
        "If this is the last page, say: '_You have seen all {total_rows} results._'"
    ),
    "empty": (
        "No results were returned. Tell the user clearly that nothing was found. "
        "Suggest likely reasons and offer a concrete rephrased question to try."
    ),
    "all_null": (
        "The query returned rows but all values appear empty or null. "
        "Tell the user the data appears to be missing or not yet populated."
    ),
    "low_relevance": (
        "These results may not answer the question. Be honest about what was "
        "returned and suggest how to rephrase for a better result."
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
            pagination_offset: How many rows have already been shown (for OFFSET calc).

        Returns {"system": ..., "user": ...}
        """
        cfg = get_client_config()
        from core.config.settings import get_settings
        settings = get_settings()

        # ── Pagination path ───────────────────────────────────────────────────
        # Completely different prompt — explicit SQL + LIMIT/OFFSET instruction.
        # The LLM cannot hallucinate the wrong table because we give it the SQL.
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
                logger.warning(
                    "[PROMPT] Pagination requested but no previous SQL found in context — "
                    "falling through to normal prompt"
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
                db_context=cfg.db_context_markdown,
                schema_context=schema_context,
                conversation_context=conversation_context,
                attempt_history=history_text,
                question=question,
            )
            return {"system": cfg.sql_system_prompt, "user": user_content}

        # MongoDB
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
            rows:              Result rows from the DB (already sliced to MAX_ROWS_FOR_LLM).
            row_count:         Total rows returned.
            quality:           'small' | 'large' | 'pagination' | 'empty' | 'all_null' | 'low_relevance'
            sql_query:         The actual SQL/Mongo query that ran.
            context:           Prior conversation turns.
            pagination_offset: How many rows were already shown (for pagination quality text).
        """
        from core.config.settings import get_settings
        cfg = get_client_config()
        max_for_llm = get_settings().MAX_ROWS_FOR_LLM
        page_size = get_settings().PAGE_SIZE

        preview_rows = rows[:max_for_llm]

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
                f"\n\n(Showing {max_for_llm} of {row_count} total rows.)"
            )

        # Format quality instruction with pagination numbers where needed
        _raw_instruction = _QUALITY_INSTRUCTIONS.get(quality, _QUALITY_INSTRUCTIONS["small"])

        if "{total_rows}" in _raw_instruction:
            quality_instruction = _raw_instruction.format(
                total_rows=row_count,
                page_size=page_size,
                offset_start=pagination_offset + 1,
                offset_end=pagination_offset + len(preview_rows),
            )
        else:
            quality_instruction = _raw_instruction

        # Sliding window conversation context
        context_text = ""
        if context:
            lines = ["Previous conversation turns (for context only — do not repeat these answers):"]
            for i, turn in enumerate(context[-5:], 1):
                lines.append(f"  Turn {i}:")
                lines.append(f"    Q: {turn.get('question', '')}")
                lines.append(f"    SQL: {turn.get('sql', '')}")
                lines.append(f"    A: {turn.get('answer', '')}")
            context_text = "\n".join(lines)
        else:
            context_text = "(No prior conversation turns)"

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
            f"context_turns={len(context) if context else 0}"
        )

        return {"system": cfg.answer_system_prompt, "user": user_content}

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _format_history(self, history: list) -> str:
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
        Format prior conversation turns for injection into the SQL generation prompt.

        Includes BOTH the question AND the SQL so the LLM understands what it
        was querying before. This is critical for follow-up queries to work correctly.
        The answer is intentionally excluded — it's verbose and irrelevant to SQL gen.
        """
        if not context:
            return "(First message — no prior conversation turns)"

        lines = ["The user has been asking questions in this session. Here are the prior turns:"]
        for i, turn in enumerate(context[-5:], 1):
            q = turn.get("question", "").strip()
            sql = turn.get("sql", "").strip()
            if q:
                lines.append(f"  Turn {i}: User asked: \"{q}\"")
            if sql:
                lines.append(f"           SQL executed: {sql}")

        lines.append("")
        lines.append("Use this context to understand what the user is referring to.")
        lines.append("If the current question is a follow-up, build on the previous SQL.")

        return "\n".join(lines)

    def _get_last_sql(self, context: list[dict]) -> str | None:
        """
        Extract the most recent non-empty SQL from the conversation context.

        Iterates from newest to oldest to find the last query that actually ran.
        Returns None if no SQL is found (caller falls back to normal prompt).
        """
        for turn in reversed(context):
            sql = turn.get("sql", "").strip()
            if sql:
                return sql
        return None