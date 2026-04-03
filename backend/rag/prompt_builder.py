"""
rag/prompt_builder.py

Builds the prompts sent to the LLM for SQL generation and answer generation.

KEY DESIGN DECISIONS:

1. db_context_markdown is NOT injected into the SQL generation user message.
   It lives in sql_system.md (the system prompt) as the BUSINESS CONTEXT section.

2. Pagination uses a completely separate, minimal prompt (_PAGINATION_PROMPT_TEMPLATE).
   The previous SQL is pulled from context and handed directly to the LLM.
   The OFFSET is a plain integer (pagination_offset) passed in from the request —
   no regex scraping of answer text needed.

3. Special signals (__OUT_OF_SCOPE__, __PRIVACY_BLOCK__, __CLARIFY__) are detected
   in query_service before _parse_query() is called, so they never reach the DB.

4. Retry context (attempt_history) is injected into the user message so the LLM
   can see exactly what it tried and what error it got.

FIX Bug 1 — row_count vs batch_row_count:
   build_answer_prompt now accepts two separate row counts:
     row_count        — the TRUE total (e.g. 28). Used in footer text and quality
                        instructions so the LLM always says "X of 28".
     batch_row_count  — how many rows are actually in the prompt (e.g. 10).
                        Used to cap the preview sent to the LLM and to compute
                        offset_end accurately.

FIX Bug 2 — show_all quality instruction:
   A new "show_all" quality label is handled. It tells the LLM to render all
   rows in the prompt with no "show more" footer and a clean "all results shown"
   conclusion. The pagination prompt also accepts effective_page_size so that
   "show all" generates LIMIT {MAX_RESULT_ROWS} OFFSET {offset} instead of
   LIMIT {PAGE_SIZE} OFFSET {offset}.

ANSWER FORMAT CONTRACT (enforced via _QUALITY_INSTRUCTIONS):
   Every non-empty DB response must follow this exact 3-part structure:
     1. One sentence introducing the result
     2. A markdown table
     3. A footer line showing pagination state (or "all shown" if complete)
"""

import json
from core.interfaces import BaseDBAdapter
from core.client_config import get_client_config
from core.logging_config import get_logger, truncate

logger = get_logger(__name__)


# ── SQL generation user message template ──────────────────────────────────────

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
# FIX Bug 2: effective_page_size replaces the hard-coded page_size so "show all"
# can pass MAX_RESULT_ROWS here, generating a LIMIT with no practical cap.

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

# Add this near the other templates at the top of prompt_builder.py

_MONGO_PAGINATION_PROMPT_TEMPLATE = """## Task
The user wants to see the next page of results from their previous query.

## Previous Mongo Query (the exact query that was just executed)
{previous_sql}

## Pagination Parameters
- Rows per page: {page_size}
- Rows already shown to the user: {current_offset}
- Next batch starts at row: {next_offset}

## Your Job
Return the SAME query with updated "limit" and "skip" values only.

Rules:
    1. Keep "collection" and "pipeline" (or "filter") identical.
    2. Set "limit" to {page_size}.
    3. Set "skip" to {next_offset}.
    4. Output ONLY raw JSON — no markdown fences, no explanation.

    ## User's message
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
# IMPORTANT PLACEHOLDERS:
#   {total_rows}    — TRUE total rows in the DB for this query (e.g. 28)
#   {batch_rows}    — rows actually in this prompt / being shown now (e.g. 10)
#   {page_size}     — rows per page from settings
#   {offset_start}  — first row number shown in this batch (1-based)
#   {offset_end}    — last row number shown in this batch

_QUALITY_INSTRUCTIONS: dict[str, str] = {

    # ── All results fit in one page ──────────────────────────────────────────
    "small": (
        "Structure your response in exactly two parts:\n"
        "1. ONE sentence introducing what the data shows "
        "(e.g. 'Here are the customer details you requested:' "
        "or 'I found {total_rows} matching record(s):'). "
        "Do not repeat the user's question verbatim.\n"
        "2. Present ALL {total_rows} rows as a markdown table. "
        "If there is only one row or the result is a single value, "
        "use a short natural-language sentence instead of a table.\n"
        "Do NOT add any footer or 'show more' line — "
        "all {total_rows} result(s) are shown above."
    ),

    # ── More rows than one page ──────────────────────────────────────────────
    "large": (
        "Structure your response in exactly three parts:\n"
        "1. ONE sentence introducing what the data shows "
        "(e.g. 'Here are the first {page_size} of {total_rows} results:' "
        "or 'Here are the top {page_size} customers by revenue:'). "
        "Do not repeat the user's question verbatim.\n"
        "2. A markdown table containing EXACTLY the first {page_size} rows "
        "from the results above — no more, no fewer. "
        "Do NOT include rows beyond position {page_size}.\n"
        "3. End your response with EXACTLY this line and nothing after it "
        "(substitute the numbers, keep the italics and bold):\n"
        "Showing {page_size} of {total_rows} total rows. "
        "Say **show more** to see the next {page_size}."
    ),

    # ── User asked for the next page ─────────────────────────────────────────
    # FIX Bug 1: offset_end is computed from batch_rows (not total_rows) so it
    # correctly says "rows 11–20" not "rows 11–28".
    "pagination": (
        "You are formatting paginated query results.\n\n"

        "Determine if this is the last page:\n"
        "- Last page if {raw_offset_end} >= {total_rows}\n"
        "- Otherwise, more rows remain\n\n"

        "IMPORTANT EDGE CASE:\n"
        "- The last page may contain fewer rows than the page size\n"
        "- Even if only a partial set of rows is shown (e.g., 6 of 10), it is STILL the last page if {raw_offset_end} >= {total_rows}\n\n"

        "Structure the response in EXACTLY three parts:\n\n"

        "1. ONE sentence confirming which rows are shown:\n"
        "'Here are rows {offset_start}–{offset_end} of {total_rows}:'\n"
        "- Do NOT repeat or restate the user's question\n\n"

        "2. Present the rows as a markdown table\n\n"

        "3. Closing line:\n"
        "- If MORE rows remain ({raw_offset_end} < {total_rows}), end with EXACTLY:\n"
        "Showing rows {offset_start}–{offset_end} of {total_rows}. Say **show more** for the next page.\n\n"

        "- If this is the LAST page ({raw_offset_end} >= {total_rows}), end with EXACTLY:\n"
        "You have now seen all {total_rows} results. How else can I help you?\n\n"

        "STRICT RULES:\n"
        "- Never mix both endings\n"
        "- Never guess row counts\n"
        "- Never produce extra explanation\n"
        "- Follow formatting EXACTLY"
    ),

    

    # FIX Bug 2 — user said "show all remaining" ─────────────────────────────
    # All unseen rows are in the prompt at once. The LLM shows them all and
    # closes with a clean "all X results shown" statement.
    "show_all": (
        "Structure your response in exactly two parts:\n"
        "1. ONE sentence confirming the user is now seeing all remaining results "
        "(e.g. 'Here are the remaining {batch_rows} results, "
        "completing all {total_rows} total:'). "
        "Do not repeat the user's question verbatim.\n"
        "2. Present ALL {batch_rows} rows as a markdown table.\n"
        "End with EXACTLY this line and nothing after it:\n"
        "You have now seen all {total_rows} results."
    ),

    # ── No rows returned ─────────────────────────────────────────────────────
    "empty": (
        "Tell the user in one sentence that no results were found. "
        "Then in a second sentence suggest the most likely reason "
        "(wrong filter, no data for that period, typo in a name, etc.) "
        "and offer one concrete rephrased question they could try."
    ),

    # ── Rows returned but all values are null/empty ──────────────────────────
    "all_null": (
        "The query returned rows but all values appear empty or null. "
        "Tell the user the data appears to be missing or not yet populated."
    ),

    # ── Results may not answer the question well ─────────────────────────────
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
        effective_page_size: int | None = None,
    ) -> dict[str, str]:
        """
        Build the SQL or Mongo query generation prompt.

        Args:
            question:            The user's natural language question.
            schema_chunks:       Top-K schema chunks from SchemaRetriever.retrieve().
            attempt_history:     List of AttemptRecord from prior failed attempts.
            context:             Prior conversation turns.
            is_pagination:       True when the user typed "show more" / "show all".
            pagination_offset:   How many rows the user has already seen (OFFSET).
            effective_page_size: FIX Bug 2. Overrides PAGE_SIZE for the LIMIT
                                 clause. Pass MAX_RESULT_ROWS for show_all requests.

        Returns {"system": ..., "user": ...}
        """
        cfg = get_client_config()
        from core.config.settings import get_settings
        settings = get_settings()
        page_size = effective_page_size if effective_page_size is not None else settings.PAGE_SIZE

        # ── Pagination path ───────────────────────────────────────────────────
        if is_pagination and context:
            previous_sql = self._get_last_sql(context)
            if previous_sql:
                if self.adapter.db_type == "mongo":
                    template = _MONGO_PAGINATION_PROMPT_TEMPLATE
                else:
                    template = _PAGINATION_PROMPT_TEMPLATE

                user_content = template.format(
                        previous_sql=previous_sql,
                        page_size=page_size,
                        current_offset=pagination_offset,
                        next_offset=pagination_offset,
                        question=question,
                )
                logger.info(
                    f"[PROMPT] Built PAGINATION prompt | "
                    f"previous_sql={truncate(previous_sql, 80)} | "
                    f"offset={pagination_offset} | "
                    f"effective_page_size={page_size}"
                )
                return {
                    "system": cfg.sql_system_prompt,
                    "user": user_content,
                }
            else:
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
        batch_row_count: int | None = None,
    ) -> dict[str, str]:
        """
        Build the natural language answer generation prompt.

        Args:
            question:         The original user question.
            rows:             Result rows from the DB (already scrubbed).
            row_count:        FIX Bug 1. TRUE total rows for this query (e.g. 28).
                              Used in footer text so the LLM always says "X of 28".
            quality:          'small' | 'large' | 'pagination' | 'show_all' | 'empty' | ...
            sql_query:        The SQL/Mongo query that ran (for transparency).
            context:          Prior conversation turns.
            pagination_offset: Rows already shown before this page (for pagination text).
            batch_row_count:  FIX Bug 1. Number of rows actually in `rows` (the
                              current batch, e.g. 10). When None, defaults to
                              len(rows). Used to compute offset_end and to cap the
                              preview sent to the LLM.
        """
        from core.config.settings import get_settings
        cfg = get_client_config()
        settings = get_settings()
        max_for_llm = settings.MAX_ROWS_FOR_LLM
        page_size = settings.PAGE_SIZE

        # batch_rows is how many rows are actually in this batch/prompt.
        # row_count (true_total) is the grand total across all pages.
        batch_rows = batch_row_count if batch_row_count is not None else len(rows)
        true_total = row_count  # renamed for clarity in this scope

        # Cap rows sent to LLM. For "large" quality, only send PAGE_SIZE rows so
        # the LLM can't accidentally include extra rows in the table.
        if quality == "large":
            preview_rows = rows[:page_size]
        else:
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

        if true_total > len(preview_rows):
            results_preview += (
                f"\n\n(Showing {len(preview_rows)} of {true_total} total rows in this prompt.)"
            )

        # Resolve quality instruction, substituting numeric placeholders.
        # FIX Bug 1: offset_end uses batch_rows (not true_total) so the range
        # reads "rows 11–20", not "rows 11–28".
        raw_instruction = _QUALITY_INSTRUCTIONS.get(quality, _QUALITY_INSTRUCTIONS["small"])

        raw_offset_end = pagination_offset + batch_rows
        offset_end = min(raw_offset_end, true_total) if true_total > 0 else raw_offset_end

        if any(
            ph in raw_instruction
            for ph in ("{total_rows}", "{page_size}", "{offset_start}", "{offset_end}", "{raw_offset_end}", "{batch_rows}")
        ):
            quality_instruction = raw_instruction.format(
                total_rows=true_total,
                batch_rows=batch_rows,
                page_size=page_size,
                offset_start=pagination_offset + 1,
                offset_end=offset_end,           # capped — for display text only
                raw_offset_end=raw_offset_end,   # uncapped — for last-page comparison
            )
        else:
            quality_instruction = raw_instruction

        # Sliding window of recent conversation turns for answer context
        context_text = self._format_context_for_answer(context or [])

        user_content = _ANSWER_USER_TEMPLATE.format(
            context=context_text,
            question=question,
            sql_query=sql_query or "(not available)",
            row_count=true_total,
            results_preview=results_preview or "(no rows returned)",
            quality_instruction=quality_instruction,
        )

        logger.debug(
            f"[PROMPT] Built answer prompt | "
            f"quality={quality} | "
            f"batch_rows={batch_rows} | "
            f"true_total={true_total} | "
            f"offset={pagination_offset}→{offset_end} | "
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
        previously queried.
        """
        if not context:
            return "None"
        lines = []
        for turn in context[-5:]:  # last 4 turns
            q = turn.get("question", "").strip()
            sql = turn.get("sql", "").strip()
            if q:
                lines.append(f"User: {q}")
            if sql:
                lines.append(f"SQL: {sql}")
        return "\n".join(lines) if lines else "None"

    def _format_context_for_answer(self, context: list[dict]) -> str:
        """Format prior turns for the answer generation prompt."""
        if not context:
            return "None"
        lines = []
        for turn in context[-4:]:  # last 4 turns
            q = turn.get("question", "").strip()
            a = turn.get("answer", "").strip()
            if q:
                lines.append(f"User: {q}")
            if a:
                lines.append(f"Assistant: {a[:300]}{'...' if len(a) > 300 else ''}")
        return "\n".join(lines) if lines else "None"

    def _get_last_sql(self, context: list[dict]) -> str:
        """Return the most recent SQL from context."""
        for turn in reversed(context):
            sql = turn.get("sql", "").strip()
            if sql:
                return sql
        return ""