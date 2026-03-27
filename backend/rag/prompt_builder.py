"""
rag/prompt_builder.py

Builds the prompts sent to the LLM for:
  1. SQL / Mongo query generation  (build_query_prompt)
  2. Natural language answer generation (build_answer_prompt)

IMPORTANT CHANGE FROM ORIGINAL:
    All hardcoded strings (SQL_SYSTEM_PROMPT, ANSWER_SYSTEM_PROMPT,
    DB_SCHEMA_CONTEXT) have been REMOVED from this file.
    They now live in the client config bundle:
        client-configs/<client>/prompts/sql_system.md
        client-configs/<client>/prompts/answer_system.md
        client-configs/<client>/db_context.yaml

    This file is now 100% generic — it works for any client
    without modification.

    To onboard a new client: copy client-configs/classicmodels/,
    edit the files in the new folder, set CLIENT_CONFIG_PATH in .env.
    This file never changes.
"""

import json
from core.interfaces import BaseDBAdapter
from core.client_config import get_client_config
from core.logging_config import get_logger, truncate

logger = get_logger(__name__)


# ── User message templates ────────────────────────────────────────────────────
# These are structural templates — not client-specific content.
# They define WHERE the dynamic parts go in the user message.
# The content (db_schema_context, business rules) all comes from client config.

_SQL_USER_TEMPLATE = """## Static DB Context
{db_context}

## RAG-Retrieved Schema (most relevant tables for this question)
{schema_context}

## Previous Attempts (if any)
{attempt_history}

## Question
{question}""".strip()


_MONGO_USER_TEMPLATE = """## RAG-Retrieved Schema (inferred from sampled documents)
{schema_context}

## Previous Attempts (if any)
{attempt_history}

## Question
{question}""".strip()


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
# These are generic — no client-specific content.

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
    """
    Builds prompts for SQL generation and NL answer generation.

    All client-specific content (system prompts, DB context, business rules)
    is loaded from the client config bundle via get_client_config().

    This class is stateless beyond the adapter reference — it reads from
    the cached ClientConfig on every call (which is a no-op after first load).
    """

    def __init__(self, adapter: BaseDBAdapter):
        self.adapter = adapter
        # Eagerly load client config at construction time so any config errors
        # surface at startup, not on the first user request.
        cfg = get_client_config()
        logger.info(
            f"[PROMPT] PromptBuilder initialized | "
            f"client='{cfg.company_name}' | "
            f"db_type={adapter.db_type}"
        )

    # ── SQL / Query generation ─────────────────────────────────────────────────

    def build_query_prompt(
        self,
        question: str,
        schema_chunks: list[dict],
        attempt_history: list = None,
    ) -> dict[str, str]:
        """
        Build the SQL or Mongo query generation prompt.

        Returns {"system": ..., "user": ...} for use as separate LLM roles.

        The system message comes from client config (sql_system.md).
        The user message combines:
          - Static DB context from db_context.yaml (table structure, valid values, FK chain)
          - RAG-retrieved schema chunks (the most relevant tables for this specific question)
          - Prior attempt history (for the retry loop)
          - The question itself

        Args:
            question:        The user's natural language question.
            schema_chunks:   Top-K schema chunks from SchemaRetriever.retrieve().
            attempt_history: List of AttemptRecord from prior failed attempts.
        """
        cfg = get_client_config()

        # Format schema chunks from RAG retrieval
        schema_context = "\n\n".join(
            chunk.get("schema_text", "") for chunk in schema_chunks
            if chunk.get("schema_text")
        )
        if not schema_context:
            schema_context = "(No relevant schema chunks retrieved)"

        history_text = self._format_history(attempt_history or [])

        logger.debug(
            f"[PROMPT] Building query prompt | "
            f"db_type={self.adapter.db_type} | "
            f"schema_chunks={len(schema_chunks)} | "
            f"has_history={bool(attempt_history)}"
        )
        logger.debug(f"[PROMPT] Schema context:\n{schema_context[:500]}")

        if self.adapter.db_type == "mysql":
            user_content = _SQL_USER_TEMPLATE.format(
                db_context=cfg.db_context_markdown,
                schema_context=schema_context,
                attempt_history=history_text,
                question=question,
            )
            return {
                "system": cfg.sql_system_prompt,
                "user": user_content,
            }

        # MongoDB path — no static DB context (schema is inferred at runtime)
        user_content = _MONGO_USER_TEMPLATE.format(
            schema_context=schema_context,
            attempt_history=history_text,
            question=question,
        )
        return {
            "system": cfg.sql_system_prompt,  # same safety rules apply
            "user": user_content,
        }

    # ── NL answer generation ──────────────────────────────────────────────────

    def build_answer_prompt(
        self,
        question: str,
        rows: list[dict],
        row_count: int,
        quality: str = "small",
        sql_query: str = "",
        context: list[dict] | None = None,
    ) -> dict[str, str]:
        """
        Build the natural language answer generation prompt.

        Returns {"system": ..., "user": ...}.

        Args:
            question:   The original user question.
            rows:       Result rows from the DB (may be large).
            row_count:  Total number of rows returned.
            quality:    Result quality signal — 'small' | 'large' | 'empty' |
                        'all_null' | 'low_relevance'
            sql_query:  The actual SQL/Mongo query that ran.
            context:    Last N conversation turns for multi-turn context.
                        Each turn: {"question": ..., "sql": ..., "answer": ...}
        """
        from core.config.settings import get_settings
        cfg = get_client_config()
        max_for_llm = get_settings().MAX_ROWS_FOR_LLM

        # Cap rows sent to LLM (full result set is still in the API response)
        preview_rows = rows[:max_for_llm]

        # Strip embedding columns — they are huge vectors, not human-readable
        cleaned = [
            {k: v for k, v in row.items() if "embed" not in k.lower()}
            for row in preview_rows
        ]

        # Format as JSON for readability (better than Python repr)
        try:
            results_preview = json.dumps(cleaned, indent=2, default=str)
        except Exception:
            results_preview = "\n".join(str(row) for row in cleaned)

        if row_count > max_for_llm:
            results_preview += (
                f"\n\n(Showing {max_for_llm} of {row_count} total rows. "
                f"The user can see all {row_count} rows in the results table.)"
            )

        _raw_instruction = _QUALITY_INSTRUCTIONS.get(quality, _QUALITY_INSTRUCTIONS["small"])
        quality_instruction = _raw_instruction.format(
            total_rows=row_count,
            page_size=get_settings().PAGE_SIZE,
        ) if "{total_rows}" in _raw_instruction else _raw_instruction

        # Sliding window conversation context (hard cap at 5 turns)
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

        return {
            "system": cfg.answer_system_prompt,
            "user": user_content,
        }

    def _format_history(self, history: list) -> str:
        """Format retry attempt history for inclusion in the prompt."""
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