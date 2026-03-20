"""
Lead owns this file.
Builds the final prompt sent to the LLM using the retrieved schema context.
"""
from core.interfaces import BaseDBAdapter


SQL_GENERATION_TEMPLATE = """
You are an expert SQL query generator.

Database type: {db_type}

Relevant schema:
{schema_context}

User question: {question}

Previous attempts and errors (if any):
{attempt_history}

Instructions:
- Generate a single valid {db_type} SELECT query that answers the question.
- Use only tables and columns present in the schema above.
- Do NOT use DROP, DELETE, UPDATE, INSERT, ALTER, or TRUNCATE.
- Return ONLY the SQL query — no explanation, no markdown, no backticks.
""".strip()

MONGO_GENERATION_TEMPLATE = """
You are an expert MongoDB query generator.

Relevant schema (inferred from sampled documents):
{schema_context}

User question: {question}

Previous attempts and errors (if any):
{attempt_history}

Instructions:
- Return a JSON object with keys: "collection", "filter", "limit" (max 20).
- Use only collections and fields present in the schema above.
- Return ONLY the JSON object — no explanation, no markdown.
""".strip()

ANSWER_GENERATION_TEMPLATE = """
You are a helpful data analyst assistant.

The user asked: {question}

The query returned {row_count} result(s):
{results_preview}

Write a clear, concise natural language answer to the user's question
based on the data above. Be specific — mention actual values from the results.
If there are no results, say so clearly and suggest why that might be.
""".strip()


class PromptBuilder:
    def __init__(self, adapter: BaseDBAdapter):
        self.adapter = adapter

    def build_query_prompt(
        self,
        question: str,
        schema_chunks: list[dict],
        attempt_history: list = None,
    ) -> str:
        """Build the SQL/Mongo generation prompt with schema context injected."""
        schema_context = "\n".join(
            chunk["schema_text"] for chunk in schema_chunks
        )
        history_text = self._format_history(attempt_history or [])

        if self.adapter.db_type == "mysql":
            return SQL_GENERATION_TEMPLATE.format(
                db_type="MySQL",
                schema_context=schema_context,
                question=question,
                attempt_history=history_text,
            )

        return MONGO_GENERATION_TEMPLATE.format(
            schema_context=schema_context,
            question=question,
            attempt_history=history_text,
        )

    def build_answer_prompt(
        self, question: str, rows: list[dict], row_count: int
    ) -> str:
        """Build the natural language answer generation prompt."""
        from core.config.settings import get_settings
        max_rows = get_settings().MAX_RESULT_ROWS

        preview_rows = rows[:max_rows]
        # Strip any embedding columns — they're huge and not human-readable
        cleaned = [
            {k: v for k, v in row.items() if "embed" not in k.lower()}
            for row in preview_rows
        ]
        results_preview = "\n".join(str(row) for row in cleaned)

        return ANSWER_GENERATION_TEMPLATE.format(
            question=question,
            row_count=row_count,
            results_preview=results_preview or "(no rows returned)",
        )

    def _format_history(self, history: list) -> str:
        if not history:
            return "None"
        lines = []
        for attempt in history:
            lines.append(
                f"Attempt {attempt.attempt_number}: {attempt.query_used}\n"
                f"Error: {attempt.error}"
            )
        return "\n\n".join(lines)
