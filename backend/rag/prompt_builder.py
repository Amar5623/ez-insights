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
Decide whether the question needs a simple find or an aggregation pipeline.

CASE A — Simple find (filtering, listing, fetching documents):
Return a JSON object with exactly these keys:
  "collection" : name of the collection to query
  "filter"     : MongoDB filter dict (use {{}} for no filter)
  "limit"      : max number of documents to return (max 20)

Example:
  {{"collection": "sales", "filter": {{"coupon_used": true}}, "limit": 20}}

CASE B — Aggregation (counting, averaging, summing, grouping):
Return a JSON object with exactly these keys:
  "collection" : name of the collection to query
  "pipeline"   : list of MongoDB aggregation stages (e.g. $match, $group, $project)
  "limit"      : max number of result documents (max 20)

Example for "how many customers use coupons on average":
  {{
    "collection": "sales",
    "pipeline": [
      {{"$group": {{"_id": "$customer_id", "coupon_count": {{"$sum": {{"$cond": ["$coupon_used", 1, 0]}}}}}}}},
      {{"$group": {{"_id": null, "avg_coupon_usage": {{"$avg": "$coupon_count"}}}}}},
      {{"$project": {{"_id": 0, "avg_coupon_usage": 1}}}}
    ],
    "limit": 5
  }}

Rules:
- Use only collections and fields present in the schema above.
- Return ONLY the raw JSON object — no explanation, no markdown, no backticks.
- The response must start with {{ and end with }}.
- Do NOT wrap the JSON in ```json or ``` fences.
- Use CASE B whenever the question asks for count, average, total, sum, max, min, or any aggregation.
""".strip()

ANSWER_GENERATION_TEMPLATE = """
You are a helpful data analyst assistant.

The user asked: {question}

Query that was executed:
{sql_query}

The query returned {row_count} result(s):
{results_preview}

{quality_instruction}
""".strip()

# ── Quality instructions injected into the answer prompt ──────────────────────
_QUALITY_INSTRUCTIONS: dict[str, str] = {
    "ok": (
        "Write a clear, concise natural language answer based on the data above. "
        "Be specific — mention actual values from the results."
    ),
    "empty": (
        "No results were returned. Tell the user clearly that nothing was found. "
        "Suggest one or two likely reasons: a filter value that doesn't match the data, "
        "no records for that time period, or a possible typo in a name. "
        "Offer a concrete rephrased question they could try next."
    ),
    "all_null": (
        "The query returned rows but all values appear to be empty or null. "
        "Tell the user the query ran successfully but the data in those fields "
        "appears to be missing or not yet populated. "
        "Suggest they check whether the data exists or try querying a different table."
    ),
    "low_relevance": (
        "These results may not directly answer the question — the returned columns "
        "do not closely match what was asked. Be honest about this: briefly describe "
        "what was actually returned, explain why it might not be the right data, "
        "and suggest how the user could rephrase their question to get a better result. "
        "Do not fabricate an answer from unrelated data."
    ),
}


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
        self,
        question: str,
        rows: list[dict],
        row_count: int,
        quality: str = "ok",
        sql_query: str = "",
    ) -> str:
        """
        Build the natural language answer generation prompt.

        Args:
            question:   The original user question.
            rows:       Full result rows from the DB (may be large).
            row_count:  Total number of rows (used in prompt even if rows are capped).
            quality:    Result quality signal from QueryService._assess_result_quality().
                        One of: 'ok' | 'empty' | 'all_null' | 'low_relevance'.
            sql_query:  The actual SQL/Mongo query that was executed.
        """
        from core.config.settings import get_settings
        max_for_llm = get_settings().MAX_ROWS_FOR_LLM

        preview_rows = rows[:max_for_llm]

        # Strip embedding columns — they are huge vectors, not human-readable.
        cleaned = [
            {k: v for k, v in row.items() if "embed" not in k.lower()}
            for row in preview_rows
        ]
        results_preview = "\n".join(str(row) for row in cleaned)

        if row_count > max_for_llm:
            results_preview += (
                f"\n\n(Note: showing {max_for_llm} of {row_count} total rows. "
                f"The user can see all {row_count} rows in the results table.)"
            )

        quality_instruction = _QUALITY_INSTRUCTIONS.get(
            quality, _QUALITY_INSTRUCTIONS["ok"]
        )

        return ANSWER_GENERATION_TEMPLATE.format(
            question=question,
            sql_query=sql_query or "(not available)",
            row_count=row_count,
            results_preview=results_preview or "(no rows returned)",
            quality_instruction=quality_instruction,
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