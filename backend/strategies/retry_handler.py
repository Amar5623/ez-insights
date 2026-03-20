"""
strategies/retry_handler.py
Dev 2 owns this file.

Wraps strategy execution in a retry loop with LLM re-prompting on failure.

How it fits in the system:
    query_service.py calls:
        result = with_retry(
            execute_fn=lambda q, query: strategy.execute(q, query),
            question=question,
            llm=self.llm,
            max_retries=get_settings().MAX_RETRIES,
        )

Flow for max_retries=3:
    LLM.generate(initial_prompt)     → query_1
    execute_fn(question, query_1)
        ├── SUCCESS → return result immediately
        └── FAIL    → record AttemptRecord(1, query_1, error)
                      LLM.generate(retry_prompt with error context) → query_2
                      execute_fn(question, query_2)
                          ├── SUCCESS → return result
                          └── FAIL    → record AttemptRecord(2, query_2, error)
                                        LLM.generate(retry_prompt) → query_3
                                        execute_fn(question, query_3)
                                            ├── SUCCESS → return result
                                            └── FAIL    → raise MaxRetriesExceeded

Key design:
    - LLM is called ONCE before the first attempt (generates the initial query)
    - LLM is called again on each failure EXCEPT the last one
    - Total LLM calls = max_retries (1 initial + max_retries-1 retries)
    - Total execute_fn calls = max_retries on full failure
    - On success: execute_fn calls = attempt number that succeeded
"""

from __future__ import annotations

from dataclasses import dataclass


# ─── Exceptions ───────────────────────────────────────────────────────────────

class MaxRetriesExceeded(Exception):
    """
    Raised when all retry attempts have been exhausted.

    The exception message includes the full attempt history so the caller
    (QueryService) can log exactly what was tried and what failed each time.
    """
    pass


# ─── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class AttemptRecord:
    """
    Records what happened on a single execution attempt.

    Fields:
        attempt_number: 1-based attempt index (1 = first try)
        query_used:     The SQL string or filter dict that was executed
        error:          The exception message from the failed attempt
    """
    attempt_number: int
    query_used: str
    error: str


# ─── Prompt builders ──────────────────────────────────────────────────────────

def _initial_prompt(question: str) -> str:
    """
    Build the prompt for the very first LLM call.
    Asks the LLM to generate a query for the given question.
    """
    return (
        f"Generate a database query to answer this question:\n"
        f"{question}\n\n"
        f"Return only the query — no explanation."
    )


def _retry_prompt(question: str, history: list[AttemptRecord]) -> str:
    """
    Build the retry prompt that includes full error context from
    all previous failed attempts.

    The test checks that the second LLM call prompt contains either
    'unknown column' (the actual error text) or 'attempt'.
    This prompt includes both — the word 'attempt' in the header and
    the full error text from each AttemptRecord.

    Example output:
        The following query attempts failed. Please correct the query.

        Question: show prices

        Attempt 1:
        Query: SELECT pric FROM products
        Error: unknown column 'pric'

        Generate a corrected query. Return only the query.
    """
    lines = [
        "The following query attempts failed. Please correct the query.",
        "",
        f"Question: {question}",
        "",
    ]

    for record in history:
        lines.append(f"Attempt {record.attempt_number}:")
        lines.append(f"Query: {record.query_used}")
        lines.append(f"Error: {record.error}")
        lines.append("")

    lines.append("Generate a corrected query. Return only the query.")

    return "\n".join(lines)


# ─── Main retry function ───────────────────────────────────────────────────────

def with_retry(
    execute_fn,
    question: str,
    llm,
    max_retries: int = 3,
):
    """
    Wrap a strategy execution in a retry loop with LLM re-prompting.

    On each failure:
        1. Records the error + query in attempt history
        2. Re-prompts the LLM with full error context
        3. Retries with the corrected query

    Raises MaxRetriesExceeded after max_retries failed attempts.

    Args:
        execute_fn:  callable(question: str, query: str) → StrategyResult
                     Typically: lambda q, query: strategy.execute(q, query)
        question:    Original natural language question from the user.
        llm:         BaseLLM instance — used to generate and correct queries.
        max_retries: Maximum number of execution attempts before giving up.
                     Default 3. Set to 1 for a single attempt with no retries.

    Returns:
        StrategyResult from the first successful execute_fn call.

    Raises:
        MaxRetriesExceeded: After all max_retries attempts have failed.
                            Message includes the full attempt history.
    """
    history: list[AttemptRecord] = []

    # Generate the initial query — LLM call #1
    generated_query = llm.generate(_initial_prompt(question))

    for attempt in range(1, max_retries + 1):
        try:
            # Try executing with the current query
            return execute_fn(question, generated_query)

        except Exception as exc:
            # Record what failed
            history.append(
                AttemptRecord(
                    attempt_number=attempt,
                    query_used=generated_query,
                    error=str(exc),
                )
            )

            # If this was the last allowed attempt → give up
            if attempt == max_retries:
                raise MaxRetriesExceeded(
                    f"Failed after {max_retries} attempt(s). "
                    f"History: {history}"
                ) from exc

            # Otherwise → re-prompt LLM with error context and retry
            generated_query = llm.generate(_retry_prompt(question, history))