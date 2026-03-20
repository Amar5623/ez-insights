from dataclasses import dataclass, field


class MaxRetriesExceeded(Exception):
    """Raised when all retry attempts have been exhausted."""
    pass


@dataclass
class AttemptRecord:
    attempt_number: int
    query_used: str
    error: str


def with_retry(execute_fn, question: str, llm, max_retries: int = 3):
    """
    Wraps a strategy execution in a retry loop.

    On each failure:
    - records the error + query in attempt history
    - re-prompts the LLM with error context to get a corrected query
    - retries execution with the new query

    Raises MaxRetriesExceeded after max_retries failed attempts.

    Dev 2 owns this function.

    Args:
        execute_fn:  callable(question, query) → StrategyResult
        question:    original user question
        llm:         BaseLLM instance for re-prompting on error
        max_retries: max attempts before giving up
    """
    # TODO (Dev 2):
    # history: list[AttemptRecord] = []
    # generated_query = llm.generate(initial_prompt(question))
    #
    # for attempt in range(1, max_retries + 1):
    #     try:
    #         return execute_fn(question, generated_query)
    #     except Exception as e:
    #         history.append(AttemptRecord(attempt, generated_query, str(e)))
    #         if attempt == max_retries:
    #             raise MaxRetriesExceeded(
    #                 f"Failed after {max_retries} attempts. History: {history}"
    #             )
    #         generated_query = llm.generate(retry_prompt(question, history))
    raise NotImplementedError
