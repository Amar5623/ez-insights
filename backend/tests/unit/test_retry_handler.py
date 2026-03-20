"""
Dev 2 owns this file.
Tests for retry_handler — no DB needed, pure logic.
Run: pytest tests/unit/test_retry_handler.py -v
"""
import pytest
from unittest.mock import MagicMock, call
from strategies.retry_handler import with_retry, MaxRetriesExceeded, AttemptRecord
from core.interfaces import StrategyResult


def make_mock_llm(responses: list[str]):
    """LLM that returns responses in sequence."""
    llm = MagicMock()
    llm.generate.side_effect = responses
    return llm


def make_strategy_result():
    return StrategyResult(
        rows=[{"id": 1}],
        query_used="SELECT 1",
        strategy_name="sql_filter",
        row_count=1,
    )


def test_succeeds_on_first_attempt():
    """Should return result immediately if first attempt works."""
    execute_fn = MagicMock(return_value=make_strategy_result())
    llm = make_mock_llm(["SELECT 1"])

    result = with_retry(execute_fn, "show products", llm, max_retries=3)

    assert result.row_count == 1
    assert execute_fn.call_count == 1


def test_retries_on_failure_then_succeeds():
    """Should retry after failure and return result on second attempt."""
    execute_fn = MagicMock(side_effect=[
        Exception("syntax error"),
        make_strategy_result(),
    ])
    llm = make_mock_llm(["SELECT bad", "SELECT * FROM products"])

    result = with_retry(execute_fn, "show products", llm, max_retries=3)

    assert result.row_count == 1
    assert execute_fn.call_count == 2


def test_raises_after_max_retries():
    """Should raise MaxRetriesExceeded after all attempts fail."""
    execute_fn = MagicMock(side_effect=Exception("always fails"))
    llm = make_mock_llm(["q1", "q2", "q3"])

    with pytest.raises(MaxRetriesExceeded):
        with_retry(execute_fn, "show products", llm, max_retries=3)

    assert execute_fn.call_count == 3


def test_error_history_passed_to_llm_on_retry():
    """LLM should receive previous error context when generating retry query."""
    execute_fn = MagicMock(side_effect=[
        Exception("unknown column 'pric'"),
        make_strategy_result(),
    ])
    llm = make_mock_llm(["SELECT pric FROM products", "SELECT price FROM products"])

    with_retry(execute_fn, "show prices", llm, max_retries=3)

    # Second LLM call should include error context
    second_call_prompt = llm.generate.call_args_list[1][0][0]
    assert "unknown column" in second_call_prompt.lower() or \
           "attempt" in second_call_prompt.lower()


def test_max_retries_one_means_single_attempt():
    execute_fn = MagicMock(side_effect=Exception("fail"))
    llm = make_mock_llm(["SELECT 1"])

    with pytest.raises(MaxRetriesExceeded):
        with_retry(execute_fn, "question", llm, max_retries=1)

    assert execute_fn.call_count == 1
