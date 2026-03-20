"""
Lead owns this file.
End-to-end pipeline test — runs with mocked LLM + adapter,
no real DB or API key needed.

Run: pytest tests/integration/test_full_pipeline.py -v
"""
import pytest
from unittest.mock import MagicMock, patch
from services.query_service import QueryService, QueryResponse
from rag.schema_retriever import SchemaRetriever
from core.interfaces import StrategyResult


@pytest.fixture
def mock_retriever(mock_adapter):
    retriever = MagicMock(spec=SchemaRetriever)
    retriever.retrieve.return_value = [
        {"entity": "products", "schema_text": "Table: products — columns: id (int), name (varchar), price (decimal)"}
    ]
    return retriever


@pytest.fixture
def mock_strategy(sample_strategy_result):
    strategy = MagicMock()
    strategy.strategy_name = "sql_filter"
    strategy.execute.return_value = sample_strategy_result
    return strategy


@pytest.fixture
def service(mock_llm, mock_adapter, mock_strategy, mock_retriever):
    mock_llm.generate.side_effect = [
        "SELECT * FROM products WHERE category = 'Sci-Fi'",   # query generation
        "There are 3 Sci-Fi books in the database.",           # answer generation
    ]
    return QueryService(
        llm=mock_llm,
        adapter=mock_adapter,
        strategy=mock_strategy,
        retriever=mock_retriever,
    )


def test_full_pipeline_returns_query_response(service):
    result = service.run("show me all sci-fi books")

    assert isinstance(result, QueryResponse)
    assert result.error is None
    assert result.row_count == 3
    assert result.strategy_used == "sql_filter"
    assert "Sci-Fi" in result.sql
    assert len(result.results) == 3


def test_pipeline_returns_natural_language_answer(service):
    result = service.run("how many sci-fi books do we have?")
    assert "3" in result.answer or "Sci-Fi" in result.answer


def test_pipeline_returns_error_on_max_retries(mock_llm, mock_adapter, mock_retriever):
    mock_llm.generate.return_value = "SELECT bad sql"
    bad_strategy = MagicMock()
    bad_strategy.strategy_name = "sql_filter"
    bad_strategy.execute.side_effect = Exception("syntax error")

    service = QueryService(
        llm=mock_llm,
        adapter=mock_adapter,
        strategy=bad_strategy,
        retriever=mock_retriever,
    )

    result = service.run("broken query")
    assert result.error is not None
    assert result.row_count == 0


def test_retriever_is_called_with_question(service, mock_retriever):
    service.run("show products under $20")
    mock_retriever.retrieve.assert_called_once_with("show products under $20")


def test_llm_called_twice_per_successful_query(service, mock_llm):
    """First call = SQL generation, second = answer generation."""
    service.run("show all products")
    assert mock_llm.generate.call_count == 2
