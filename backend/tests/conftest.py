"""
Shared pytest fixtures available to all tests.
No DB or external service required — everything is mocked.
"""
import pytest
from unittest.mock import MagicMock, AsyncMock
from core.interfaces import BaseDBAdapter, BaseLLM, StrategyResult


@pytest.fixture
def mock_adapter():
    """A mock BaseDBAdapter that returns empty results by default."""
    adapter = MagicMock(spec=BaseDBAdapter)
    adapter.db_type = "mysql"
    adapter.execute_query.return_value = []
    adapter.fetch_schema.return_value = {
        "products": [
            {"column": "id", "type": "int", "nullable": False},
            {"column": "name", "type": "varchar(255)", "nullable": False},
            {"column": "price", "type": "decimal(10,2)", "nullable": True},
            {"column": "category", "type": "varchar(100)", "nullable": True},
        ]
    }
    adapter.health_check.return_value = True
    return adapter


@pytest.fixture
def mock_llm():
    """A mock BaseLLM that returns a simple SELECT by default."""
    llm = MagicMock(spec=BaseLLM)
    llm.provider_name = "mock"
    llm.generate.return_value = "SELECT * FROM products LIMIT 10"
    llm.generate_with_history.return_value = "SELECT * FROM products LIMIT 10"
    return llm


@pytest.fixture
def sample_rows():
    """Realistic sample rows for result assertions."""
    return [
        {"id": 1, "name": "Dune", "price": 12.99, "category": "Sci-Fi"},
        {"id": 2, "name": "Foundation", "price": 9.99, "category": "Sci-Fi"},
        {"id": 3, "name": "Neuromancer", "price": 11.49, "category": "Sci-Fi"},
    ]


@pytest.fixture
def sample_strategy_result(sample_rows):
    """A completed StrategyResult for use in service-layer tests."""
    return StrategyResult(
        rows=sample_rows,
        query_used="SELECT * FROM products WHERE category = 'Sci-Fi'",
        strategy_name="sql_filter",
        row_count=3,
    )
