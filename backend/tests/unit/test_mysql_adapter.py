"""
Dev 1 owns this file.
Tests for MySQLAdapter — use mocks, no real DB needed.
Run: pytest tests/unit/test_mysql_adapter.py -v
"""
from unittest.mock import MagicMock, patch
import pytest
from adapters.mysql_adapter import MySQLAdapter


@pytest.fixture
def adapter():
    with patch("adapters.mysql_adapter.pymysql"):
        a = MySQLAdapter()
        a._connection = MagicMock()
        return a


def test_execute_query_returns_list_of_dicts(adapter):
    """execute_query should return rows as list[dict]"""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [{"id": 1, "name": "test"}]
    adapter._connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    adapter._connection.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # TODO (Dev 1): uncomment once execute_query is implemented
    # result = adapter.execute_query("SELECT * FROM products")
    # assert isinstance(result, list)
    # assert result[0]["name"] == "test"
    pytest.skip("Implement execute_query first")


def test_health_check_returns_true_on_live_connection(adapter):
    adapter._connection.ping = MagicMock()
    # TODO (Dev 1): uncomment once health_check is implemented
    # assert adapter.health_check() is True
    pytest.skip("Implement health_check first")


def test_fetch_schema_returns_structured_dict(adapter):
    # TODO (Dev 1): mock cursor responses for SHOW TABLES + DESCRIBE
    # result = adapter.fetch_schema()
    # assert isinstance(result, dict)
    # assert all(isinstance(v, list) for v in result.values())
    pytest.skip("Implement fetch_schema first")


def test_execute_query_raises_on_connection_error(adapter):
    adapter._connection = None
    with pytest.raises(Exception):
        adapter.execute_query("SELECT 1")
