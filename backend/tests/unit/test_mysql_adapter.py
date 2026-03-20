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

    result = adapter.execute_query("SELECT * FROM products")
    assert isinstance(result, list)
    assert result[0]["name"] == "test"


def test_execute_query_returns_empty_list_when_no_rows(adapter):
    """execute_query must return [] not None when no rows found"""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    adapter._connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    adapter._connection.cursor.return_value.__exit__ = MagicMock(return_value=False)

    result = adapter.execute_query("SELECT * FROM products WHERE id = 9999")
    assert result == []
    assert result is not None   # NEVER None


def test_execute_query_raises_on_connection_error(adapter):
    """execute_query must raise if connection is None"""
    adapter._connection = None
    with pytest.raises(Exception):
        adapter.execute_query("SELECT 1")


def test_health_check_returns_true_on_live_connection(adapter):
    """health_check returns True when SELECT 1 succeeds"""
    mock_cursor = MagicMock()
    adapter._connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    adapter._connection.cursor.return_value.__exit__ = MagicMock(return_value=False)

    assert adapter.health_check() is True


def test_health_check_returns_false_on_dead_connection(adapter):
    """health_check returns False when connection is dead — never raises"""
    adapter._connection.cursor.side_effect = Exception("connection lost")

    result = adapter.health_check()
    assert result is False   # must return False, not raise


def test_fetch_schema_returns_structured_dict(adapter):
    """fetch_schema returns dict of table → list of column dicts"""
    mock_cursor = MagicMock()

    # SHOW TABLES returns one row per table
    # DESCRIBE returns column rows
    mock_cursor.fetchall.side_effect = [
        [{"Tables_in_nlsql_db": "books"}],    # SHOW TABLES result
        [                                       # DESCRIBE books result
            {"Field": "id",    "Type": "int(11)",      "Null": "NO"},
            {"Field": "title", "Type": "varchar(255)", "Null": "NO"},
            {"Field": "price", "Type": "decimal(10,2)","Null": "YES"},
        ]
    ]
    adapter._connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    adapter._connection.cursor.return_value.__exit__ = MagicMock(return_value=False)

    result = adapter.fetch_schema()

    assert isinstance(result, dict)
    assert "books" in result
    assert isinstance(result["books"], list)
    assert result["books"][0]["column"] == "id"
    assert result["books"][0]["nullable"] is False
    assert result["books"][2]["nullable"] is True   # price is nullable


def test_disconnect_resets_connection_to_none(adapter):
    """disconnect must reset _connection to None"""
    adapter.disconnect()
    assert adapter._connection is None


def test_disconnect_safe_when_not_connected(adapter):
    """disconnect must not raise if called before connect"""
    adapter._connection = None
    adapter.disconnect()   # should not raise anything