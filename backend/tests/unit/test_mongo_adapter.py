"""
Dev 1 owns this file.
Tests for MongoAdapter — use mocks, no real DB needed.
Run: pytest tests/unit/test_mongo_adapter.py -v
"""
from unittest.mock import MagicMock, patch
import pytest
from bson import ObjectId
from adapters.mongo_adapter import MongoAdapter


@pytest.fixture
def adapter():
    with patch("pymongo.MongoClient"):   # ← only change this line
        a = MongoAdapter()
        a._client = MagicMock()
        a._db = MagicMock()
        return a


def test_execute_query_returns_list_of_dicts(adapter):
    """execute_query should return rows as list[dict]"""
    mock_collection = MagicMock()
    mock_collection.find.return_value.limit.return_value = [
        {"_id": "abc123", "name": "Dune", "price": 12.99}
    ]
    adapter._db.__getitem__.return_value = mock_collection

    result = adapter.execute_query({
        "collection": "books",
        "filter": {},
        "limit": 20
    })

    assert isinstance(result, list)
    assert result[0]["name"] == "Dune"


def test_execute_query_converts_objectid_to_str(adapter):
    """ObjectId fields must be converted to str — JSON can't serialize ObjectId"""
    mock_collection = MagicMock()
    mock_collection.find.return_value.limit.return_value = [
        {"_id": ObjectId("507f1f77bcf86cd799439011"), "name": "Dune"}
    ]
    adapter._db.__getitem__.return_value = mock_collection

    result = adapter.execute_query({
        "collection": "books",
        "filter": {},
        "limit": 10
    })

    assert isinstance(result[0]["_id"], str)   # must be string not ObjectId
    assert result[0]["_id"] == "507f1f77bcf86cd799439011"


def test_execute_query_returns_empty_list_when_no_docs(adapter):
    """execute_query must return [] not None when no documents found"""
    mock_collection = MagicMock()
    mock_collection.find.return_value.limit.return_value = []
    adapter._db.__getitem__.return_value = mock_collection

    result = adapter.execute_query({
        "collection": "books",
        "filter": {"price": {"$gt": 9999}},
        "limit": 10
    })

    assert result == []
    assert result is not None   # NEVER None


def test_execute_query_raises_on_no_connection(adapter):
    """execute_query must raise RuntimeError if not connected"""
    adapter._db = None
    with pytest.raises(RuntimeError):
        adapter.execute_query({"collection": "books", "filter": {}})


def test_execute_query_raises_on_missing_collection_key(adapter):
    """execute_query must raise ValueError if collection key is missing"""
    with pytest.raises(ValueError):
        adapter.execute_query({"filter": {"price": {"$gt": 10}}})


def test_execute_query_raises_if_query_is_not_dict(adapter):
    """MongoAdapter must reject SQL strings — only accepts dict queries"""
    with pytest.raises(ValueError):
        adapter.execute_query("SELECT * FROM books")


def test_health_check_returns_true_when_ping_succeeds(adapter):
    """health_check returns True when MongoDB ping command succeeds"""
    adapter._client.admin.command = MagicMock(return_value={"ok": 1})

    assert adapter.health_check() is True
    adapter._client.admin.command.assert_called_once_with("ping")


def test_health_check_returns_false_when_ping_fails(adapter):
    """health_check returns False when ping raises — never raises itself"""
    adapter._client.admin.command.side_effect = Exception("connection lost")

    result = adapter.health_check()
    assert result is False   # must return False, not raise


def test_fetch_schema_returns_structured_dict(adapter):
    """fetch_schema returns dict of collection → list of field dicts"""
    adapter._db.list_collection_names.return_value = ["books"]
    adapter._db.__getitem__.return_value.find.return_value.limit.return_value = [
        {"title": "Dune", "price": 12.99, "in_stock": True}
    ]

    result = adapter.fetch_schema()

    assert "books" in result
    fields = {f["field"] for f in result["books"]}
    assert "title" in fields
    assert "price" in fields


def test_disconnect_resets_client_and_db_to_none(adapter):
    """disconnect must reset both _client and _db to None"""
    adapter.disconnect()
    assert adapter._client is None
    assert adapter._db is None


def test_disconnect_safe_when_not_connected(adapter):
    """disconnect must not raise if called before connect"""
    adapter._client = None
    adapter._db = None
    adapter.disconnect()   # should not raise anything