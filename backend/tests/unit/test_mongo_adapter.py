"""
Dev 1 owns this file.
Tests for MongoAdapter — use mocks, no real DB needed.
Run: pytest tests/unit/test_mongo_adapter.py -v
"""
from unittest.mock import MagicMock, patch
import pytest
from adapters.mongo_adapter import MongoAdapter


@pytest.fixture
def adapter():
    with patch("adapters.mongo_adapter.pymongo"):
        a = MongoAdapter()
        a._client = MagicMock()
        a._db = MagicMock()
        return a


def test_execute_query_returns_list_of_dicts(adapter):
    """execute_query should return rows as list[dict] with ObjectId converted to str."""
    mock_collection = MagicMock()
    mock_collection.find.return_value.limit.return_value = [
        {"_id": "abc123", "name": "Dune", "price": 12.99}
    ]
    adapter._db.__getitem__.return_value = mock_collection

    # TODO (Dev 1): uncomment once execute_query is implemented
    # result = adapter.execute_query({"collection": "books", "filter": {}, "limit": 20})
    # assert isinstance(result, list)
    # assert result[0]["name"] == "Dune"
    # assert isinstance(result[0]["_id"], str)   # ObjectId must be stringified
    pytest.skip("Implement execute_query first")


def test_health_check_pings_admin(adapter):
    adapter._client.admin.command = MagicMock(return_value={"ok": 1})
    # TODO (Dev 1): uncomment once health_check is implemented
    # assert adapter.health_check() is True
    # adapter._client.admin.command.assert_called_once_with("ping")
    pytest.skip("Implement health_check first")


def test_fetch_schema_returns_structured_dict(adapter):
    adapter._db.list_collection_names.return_value = ["books"]
    adapter._db.__getitem__.return_value.find.return_value.limit.return_value = [
        {"title": "Dune", "price": 12.99, "in_stock": True}
    ]
    # TODO (Dev 1): uncomment once fetch_schema is implemented
    # result = adapter.fetch_schema()
    # assert "books" in result
    # fields = {f["field"] for f in result["books"]}
    # assert "title" in fields
    # assert "price" in fields
    pytest.skip("Implement fetch_schema first")


def test_execute_query_stringifies_object_ids(adapter):
    """ObjectId fields must be converted to str so JSON serialisation works."""
    # TODO (Dev 1): mock a doc with a real bson.ObjectId and verify conversion
    pytest.skip("Implement ObjectId handling first")
