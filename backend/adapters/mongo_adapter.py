from typing import Any
from bson import ObjectId
from core.interfaces import BaseDBAdapter
from core.config.settings import get_settings


class MongoAdapter(BaseDBAdapter):
    """
    MongoDB implementation of BaseDBAdapter using pymongo.
    Dev 1 owns this file.

    DO NOT import this directly anywhere — use db_factory.create_db_adapter()
    """

    def __init__(self):
        self._client = None
        self._db = None
        s = get_settings()
        self._uri = s.MONGO_URI
        self._db_name = s.MONGO_DATABASE

    def connect(self) -> None:
        if self._client is not None:
            return
        try:
            from pymongo import MongoClient
            self._client = MongoClient(self._uri)
            self._db = self._client[self._db_name]
        except Exception as e:
            raise ConnectionError(
                f"MongoDB connection failed — uri={self._uri} — {e}"
            ) from e

    def disconnect(self) -> None:
        if self._client is None:
            return
        try:
            self._client.close()
        finally:
            self._client = None
            self._db = None

    def execute_query(self, query: str | dict, params: Any = None) -> list[dict]:
        if self._db is None:
            raise RuntimeError("Not connected. Call connect() first.")

        if not isinstance(query, dict):
            raise ValueError(
                f"MongoAdapter expects a dict query, got: {type(query).__name__}"
            )

        try:
            collection_name = query.get("collection")
            filter_dict     = query.get("filter", {})
            limit           = query.get("limit", 100)

            if not collection_name:
                raise ValueError("Query dict must have a 'collection' key.")

            cursor = self._db[collection_name].find(filter_dict).limit(limit)

            results = []
            for doc in cursor:
                clean_doc = {}
                for key, value in doc.items():
                    if isinstance(value, ObjectId):
                        clean_doc[key] = str(value)
                    else:
                        clean_doc[key] = value
                results.append(clean_doc)

            return results if results else []

        except ValueError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"MongoDB query failed: {e}\nQuery was: {str(query)[:100]}"
            ) from e

    def fetch_schema(self) -> dict:
        if self._db is None:
            raise RuntimeError("Not connected. Call connect() first.")
        from adapters.schema_inspector.mongo import inspect_mongo_schema
        return inspect_mongo_schema(self._db)

    def health_check(self) -> bool:
        try:
            self._client.admin.command("ping")
            return True
        except Exception:
            return False

    @property
    def db_type(self) -> str:
        return "mongo"