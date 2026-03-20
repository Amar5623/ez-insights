from typing import Any
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
        # TODO (Dev 1):
        # from pymongo import MongoClient
        # self._client = MongoClient(self._uri)
        # self._db = self._client[self._db_name]
        raise NotImplementedError

    def disconnect(self) -> None:
        # TODO (Dev 1): self._client.close() safely
        raise NotImplementedError

    def execute_query(self, query: str | dict, params: Any = None) -> list[dict]:
        # TODO (Dev 1):
        # query here is a dict like:
        #   {"collection": "products", "filter": {"price": {"$gt": 20}}, "limit": 20}
        # - extract collection name + filter from query dict
        # - run self._db[collection].find(filter).limit(n)
        # - return list of dicts (convert ObjectId to str)
        raise NotImplementedError

    def fetch_schema(self) -> dict:
        # TODO (Dev 1): delegate to schema_inspector/mongo.py
        from adapters.schema_inspector.mongo import inspect_mongo_schema
        return inspect_mongo_schema(self._db)

    def health_check(self) -> bool:
        # TODO (Dev 1): self._client.admin.command("ping")
        raise NotImplementedError

    @property
    def db_type(self) -> str:
        return "mongo"
