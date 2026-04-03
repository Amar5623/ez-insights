"""
adapters/mongo_adapter.py
Dev 1 owns this file.
"""

from typing import Any
from bson import ObjectId

from core.interfaces import BaseDBAdapter
from core.config.settings import get_settings


class MongoAdapter(BaseDBAdapter):

    def __init__(self):
        self._client = None
        self._db = None
        s = get_settings()
        self._uri = s.MONGO_URI
        self._db_name = s.MONGO_DATABASE

        # ── Lifecycle ─────────────────────────────────────────

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

    # ── Query execution ───────────────────────────────────

    def execute_query(self, query: str | dict, params: Any = None) -> list[dict]:
        if self._db is None:
            raise RuntimeError("MongoAdapter: not connected. Call connect() first.")

        if not isinstance(query, dict):
            raise ValueError(
                f"MongoAdapter expects dict, got {type(query).__name__}"
            )

        try:
            collection_name = query.get("collection")
            limit = int(query.get("limit", 100))
            skip = int(query.get("skip", 0))
            projection = query.get("projection")
            sort = query.get("sort")

            if not collection_name:
                raise ValueError("Missing 'collection' in query")

            collection = self._db[collection_name]

            # ── Aggregation ──
            if "pipeline" in query:
                return self._run_aggregation(
                    collection,
                    query["pipeline"],
                    limit,
                    skip
                )

            # ── Find ──
            return self._run_find(
                collection,
                query.get("filter", {}),
                limit,
                skip,
                projection,
                sort
            )

        except ValueError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"MongoDB query failed\nQuery: {query}\nError: {e}"
            ) from e

    # ── FIND ─────────────────────────────────────────────

    def _run_find(
        self,
        collection,
        filter_dict: dict,
        limit: int,
        skip: int,
        projection: dict | None,
        sort: list | None
    ) -> list[dict]:

        if not isinstance(filter_dict, dict):
            raise ValueError("'filter' must be dict")

        cursor = collection.find(filter_dict, projection)

        if sort:
            cursor = cursor.sort(sort)

        if skip:
            cursor = cursor.skip(skip)

        if limit:
            cursor = cursor.limit(limit)

        return self._cursor_to_list(cursor)

    # ── AGGREGATION ──────────────────────────────────────

    def _run_aggregation(
        self,
        collection,
        pipeline: list,
        limit: int,
        skip: int
    ) -> list[dict]:

        if not isinstance(pipeline, list):
            raise ValueError("'pipeline' must be list")

        pipeline = list(pipeline)

        has_limit = any("$limit" in stage for stage in pipeline if isinstance(stage, dict))
        has_skip  = any("$skip"  in stage for stage in pipeline if isinstance(stage, dict))

        if skip > 0 and not has_skip:
            pipeline.append({"$skip": skip})

        if limit > 0 and not has_limit:
            pipeline.append({"$limit": limit})

        cursor = collection.aggregate(pipeline)
        return self._cursor_to_list(cursor)

    # ── OBJECTID SAFE CONVERSION ─────────────────────────

    def _serialize(self, value):
        """Recursively convert ObjectId → str"""
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, list):
            return [self._serialize(v) for v in value]
        if isinstance(value, dict):
            return {k: self._serialize(v) for k, v in value.items()}
        return value

    def _cursor_to_list(self, cursor) -> list[dict]:
        return [self._serialize(doc) for doc in cursor]

    # ── Schema ───────────────────────────────────────────

    def fetch_schema(self) -> dict:
        if self._db is None:
            raise RuntimeError("MongoAdapter not connected")
        from adapters.schema_inspector.mongo import inspect_mongo_schema
        return inspect_mongo_schema(self._db)

    # ── Health ───────────────────────────────────────────

    def health_check(self) -> bool:
        if self._client is None:
            return False
        try:
            self._client.admin.command("ping")
            return True
        except Exception:
            return False

    # ── Type ─────────────────────────────────────────────

    @property
    def db_type(self) -> str:
        return "mongo"