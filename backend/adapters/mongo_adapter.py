"""
adapters/mongo_adapter.py
Dev 1 owns this file.

MongoDB implementation of BaseDBAdapter using pymongo.

DO NOT import this directly anywhere — use db_factory.create_db_adapter()

──────────────────────────────────────────────────────────────────────────────
MONGO-SPECIFIC: execute_query() format
──────────────────────────────────────────────────────────────────────────────
Unlike MySQL which takes a SQL string, MongoAdapter takes a dict produced by
the LLM (after JSON parsing in query_service._parse_mongo_json).

The dict must always have a "collection" key, then either:

  Simple find (filtering, listing):
    {
      "collection": "books",
      "filter"    : {"genre": "sci-fi"},   ← standard pymongo filter
      "limit"     : 20
    }

  Aggregation pipeline (count, average, group, sum, etc.):
    {
      "collection": "sales",
      "pipeline"  : [                      ← list of aggregation stages
          {"$match":  {"status": "paid"}},
          {"$group":  {"_id": null, "total": {"$sum": "$amount"}}},
          {"$project":{"_id": 0, "total": 1}}
      ],
      "limit"     : 5
    }

The "filter" vs "pipeline" distinction is resolved inside execute_query().
All other DB-adapter code (connect, disconnect, fetch_schema, health_check)
is identical in structure to any other BaseDBAdapter implementation and has
no MongoDB-specific quirks.
──────────────────────────────────────────────────────────────────────────────
"""

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

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Open the MongoDB connection. Idempotent — safe to call twice."""
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
        """Close the MongoDB connection. Safe to call even if never connected."""
        if self._client is None:
            return
        try:
            self._client.close()
        finally:
            self._client = None
            self._db = None

    # ── Query execution ───────────────────────────────────────────────────────

    def execute_query(self, query: str | dict, params: Any = None) -> list[dict]:
        """
        Execute a MongoDB query and return results as a list of plain dicts.

        ─────────────────────────────────────────────────────────────────────
        MONGO-SPECIFIC: accepts a dict, not a SQL string
        ─────────────────────────────────────────────────────────────────────

        Args:
            query:  A dict with the following required key:
                      "collection" (str) — the collection to query

                    And ONE of these mutually-exclusive query keys:

                    "filter" path  — simple find (filtering / listing):
                      "filter" (dict) — standard pymongo filter dict
                                        e.g. {"price": {"$gt": 50}}
                      "limit" (int)   — max documents to return (default 100)

                    "pipeline" path — aggregation (count, sum, avg, group):
                      "pipeline" (list) — list of MongoDB aggregation stages
                                          e.g. [{"$group": ...}, {"$project": ...}]
                      "limit" (int)     — appended as a {"$limit": N} stage
                                          unless the pipeline already has one

            params: Unused for MongoDB — pass None (kept for interface compat).

        Returns:
            list[dict] — one dict per document/result row.
            All ObjectId values are converted to str (JSON-safe).
            Returns [] when no documents match — never returns None.

        Raises:
            ValueError:   If query is not a dict, missing 'collection', or
                          'pipeline' is not a list.
            RuntimeError: For pymongo-level errors (wrapped with context).
        """
        # ── Guard: must be connected ──────────────────────────────────────────
        if self._db is None:
            raise RuntimeError(
                "MongoAdapter: not connected. Call connect() first."
            )

        # ── Type check: Mongo only accepts dicts, not SQL strings ─────────────
        if not isinstance(query, dict):
            raise ValueError(
                f"MongoAdapter expects a dict query, got: {type(query).__name__}. "
                "MySQL SQL strings are not valid here — check DB_TYPE in .env."
            )

        try:
            collection_name = query.get("collection")
            limit = int(query.get("limit", 100))

            if not collection_name:
                raise ValueError(
                    "MongoDB query dict must have a 'collection' key.\n"
                    f"Received keys: {list(query.keys())}"
                )

            collection = self._db[collection_name]

            # ── MONGO: Pipeline path (aggregation — count, sum, avg, group, etc.)
            if "pipeline" in query:
                return self._run_aggregation(collection, query["pipeline"], limit)

            # ── MONGO: Filter path (simple find — listing, filtering)
            return self._run_find(collection, query.get("filter", {}), limit)

        except ValueError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"MongoDB query failed.\n"
                f"Query  : {str(query)[:200]}\n"
                f"Error  : {e}"
            ) from e

    def _run_find(self, collection, filter_dict: dict, limit: int) -> list[dict]:
        """
        MONGO-SPECIFIC: Execute a simple find query.

        Used when the LLM returns {"collection": ..., "filter": ..., "limit": ...}.
        Covers filtering, listing, and fetching documents by field values.
        """
        if not isinstance(filter_dict, dict):
            raise ValueError(
                f"'filter' must be a dict, got {type(filter_dict).__name__}."
            )

        cursor = collection.find(filter_dict).limit(limit)
        return self._cursor_to_list(cursor)

    def _run_aggregation(
        self, collection, pipeline: list, limit: int
    ) -> list[dict]:
        """
        MONGO-SPECIFIC: Execute an aggregation pipeline.

        Used when the LLM returns {"collection": ..., "pipeline": [...], "limit": ...}.
        Covers count, sum, average, grouping, and all aggregation queries.

        A {"$limit": N} stage is appended to the pipeline automatically unless
        the pipeline already contains one — prevents returning huge result sets
        from accidental aggregations.
        """
        if not isinstance(pipeline, list):
            raise ValueError(
                f"'pipeline' must be a list of aggregation stages, "
                f"got {type(pipeline).__name__}."
            )

        # Append $limit only if not already present in the pipeline
        has_limit_stage = any(
            isinstance(stage, dict) and "$limit" in stage
            for stage in pipeline
        )
        if not has_limit_stage and limit > 0:
            pipeline = list(pipeline) + [{"$limit": limit}]

        cursor = collection.aggregate(pipeline)
        return self._cursor_to_list(cursor)

    def _cursor_to_list(self, cursor) -> list[dict]:
        """
        MONGO-SPECIFIC: Drain a pymongo cursor to a plain list of dicts.

        Converts every ObjectId field to str — JSON cannot serialise ObjectId
        and the API layer will crash without this conversion.

        Returns [] (not None) when the cursor is empty.
        """
        results = []
        for doc in cursor:
            clean_doc = {}
            for key, value in doc.items():
                clean_doc[key] = str(value) if isinstance(value, ObjectId) else value
            results.append(clean_doc)
        return results

    # ── Schema inspection ─────────────────────────────────────────────────────

    def fetch_schema(self) -> dict:
        """
        MONGO-SPECIFIC: Sample documents to infer the collection schema.

        Delegates to adapters/schema_inspector/mongo.py which samples
        the top N documents per collection and infers field types.

        Returns:
            {
                "collection_name": [
                    {"field": "price", "inferred_type": "float"},
                    ...
                ],
                ...
            }
        """
        if self._db is None:
            raise RuntimeError(
                "MongoAdapter: not connected. Call connect() first."
            )
        from adapters.schema_inspector.mongo import inspect_mongo_schema
        return inspect_mongo_schema(self._db)

    # ── Health check ──────────────────────────────────────────────────────────

    def health_check(self) -> bool:
        """
        Ping MongoDB and return True if the connection is alive.
        Returns False on any error — never raises.
        """
        try:
            self._client.admin.command("ping")
            return True
        except Exception:
            return False

    # ── DB type identifier ────────────────────────────────────────────────────

    @property
    def db_type(self) -> str:
        """Return 'mongo' — used by strategies and query_service to branch logic."""
        return "mongo"