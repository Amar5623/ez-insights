"""
core/interfaces/base_db_adapter.py
Lead owns this file — do not modify without team discussion.

THE most important interface for Dev 1.
Dev 1: read every docstring below before writing a single line in adapters/.

To add a new DB (e.g. PostgreSQL):
  1. Create adapters/postgres_adapter.py, subclass BaseDBAdapter
  2. Implement every @abstractmethod
  3. Register in core/factory/db_factory.py
  4. Add env vars to settings.py and .env.example

GOLDEN RULE for ALL devs:
  NEVER do:  from adapters.mysql_adapter import MySQLAdapter
  ALWAYS do: from core.interfaces import BaseDBAdapter
  The factory injects the right adapter — your code must not care which DB it is.
"""
from abc import ABC, abstractmethod
from typing import Any


class BaseDBAdapter(ABC):
    """
    Abstract base class for all database adapters.

    Implementations:
      - adapters/mysql_adapter.py   → MySQLAdapter   (Dev 1)
      - adapters/mongo_adapter.py   → MongoAdapter   (Dev 1)

    Lifecycle (managed by main.py at startup/shutdown):
        adapter = create_db_adapter()   # factory reads DB_TYPE from .env
        adapter.connect()               # open connection
        # ... application runs ...
        adapter.disconnect()            # clean shutdown

    All methods that return data MUST return list[dict].
    No raw cursors, no pymongo Cursor objects, no generator yields.
    The rest of the system expects plain Python dicts it can serialise to JSON.
    """

    @abstractmethod
    def connect(self) -> None:
        """
        Open the database connection (or initialise a connection pool).

        Called once at app startup by main.py lifespan.
        Must be idempotent — calling connect() twice must not raise.

        Dev 1 notes:
          MySQL : use PyMySQL.connect() or initialise a DBUtils PooledDB
          Mongo : use pymongo.MongoClient() — the client is already a pool

        Raises:
            ConnectionError: If the database cannot be reached.
                             Include host/port in the error message so it's
                             immediately obvious what failed.
        """
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """
        Close the connection cleanly.

        Called at app shutdown by main.py lifespan.
        Must not raise even if connect() was never called.

        Dev 1 notes:
          MySQL : cursor.close() then connection.close()
          Mongo : client.close()
        """
        ...

    @abstractmethod
    def execute_query(
        self, query: str | dict, params: Any = None
    ) -> list[dict]:
        """
        Execute a query and return all result rows as a list of dicts.

        This is the method strategies call to actually run queries against
        the database. The adapter hides all driver-level details.

        Args:
            query:
              For SQL adapters (MySQL):
                A SQL string, e.g. "SELECT * FROM products WHERE price > %s"
                May use %s placeholders for parameterised queries.
              For Mongo adapters (MongoDB):
                A filter dict, e.g. {"price": {"$gt": 50}}
                The collection name must be embedded in the filter or
                stored as adapter state — see MongoAdapter for the convention.

            params:
              For SQL: tuple/list of values substituted into %s placeholders.
                       None if no placeholders.
              For Mongo: unused — pass None.

        Returns:
            A list of dicts, one per row/document.
            Example: [{"id": 1, "name": "Widget", "price": 12.99}, ...]
            Returns [] (empty list) if the query matches no rows.
            NEVER returns None.

        CRITICAL rules for Dev 1:
          - Never return a raw cursor or pymongo Cursor — always list(cursor)
          - For MongoDB: convert every ObjectId field to str() before returning.
            JSON cannot serialise ObjectId and the API layer will crash.
          - Column names / field names are the dict keys (use cursor.description
            for MySQL, document.keys() for Mongo).

        Raises:
            ValueError:   If the generated query is syntactically invalid.
            RuntimeError: For unexpected driver-level errors (wrap the original
                          exception with context).
        """
        ...

    @abstractmethod
    def fetch_schema(self) -> dict:
        """
        Inspect the database and return a structured schema dict.

        Called by SchemaRetriever at startup to build the vector index.
        The schema tells the LLM what tables/collections and columns/fields exist.

        Returns for SQL (MySQL):
            {
                "products": [
                    {"column": "id",    "type": "int(11)",    "nullable": False},
                    {"column": "name",  "type": "varchar(255)","nullable": False},
                    {"column": "price", "type": "decimal(10,2)","nullable": True},
                ],
                "orders": [
                    {"column": "id",         "type": "int(11)", "nullable": False},
                    {"column": "product_id", "type": "int(11)", "nullable": False},
                ],
            }

        Returns for Mongo:
            {
                "products": [
                    {"field": "_id",   "inferred_type": "ObjectId"},
                    {"field": "name",  "inferred_type": "str"},
                    {"field": "price", "inferred_type": "float"},
                ],
                "orders": [
                    {"field": "_id",        "inferred_type": "ObjectId"},
                    {"field": "product_id", "inferred_type": "str"},
                ],
            }

        Dev 1 implementation notes:
          MySQL:
            - SHOW TABLES  → list of table names
            - For each table: DESCRIBE <table>  → columns
            - Map DESCRIBE columns: Field→column, Type→type, Null=="YES"→nullable

          Mongo:
            - db.list_collection_names() → collection names
            - For each collection: sample top 100 documents
            - Loop all sampled docs and collect UNIQUE field names + infer type
            - Use type(value).__name__ to infer type, handle ObjectId specially
            - Not every document has every field — union all field names seen

        Raises:
            RuntimeError: If schema inspection fails (e.g. no connection).
        """
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """
        Probe the database connection and return whether it is alive.

        Used by GET /api/health to report db_connected status.
        Must not raise — return False on any error.

        Dev 1 notes:
          MySQL : run "SELECT 1" and return True if it succeeds
          Mongo : run db.command("ping") and return True if it succeeds

        Returns:
            True  — connection is alive and query/command succeeded
            False — connection is dead, timed out, or raised any exception
        """
        ...

    @property
    @abstractmethod
    def db_type(self) -> str:
        """
        Human-readable DB type string.

        Returns:
            One of: 'mysql', 'mongo'
            Must match the DB_TYPE env var value for this adapter.
        """
        ...