from typing import Any
import psycopg2
import psycopg2.extras
from core.interfaces import BaseDBAdapter
from core.config.settings import get_settings

class PostgreSQLAdapter(BaseDBAdapter):
    def __init__(self):
        self._connection = None
        s = get_settings()
        self._config = {
            "host": s.POSTGRES_HOST,
            "port": s.POSTGRES_PORT,
            "user": s.POSTGRES_USER,
            "password": s.POSTGRES_PASSWORD,
            "dbname": s.POSTGRES_DATABASE,
            "options": "-c search_path=public",
        }

    def connect(self) -> None:
        if self._connection is not None:
            return
        try:
            self._connection = psycopg2.connect(**self._config)
            self._connection.autocommit = True
            with self._connection.cursor() as cursor:
                cursor.execute("SET search_path TO public;")
        except psycopg2.Error as e:
            raise ConnectionError(f"PostgreSQL connection failed: {e}") from e

    def disconnect(self) -> None:
        if self._connection is None:
            return
        try:
            self._connection.close()
        finally:
            self._connection = None

    def execute_query(self, query: str | dict, params: Any = None) -> list[dict]:
        if self._connection is None:
            raise RuntimeError("Not connected. Call connect() first.")
        try:
            with self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(row) for row in results] if results else []
        except psycopg2.ProgrammingError as e:
            raise ValueError(f"Invalid query syntax: {e}\nQuery: {str(query)[:100]}") from e
        except psycopg2.Error as e:
            raise RuntimeError(f"Query execution failed: {e}\nQuery: {str(query)[:100]}") from e

    def fetch_schema(self) -> dict:
        if self._connection is None:
            raise RuntimeError("Not connected. Call connect() first.")
        from adapters.schema_inspector.postgres import inspect_postgres_schema
        return inspect_postgres_schema(self._connection)

    def health_check(self) -> bool:
        try:
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except Exception:
            return False

    @property
    def db_type(self) -> str:
        return "postgres"   