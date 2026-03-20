from typing import Any
import pymysql   
from core.interfaces import BaseDBAdapter
from core.config.settings import get_settings


class MySQLAdapter(BaseDBAdapter):
    """
    MySQL implementation of BaseDBAdapter using PyMySQL.
    Dev 1 owns this file.

    DO NOT import this directly anywhere — use db_factory.create_db_adapter()
    """

    def __init__(self):
        self._connection = None
        s = get_settings()
        self._config = {
            "host": s.MYSQL_HOST,
            "port": s.MYSQL_PORT,
            "user": s.MYSQL_USER,
            "password": s.MYSQL_PASSWORD,
            "database": s.MYSQL_DATABASE,
            "cursorclass": pymysql.cursors.DictCursor,
            "autocommit": True,
        }

    def connect(self) -> None:
     if self._connection is not None:        # idempotent — don't reconnect if already open
        return
     try:
        self._connection = pymysql.connect(**self._config)
     except pymysql.Error as e:
        raise ConnectionError(
            f"MySQL connection failed — host={self._config['host']} "
            f"port={self._config['port']} — {e}"
        ) from e

    def disconnect(self) -> None:
     if self._connection is None:    # must not raise if connect() was never called
        return
     try:
        self._connection.close()
     finally:
        self._connection = None     # always reset to None even if close() fails

    def execute_query(self, query: str | dict, params: Any = None) -> list[dict]:
        if self._connection is None:
            raise RuntimeError("Not connected. Call connect() first.")
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                return results if results else []   # NEVER return None
        except pymysql.err.ProgrammingError as e:
            raise ValueError(
                f"Invalid query syntax: {e}\nQuery was: {str(query)[:100]}"
            ) from e
        except pymysql.Error as e:
            raise RuntimeError(
                f"Query execution failed: {e}\nQuery was: {str(query)[:100]}"
            ) from e

    def fetch_schema(self) -> dict:
        if self._connection is None:
            raise RuntimeError("Not connected. Call connect() first.")
        from adapters.schema_inspector.mysql import inspect_mysql_schema
        return inspect_mysql_schema(self._connection)

    def health_check(self) -> bool:
       try:
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
       except Exception:
            return False
    @property
    def db_type(self) -> str:
        return "mysql"
