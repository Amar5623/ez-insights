from typing import Any
import PyMySQL
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
            "cursorclass": PyMySQL.cursors.DictCursor,
            "autocommit": True,
        }

    def connect(self) -> None:
        # TODO (Dev 1): open PyMySQL connection using self._config
        raise NotImplementedError

    def disconnect(self) -> None:
        # TODO (Dev 1): close self._connection safely
        raise NotImplementedError

    def execute_query(self, query: str | dict, params: Any = None) -> list[dict]:
        # TODO (Dev 1):
        # - ensure connected
        # - create cursor, execute query with params
        # - fetchall() and return as list[dict]
        # - on error: raise with clear message including query snippet
        raise NotImplementedError

    def fetch_schema(self) -> dict:
        # TODO (Dev 1): delegate to schema_inspector/mysql.py
        from adapters.schema_inspector.mysql import inspect_mysql_schema
        return inspect_mysql_schema(self._connection)

    def health_check(self) -> bool:
        # TODO (Dev 1): ping the connection, return True/False
        raise NotImplementedError

    @property
    def db_type(self) -> str:
        return "mysql"
