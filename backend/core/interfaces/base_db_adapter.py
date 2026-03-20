from abc import ABC, abstractmethod
from typing import Any


class BaseDBAdapter(ABC):
    """
    Abstract base class for all database adapters.
    Dev 1 implements this for MySQL and MongoDB.
    Never import a concrete adapter directly — use db_factory.py.
    """

    @abstractmethod
    def connect(self) -> None:
        """Open the database connection."""
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection cleanly."""
        ...

    @abstractmethod
    def execute_query(
        self, query: str | dict, params: Any = None
    ) -> list[dict]:
        """
        Execute a query and return rows as a list of dicts.

        For SQL adapters: query is a SQL string, params are substitution values.
        For Mongo adapters: query is a filter dict, params are unused.
        """
        ...

    @abstractmethod
    def fetch_schema(self) -> dict:
        """
        Return the full database schema as a structured dict.

        SQL:   { table_name: [ {column, type, nullable} ] }
        Mongo: { collection_name: [ {field, inferred_type} ] }
        """
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """Return True if the connection is alive, False otherwise."""
        ...

    @property
    @abstractmethod
    def db_type(self) -> str:
        """Human-readable DB type e.g. 'mysql', 'mongo'."""
        ...
