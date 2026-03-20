from contextlib import contextmanager
from typing import Generator
import PyMySQL
from core.config.settings import get_settings


class MySQLConnectionPool:
    """
    Simple connection pool for MySQL using DBUtils or a manual implementation.
    Dev 1 owns this file.

    Usage:
        pool = MySQLConnectionPool()
        with pool.get_connection() as conn:
            cursor = conn.cursor()
            ...
    """

    def __init__(self, pool_size: int = 5):
        # TODO (Dev 1): initialise pool
        # Option A — manual list of connections
        # Option B — use dbutils: pip install DBUtils
        #   from dbutils.pooled_db import PooledDB
        #   self._pool = PooledDB(PyMySQL, pool_size, **config)
        self._pool_size = pool_size
        self._pool = None
        raise NotImplementedError("Initialise the pool in __init__")

    @contextmanager
    def get_connection(self) -> Generator:
        """
        Context manager — yields a live connection, returns it to the pool on exit.

        with pool.get_connection() as conn:
            ...
        """
        # TODO (Dev 1):
        # conn = self._pool.connection()
        # try:
        #     yield conn
        # finally:
        #     conn.close()   # returns to pool, not actually closed
        raise NotImplementedError
