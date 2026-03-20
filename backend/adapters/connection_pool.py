from contextlib import contextmanager
from typing import Generator
import pymysql                          # lowercase — same fix as mysql_adapter
from dbutils.pooled_db import PooledDB  # DBUtils already in requirements.txt
from core.config.settings import get_settings


class MySQLConnectionPool:
    """
    Simple connection pool for MySQL using DBUtils.
    Dev 1 owns this file.

    Usage:
        pool = MySQLConnectionPool()
        with pool.get_connection() as conn:
            cursor = conn.cursor()
            ...
    """

    def __init__(self, pool_size: int = 5):
        s = get_settings()

        # config dict — same fields as MySQLAdapter
        self._config = {
            "host":     s.MYSQL_HOST,
            "port":     s.MYSQL_PORT,
            "user":     s.MYSQL_USER,
            "password": s.MYSQL_PASSWORD,
            "database": s.MYSQL_DATABASE,
            "cursorclass": pymysql.cursors.DictCursor,
            "autocommit": True,
        }

        self._pool_size = pool_size

        # create the pool — DBUtils manages all connections inside
        self._pool = PooledDB(
            creator=pymysql,        # which library to use
            maxconnections=pool_size,  # max connections allowed at once
            mincached=2,            # keep at least 2 connections ready at all times
            maxcached=pool_size,    # max idle connections to keep in pool
            blocking=True,          # if pool is full — wait, don't crash
            **self._config          # unpack all connection settings
        )

    @contextmanager
    def get_connection(self) -> Generator:
        """
        Context manager — borrows a connection from pool, returns it on exit.

        with pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM books")
        # connection automatically returned to pool here
        """
        conn = self._pool.connection()  # borrow a connection from pool
        try:
            yield conn                  # hand it to whoever called this
        finally:
            conn.close()               # return it to pool — NOT actually closed