"""Database utilities for PostgreSQL access."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterable, Sequence

import psycopg
from psycopg import conninfo as psycopg_conninfo
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import DatabaseSettings

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, settings: DatabaseSettings, *, max_pool_size: int = 10) -> None:
        self.settings = settings
        self.max_pool_size = max_pool_size
        self._pool: ConnectionPool | None = None

    def connect(self) -> None:
        if self._pool is not None:
            return
        conninfo = {
            "host": self.settings.host,
            "port": self.settings.port,
            "user": self.settings.user,
            "password": self.settings.password,
            "dbname": self.settings.dbname,
        }
        if self.settings.sslmode:
            conninfo["sslmode"] = self.settings.sslmode
        logger.debug(
            "Creating connection pool for %s@%s:%s/%s",
            self.settings.user,
            self.settings.host,
            self.settings.port,
            self.settings.dbname,
        )
        dsn = psycopg_conninfo.make_conninfo(**conninfo)
        self._pool = ConnectionPool(dsn, min_size=1, max_size=self.max_pool_size)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    @contextmanager
    def cursor(self) -> Iterable[psycopg.Cursor]:
        if self._pool is None:
            self.connect()
        assert self._pool is not None
        with self._pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                try:
                    yield cur
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

    def execute_batch(self, query: str, params: Sequence[Sequence]) -> None:
        if not params:
            return
        with self.cursor() as cur:
            cur.executemany(query, params)

    def fetchall(self, query: str, params: Sequence | None = None) -> list[dict]:
        with self.cursor() as cur:
            cur.execute(query, params or ())
            return list(cur.fetchall())

    def fetchone(self, query: str, params: Sequence | None = None) -> dict | None:
        with self.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchone()


__all__ = ["Database"]
