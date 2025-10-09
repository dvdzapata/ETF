"""Database utilities for PostgreSQL access."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator, Sequence

import psycopg
from psycopg import conninfo as psycopg_conninfo
from psycopg.rows import dict_row

try:  # pragma: no cover - fallback exercised only when optional dependency missing
    from psycopg_pool import ConnectionPool as _PsycopgConnectionPool
except ModuleNotFoundError:  # pragma: no cover - keep runtime resilient without pool package
    _PsycopgConnectionPool = None

from .config import DatabaseSettings

logger = logging.getLogger(__name__)


class _DirectConnectionPool:
    """Very small substitute when psycopg_pool is unavailable."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    @contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        conn = psycopg.connect(self._dsn)
        try:
            yield conn
        finally:
            conn.close()

    def close(self) -> None:  # pragma: no cover - nothing to close explicitly
        return


class Database:
    def __init__(self, settings: DatabaseSettings, *, max_pool_size: int = 10) -> None:
        self.settings = settings
        self.max_pool_size = max_pool_size
        self._pool: object | None = None

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
        if _PsycopgConnectionPool is not None:
            self._pool = _PsycopgConnectionPool(dsn, min_size=1, max_size=self.max_pool_size)
        else:
            logger.warning(
                "psycopg_pool is not installed; using direct connections without pooling."
            )
            self._pool = _DirectConnectionPool(dsn)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    @contextmanager
    def cursor(self) -> Generator[psycopg.Cursor, None, None]:
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

    def execute_batch(self, query: str, params: Sequence[Sequence]) -> int:
        if not params:
            return 0
        with self.cursor() as cur:
            cur.executemany(query, params)
            rowcount = cur.rowcount if cur.rowcount not in (-1, None) else 0
        logger.debug("Executed batch query affecting %s rows", rowcount)
        return rowcount

    def fetchall(self, query: str, params: Sequence | None = None) -> list[dict]:
        with self.cursor() as cur:
            cur.execute(query, params or ())
            return list(cur.fetchall())

    def fetchone(self, query: str, params: Sequence | None = None) -> dict | None:
        with self.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchone()


__all__ = ["Database"]
