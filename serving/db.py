"""Pooled read access for the API.

The API is the only component that serves concurrent requests, so it is the only
one that needs a pool: opening a fresh Postgres connection per HTTP request adds
tens of milliseconds and, under a dashboard refreshing every few seconds, would
churn connections for no reason.
"""
from __future__ import annotations

import threading
from typing import Any, Sequence

from psycopg2 import pool as pg_pool

from common.config import SETTINGS

_pool: pg_pool.SimpleConnectionPool | None = None
_lock = threading.Lock()


def get_pool() -> pg_pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        with _lock:
            if _pool is None:
                _pool = pg_pool.SimpleConnectionPool(1, 8, **SETTINGS.psycopg2_kwargs())
    return _pool


def fetch(sql: str, params: Sequence[Any] | None = None) -> list[dict]:
    """Run a read query and return rows as dictionaries."""
    conn = get_pool().getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return []
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]
    finally:
        # Reads leave an idle transaction open otherwise, which pins WAL.
        conn.rollback()
        get_pool().putconn(conn)


def fetch_one(sql: str, params: Sequence[Any] | None = None) -> dict | None:
    rows = fetch(sql, params)
    return rows[0] if rows else None


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
