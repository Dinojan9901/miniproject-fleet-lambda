"""PostgreSQL access shared by the speed layer, the batch layer and the API.

Writes go through `upsert`, which is a single `INSERT ... ON CONFLICT DO UPDATE`
executed with `execute_values`. That matters for correctness, not just speed:
both Spark layers deliver **at-least-once**, so the same micro-batch can be
replayed after a failure. Making every write idempotent on its primary key is
what turns at-least-once delivery into an at-most-once *effect* in the serving
store.
"""
from __future__ import annotations

import contextlib
import time
from typing import Iterable, Sequence

import psycopg2
from psycopg2.extras import execute_values

from .config import SETTINGS


def connect(retries: int = 30, delay: float = 2.0):
    """Open a connection, tolerating Postgres still starting up."""
    last: Exception | None = None
    for _ in range(retries):
        try:
            return psycopg2.connect(**SETTINGS.psycopg2_kwargs())
        except psycopg2.OperationalError as exc:
            last = exc
            time.sleep(delay)
    raise RuntimeError(f"postgres unreachable after {retries} attempts: {last}")


@contextlib.contextmanager
def cursor(conn=None):
    """Yield a cursor, committing on success and rolling back on failure."""
    own = conn is None
    conn = conn or connect()
    try:
        with conn:
            with conn.cursor() as cur:
                yield cur
    finally:
        if own:
            conn.close()


def upsert(
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence],
    conflict_columns: Sequence[str],
    update_columns: Sequence[str] | None = None,
    where: str | None = None,
    conn=None,
) -> int:
    """Idempotent bulk write. Returns the number of rows sent."""
    rows = list(rows)
    if not rows:
        return 0

    update_columns = list(update_columns if update_columns is not None else
                          [c for c in columns if c not in conflict_columns])
    if update_columns:
        assignments = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
        action = f"DO UPDATE SET {assignments}" + (f" WHERE {where}" if where else "")
    else:
        action = "DO NOTHING"

    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_columns)}) {action}"
    )
    with cursor(conn) as cur:
        execute_values(cur, sql, rows)
    return len(rows)


def query(sql: str, params: Sequence | None = None, conn=None) -> list[tuple]:
    with cursor(conn) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def query_dicts(sql: str, params: Sequence | None = None, conn=None) -> list[dict]:
    with cursor(conn) as cur:
        cur.execute(sql, params)
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]
