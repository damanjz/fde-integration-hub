"""Postgres helpers: async pool, and sync utilities for schema and test databases."""

from __future__ import annotations

from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "db" / "schema.sql"


async def open_pool(url: str, size: int = 20) -> AsyncConnectionPool:
    if not url:
        raise RuntimeError("HUB_DATABASE_URL is empty: copy .env.example to .env and set it")
    pool = AsyncConnectionPool(
        url,
        min_size=2,
        max_size=size,
        kwargs={"row_factory": dict_row},
        open=False,
        timeout=15,
    )
    await pool.open(wait=True, timeout=15)
    return pool


def swap_database(url: str, dbname: str) -> str:
    base, _, _ = url.rpartition("/")
    return f"{base}/{dbname}"


def ensure_database(url: str, dbname: str, *, recreate: bool = False) -> str:
    """Create (or recreate) a database next to the one in `url` and return its URL."""
    admin_url = swap_database(url, "postgres")
    with psycopg.connect(admin_url, autocommit=True, connect_timeout=3) as conn:
        if recreate:
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(dbname)))
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)).fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    return swap_database(url, dbname)


def apply_schema(url: str) -> None:
    with psycopg.connect(url, autocommit=True, connect_timeout=3) as conn:
        conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def reset_data(url: str) -> None:
    with psycopg.connect(url, autocommit=True, connect_timeout=3) as conn:
        conn.execute(
            "TRUNCATE delivery_audit, delivery_attempts, deliveries, events, destination_state, counters "
            "RESTART IDENTITY CASCADE"
        )
