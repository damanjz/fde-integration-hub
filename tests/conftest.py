"""Shared fixtures: an isolated test database and async support on the selector event loop."""

from __future__ import annotations

import asyncio
import os

import psycopg
import pytest

from hub.config import Settings
from hub.db import apply_schema, ensure_database, open_pool, reset_data
from hub.logging_ import set_known_secrets

BASE_URL = os.environ.get("HUB_TEST_DATABASE_URL") or Settings().database_url  # from .env
TEST_DB = "hub_test"


@pytest.fixture(scope="session")
def anyio_backend():
    # psycopg's async API cannot run on the Windows proactor loop.
    return ("asyncio", {"loop_factory": asyncio.SelectorEventLoop})


@pytest.fixture(scope="session")
def test_db_url():
    """A throwaway database next to the compose one; DB tests skip if Postgres is not running."""
    if not BASE_URL:
        pytest.skip("HUB_DATABASE_URL is not set (copy .env.example to .env)")
    try:
        url = ensure_database(BASE_URL, TEST_DB, recreate=True)
        apply_schema(url)
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable (docker compose up -d db): {exc}")
    return url


@pytest.fixture
def settings(test_db_url) -> Settings:
    s = Settings(
        _env_file=None,
        database_url=test_db_url,
        webhook_secrets="current-secret,previous-secret",
        admin_key="admin-key-for-tests",
        workers=0,
        backoff_base_s=0.01,
        backoff_cap_s=0.05,
        max_attempts=4,
        lease_s=5.0,
        poll_interval_s=0.01,
        breaker_threshold=50,
        ambiguous_settle_s=0.0,
    )
    set_known_secrets(s.all_secret_values())
    return s


@pytest.fixture
async def pool(test_db_url):
    reset_data(test_db_url)
    p = await open_pool(test_db_url, 10)
    yield p
    await p.close()
