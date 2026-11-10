from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest

from hub.retry import Outcome, backoff_delay, classify_exception, classify_status, parse_retry_after


@pytest.mark.parametrize("status", [200, 201, 202, 204])
def test_2xx_is_success(status):
    assert classify_status(status) is Outcome.SUCCESS


def test_429_is_throttled():
    assert classify_status(429) is Outcome.THROTTLED


def test_401_is_auth():
    assert classify_status(401) is Outcome.AUTH


@pytest.mark.parametrize("status", [500, 502, 503, 504, 599])
def test_5xx_is_retry(status):
    assert classify_status(status) is Outcome.RETRY


@pytest.mark.parametrize("status", [400, 403, 404, 409, 410, 422, 301])
def test_other_non_2xx_is_permanent(status):
    assert classify_status(status) is Outcome.PERMANENT


def test_exceptions_before_the_request_left_are_not_ambiguous():
    req = httpx.Request("POST", "http://x")
    assert classify_exception(httpx.ConnectError("x", request=req)) == ("connect_error", False)
    assert classify_exception(httpx.ConnectTimeout("x", request=req)) == ("connect_timeout", False)
    assert classify_exception(httpx.PoolTimeout("x", request=req)) == ("pool_timeout", False)


def test_exceptions_after_the_request_left_are_ambiguous():
    req = httpx.Request("POST", "http://x")
    assert classify_exception(httpx.ReadTimeout("x", request=req)) == ("timeout", True)
    assert classify_exception(httpx.RemoteProtocolError("x", request=req)) == ("protocol_error", True)
    assert classify_exception(httpx.ReadError("x", request=req)) == ("network_error", True)


def test_backoff_schedule_bounds_and_cap():
    rng = random.Random(1)
    base, cap = 1.0, 30.0
    expected_max = [1, 2, 4, 8, 16, 30, 30, 30]
    for failures, exp in enumerate(expected_max, start=1):
        for _ in range(200):
            d = backoff_delay(failures, base, cap, rng)
            assert exp / 2 <= d <= exp


def test_backoff_has_jitter_and_is_reproducible():
    a = [backoff_delay(3, 1.0, 30.0, random.Random(5)) for _ in range(3)]
    assert len(set(a)) == 1  # same seed, same value
    spread = {round(backoff_delay(3, 1.0, 30.0, random.Random(s)), 3) for s in range(20)}
    assert len(spread) > 10


def test_backoff_never_zero_and_grows_in_floor():
    floors = [backoff_delay(n, 1.0, 1000.0, random.Random(0)) for n in range(1, 8)]
    assert min(floors) > 0
    assert 2**6 / 2 <= floors[-1]


def test_retry_after_delta_seconds():
    assert parse_retry_after("1") == 1.0
    assert parse_retry_after(" 2.5 ") == 2.5
    assert parse_retry_after("0") == 0.0


def test_retry_after_http_date():
    now = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
    header = format_datetime(now + timedelta(seconds=7), usegmt=True)
    assert parse_retry_after(header, now=now) == pytest.approx(7.0)
    past = format_datetime(now - timedelta(seconds=30), usegmt=True)
    assert parse_retry_after(past, now=now) == 0.0


def test_retry_after_is_clamped_and_garbage_ignored():
    assert parse_retry_after("86400", cap=120) == 120
    assert parse_retry_after("-5") == 0.0
    assert parse_retry_after("soon") is None
    assert parse_retry_after(None) is None
    assert parse_retry_after("") is None
