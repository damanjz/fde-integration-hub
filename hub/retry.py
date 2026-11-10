"""Retry policy as pure functions: classification, backoff schedule, Retry-After parsing."""

from __future__ import annotations

import random
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from enum import StrEnum

import httpx

# Error classes where no response came back, so the destination may still be working on the request.
NO_RESPONSE_CLASSES = frozenset({"timeout", "protocol_error", "network_error", "attempt_timeout"})


class Outcome(StrEnum):
    SUCCESS = "success"
    RETRY = "retry"  # 5xx, timeout, connection error: try again with backoff
    THROTTLED = "throttled"  # 429: try again after Retry-After, not counted against the retry budget
    AUTH = "auth"  # 401: refresh the credential once, then treat as RETRY
    PERMANENT = "permanent"  # any other 4xx (or odd status): dead-letter now


def classify_status(status: int) -> Outcome:
    """Spec rule: retry 429 and 5xx; 401 gets a token refresh; every other non-2xx is permanent."""
    if 200 <= status < 300:
        return Outcome.SUCCESS
    if status == 429:
        return Outcome.THROTTLED
    if status == 401:
        return Outcome.AUTH
    if status >= 500:
        return Outcome.RETRY
    return Outcome.PERMANENT


def classify_exception(exc: BaseException) -> tuple[str, bool]:
    """Return (error_class, request_may_have_been_processed).

    A failed connect or an exhausted pool means no request left the building, so nothing needs
    reconciling. Anything after the request was written (read timeout, reset) is ambiguous.
    """
    if isinstance(exc, httpx.ConnectTimeout):
        return "connect_timeout", False
    if isinstance(exc, httpx.ConnectError):
        return "connect_error", False
    if isinstance(exc, httpx.PoolTimeout):
        return "pool_timeout", False
    if isinstance(exc, httpx.TimeoutException):
        return "timeout", True
    if isinstance(exc, httpx.RemoteProtocolError):
        return "protocol_error", True
    if isinstance(exc, httpx.TransportError):
        return "network_error", True
    return exc.__class__.__name__, True


def backoff_delay(failures: int, base: float, cap: float, rng: random.Random | None = None) -> float:
    """Exponential backoff with equal jitter: half fixed, half random.

    failures is the number of failed attempts so far (>= 1). The result is in [exp/2, exp] where
    exp = min(cap, base * 2**(failures-1)): jitter spreads retries, the floor keeps them from
    collapsing to zero and hammering a struggling destination.
    """
    exp = min(cap, base * (2 ** max(0, failures - 1)))
    r = rng or random
    return exp / 2 + r.random() * exp / 2


def parse_retry_after(value: str | None, *, now: datetime | None = None, cap: float = 120.0) -> float | None:
    """Seconds to wait from a Retry-After header (delta-seconds or HTTP date), clamped to [0, cap]."""
    if not value:
        return None
    value = value.strip()
    try:
        seconds = float(value)
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = (when - (now or datetime.now(UTC))).total_seconds()
    return max(0.0, min(cap, seconds))
