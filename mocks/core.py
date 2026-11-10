"""Shared mock machinery: fault configuration, per-provider state, the gateway checks, OAuth tokens."""

from __future__ import annotations

import asyncio
import json
import os
import random
import secrets
import time
from collections import Counter, deque
from urllib.parse import parse_qs

from fastapi import Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

PROVIDERS = ("storefront", "warehouse", "helpdesk", "accounting")


class Faults(BaseModel):
    error_rate: float = 0.0  # share of requests answered 500/502/503 before any processing
    latency_ms: int = 0  # added to every request
    latency_jitter_ms: int = 0  # plus uniform(0, jitter)
    outage: bool = False  # provider is down: 503 for everything
    lost_response_rate: float = 0.0  # request is processed and recorded, the caller gets a 502
    accept_then_timeout_rate: float = 0.0  # committed, then the answer comes after accept_timeout_delay_s
    accept_timeout_delay_s: float = 5.0  # must exceed the hub's client timeout for the hub to see a timeout
    slow_commit_rate: float = 0.0  # POSTs processed slowly BEFORE they commit ...
    slow_commit_delay_s: float = 12.0  # ... so the caller times out while the request is still in progress
    rate_limit_per_sec: float = 0.0  # 0 = unlimited; sliding one-second window, 429 + Retry-After
    token_ttl_s: int = 300


def default_faults() -> dict[str, Faults]:
    faults = {name: Faults() for name in PROVIDERS}
    faults["warehouse"] = Faults(rate_limit_per_sec=5, token_ttl_s=60)
    for name, raw in json.loads(os.environ.get("MOCK_FAULTS", "{}") or "{}").items():
        faults[name] = faults[name].model_copy(update=raw)
    return faults


CREDENTIALS = {
    "warehouse": (
        os.environ.get("MOCK_WAREHOUSE_CLIENT_ID", "hub-warehouse"),
        os.environ.get("MOCK_WAREHOUSE_CLIENT_SECRET", "dev-warehouse-secret"),
    ),
    "accounting": (
        os.environ.get("MOCK_ACCOUNTING_CLIENT_ID", "hub-accounting"),
        os.environ.get("MOCK_ACCOUNTING_CLIENT_SECRET", "dev-accounting-secret"),
    ),
}
HELPDESK_API_KEY = os.environ.get("MOCK_HELPDESK_API_KEY", "dev-helpdesk-key")
IDEMPOTENCY_TTL_S = 24 * 3600


class State:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.faults = default_faults()
        self.rng = random.Random(int(os.environ.get("MOCK_SEED", "7")))
        self.tokens: dict[str, dict[str, float]] = {"warehouse": {}, "accounting": {}}
        self.windows: dict[str, deque[float]] = {name: deque() for name in PROVIDERS}
        self.received: dict[str, Counter] = {name: Counter() for name in PROVIDERS}
        self.received_log: dict[str, list[dict]] = {name: [] for name in PROVIDERS}
        self.arrivals = 0  # global commit counter: gives a total order of accepted requests
        self.idempotency: dict[str, tuple[float, int, dict]] = {}  # helpdesk: key -> (expiry, status, body)
        self.stats: dict[str, Counter] = {name: Counter() for name in PROVIDERS}


STATE = State()


def _json(status: int, body: dict, headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse(body, status_code=status, headers=headers)


def check_oauth(provider: str, request: Request) -> bool:
    header = request.headers.get("authorization", "")
    if not header.lower().startswith("bearer "):
        return False
    expiry = STATE.tokens[provider].get(header[7:])
    return expiry is not None and expiry > time.time()


async def gateway(
    provider: str, request: Request, *, auth: str | None, token_endpoint: bool = False
) -> Response | None:
    """Everything a real provider does before the business logic. Returns a response to stop, else None."""
    faults, stats = STATE.faults[provider], STATE.stats[provider]
    stats["requests"] += 1
    if faults.outage:
        stats["outage_503"] += 1
        return _json(503, {"error": "service unavailable"})
    if auth == "oauth" and not check_oauth(provider, request):
        stats["unauthorized_401"] += 1
        return _json(401, {"error": "invalid_token"})
    if auth == "apikey" and request.headers.get("x-api-key") != HELPDESK_API_KEY:
        stats["unauthorized_401"] += 1
        return _json(401, {"error": "invalid_api_key"})
    if faults.rate_limit_per_sec > 0 and not token_endpoint:
        window, now = STATE.windows[provider], time.monotonic()
        while window and now - window[0] >= 1.0:
            window.popleft()
        if len(window) >= faults.rate_limit_per_sec:
            stats["rate_limited_429"] += 1
            return _json(429, {"error": "rate limit exceeded"}, {"Retry-After": "1"})
        window.append(now)
    delay = faults.latency_ms + STATE.rng.random() * faults.latency_jitter_ms
    if delay > 0:
        await asyncio.sleep(delay / 1000)
    if faults.error_rate > 0 and STATE.rng.random() < faults.error_rate:
        stats["injected_5xx"] += 1
        return _json(STATE.rng.choice([500, 500, 502, 503]), {"error": "injected fault"})
    if (
        faults.slow_commit_rate > 0
        and request.method == "POST"
        and not token_endpoint
        and STATE.rng.random() < faults.slow_commit_rate
    ):
        await request.body()  # buffered: the handler still commits after the caller has hung up
        stats["slow_commit"] += 1
        await asyncio.sleep(faults.slow_commit_delay_s)
    return None


async def issue_token(provider: str, request: Request) -> Response:
    blocked = await gateway(provider, request, auth=None, token_endpoint=True)
    if blocked is not None:
        return blocked
    form = {k: v[0] for k, v in parse_qs((await request.body()).decode()).items()}
    client_id, client_secret = CREDENTIALS[provider]
    if (
        form.get("grant_type") != "client_credentials"
        or not secrets.compare_digest(form.get("client_id", ""), client_id)
        or not secrets.compare_digest(form.get("client_secret", ""), client_secret)
    ):
        STATE.stats[provider]["token_rejected"] += 1
        return _json(401, {"error": "invalid_client"})
    ttl = STATE.faults[provider].token_ttl_s
    now = time.time()
    tokens = STATE.tokens[provider]
    for old in [t for t, exp in tokens.items() if exp <= now]:
        del tokens[old]
    token = secrets.token_urlsafe(24)
    tokens[token] = now + ttl
    STATE.stats[provider]["tokens_issued"] += 1
    return _json(200, {"access_token": token, "token_type": "Bearer", "expires_in": ttl})


def record(provider: str, reference: str, order_number: str, kind: str = "") -> None:
    """Commit an accepted request. This log is what the chaos test counts: never the hub's own tables."""
    STATE.arrivals += 1
    STATE.received[provider][reference] += 1
    STATE.received_log[provider].append(
        {
            "n": STATE.arrivals,
            "reference": reference,
            "order_number": order_number,
            "kind": kind,
            "at": time.time(),
        }
    )
    STATE.stats[provider]["accepted"] += 1


async def after_commit(provider: str) -> Response | None:
    """Faults that hit after the request is committed. Return a response to replace the normal one.

    lost response: the caller is told 502 although the request landed.
    accept then time out: the answer is sent only after accept_timeout_delay_s, i.e. after the caller gave up.
    """
    faults, stats = STATE.faults[provider], STATE.stats[provider]
    if faults.lost_response_rate > 0 and STATE.rng.random() < faults.lost_response_rate:
        stats["lost_responses"] += 1
        return _json(502, {"error": "bad gateway"})
    if faults.accept_then_timeout_rate > 0 and STATE.rng.random() < faults.accept_then_timeout_rate:
        stats["accept_then_timeout"] += 1
        await asyncio.sleep(faults.accept_timeout_delay_s)
    return None


def admin_authorized(request: Request) -> bool:
    configured = os.environ.get("MOCK_ADMIN_KEY", "")
    return bool(configured) and secrets.compare_digest(request.headers.get("x-admin-key", ""), configured)


def lookup_response(provider: str, reference: str, field: str) -> Response:
    """A search endpoint: 200 with the matching records. No match is a 200 with an empty list, not a 404."""
    count = STATE.received[provider].get(reference, 0)
    items = [{field: reference, "status": "accepted"} for _ in range(min(count, 1))]
    return _json(200, {"items": items})
