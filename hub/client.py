"""Destination client: one class for every destination, driven by a DestinationSpec.

Every HTTP call to a destination goes through the gate (rate limit, Retry-After block, breaker),
is timed, classified and recorded as a CallRecord. The worker persists the records, so the failure log is
complete even for calls that were retried inside a single attempt (401 then refresh then resend).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx

from hub.auth import ApiKeyAuth, OAuthClientCredentials, TokenError
from hub.gate import Gate, pass_gate
from hub.logging_ import redact_text
from hub.retry import (
    NO_RESPONSE_CLASSES,
    Outcome,
    classify_exception,
    classify_status,
    parse_retry_after,
)

BODY_LIMIT = 4000


@dataclass
class CallRecord:
    kind: str  # send | lookup
    started_at: datetime
    latency_ms: int
    status_code: int | None
    error_class: str | None
    outcome: Outcome
    retry_after_s: float | None = None
    detail: str | None = None


@dataclass
class CallResult:
    outcome: Outcome
    status_code: int | None = None
    error_class: str | None = None
    body: str = ""
    retry_after_s: float | None = None
    ambiguous: bool = False  # the destination may have processed the request
    found: bool | None = None  # lookup only
    duplicate: bool = False  # destination said "already have it" (e.g. accounting 409 duplicate number)
    outage: bool = False  # failed while the breaker is open or half-open: spends no retry budget
    maybe_in_flight: bool = False  # no answer (or a gateway error): the request may still be running there
    calls: list[CallRecord] = field(default_factory=list)


@dataclass(frozen=True)
class DestinationSpec:
    name: str
    base_url: str
    send_path: str
    lookup_path: str | None
    timeout_s: float
    auth: OAuthClientCredentials | ApiKeyAuth
    lookup_param: str = "reference"
    idempotency_header: bool = False
    duplicate_statuses: tuple[int, ...] = ()
    duplicate_error: str | None = None
    duplicate_number_field: str | None = None


def lookup_found(text: str, field_name: str, key: str) -> bool | None:
    """Does a lookup response contain a record whose `field_name` equals `key`?

    Accepts a single record, a list of records, or an object holding a list (`items`, `data`, `results`,
    `shipments`). A 200 with an empty list is "not found". None means the body could not be understood,
    which the caller treats as a failed lookup, never as found.
    """
    try:
        body = json.loads(text)
    except ValueError:
        return None

    def matches(rec: Any) -> bool:
        return isinstance(rec, dict) and rec.get(field_name) == key

    if isinstance(body, list):
        return any(matches(r) for r in body)
    if isinstance(body, dict):
        if matches(body):
            return True
        for name in ("items", "data", "results", "shipments"):
            if isinstance(body.get(name), list):
                return any(matches(r) for r in body[name])
        if body == {} or "found" in body:
            return False
    return None


def is_duplicate_conflict(text: str, spec: DestinationSpec, key: str) -> bool:
    """A 409 only counts as "already delivered" if it is the duplicate error for this very delivery key."""
    if spec.duplicate_error is None and spec.duplicate_number_field is None:
        return False
    try:
        body = json.loads(text)
    except ValueError:
        return False
    if not isinstance(body, dict):
        return False
    if spec.duplicate_error is not None and body.get("error") != spec.duplicate_error:
        return False
    return spec.duplicate_number_field is None or body.get(spec.duplicate_number_field) == key


class DestinationClient:
    def __init__(
        self,
        spec: DestinationSpec,
        http: httpx.AsyncClient,
        gate: Gate,
        *,
        max_inline_wait_s: float = 2.0,
        max_retry_after_s: float = 120.0,
    ):
        self.spec = spec
        self._http = http
        self._gate = gate
        self._max_inline_wait = max_inline_wait_s
        self._max_retry_after = max_retry_after_s

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def supports_lookup(self) -> bool:
        return self.spec.lookup_path is not None

    async def send(self, payload: dict[str, Any], delivery_key: str) -> CallResult:
        extra = {"Idempotency-Key": delivery_key} if self.spec.idempotency_header else {}
        return await self._request(
            "send", "POST", self.spec.send_path, expect_key=delivery_key, extra_headers=extra, json=payload
        )

    async def lookup(self, delivery_key: str) -> CallResult:
        """Has the destination already recorded this delivery? A record carrying our key must come back."""
        assert self.spec.lookup_path is not None
        return await self._request(
            "lookup",
            "GET",
            self.spec.lookup_path,
            expect_key=delivery_key,
            params={self.spec.lookup_param: delivery_key},
        )

    async def _request(
        self,
        kind: str,
        method: str,
        path: str,
        *,
        expect_key: str,
        extra_headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> CallResult:
        calls: list[CallRecord] = []
        refreshed = False
        while True:
            await pass_gate(self._gate, self.name, self._max_inline_wait)  # may raise GateBlocked
            try:
                headers, token = await self.spec.auth.header()
                headers = {**headers, **(extra_headers or {})}
            except TokenError as exc:
                tripped = await self._gate.failure(self.name) if exc.outage else False
                calls.append(
                    CallRecord(
                        kind,
                        datetime.now(UTC),
                        0,
                        exc.status_code,
                        exc.error_class,
                        Outcome.RETRY,
                        detail=redact_text(exc.detail) or None,
                    )
                )
                return CallResult(
                    Outcome.RETRY, exc.status_code, exc.error_class, outage=tripped, calls=calls
                )

            started = datetime.now(UTC)
            t0 = time.perf_counter()
            try:
                resp = await self._http.request(
                    method,
                    self.spec.base_url + path,
                    headers=headers,
                    timeout=httpx.Timeout(self.spec.timeout_s, connect=min(5.0, self.spec.timeout_s)),
                    **kwargs,
                )
            except httpx.HTTPError as exc:
                latency = int((time.perf_counter() - t0) * 1000)
                error_class, ambiguous = classify_exception(exc)
                tripped = await self._gate.failure(self.name)
                calls.append(
                    CallRecord(
                        kind,
                        started,
                        latency,
                        None,
                        error_class,
                        Outcome.RETRY,
                        detail=redact_text(str(exc))[:500] or None,
                    )
                )
                return CallResult(
                    Outcome.RETRY,
                    None,
                    error_class,
                    ambiguous=ambiguous,
                    outage=tripped,
                    maybe_in_flight=error_class in NO_RESPONSE_CLASSES,
                    calls=calls,
                )

            latency = int((time.perf_counter() - t0) * 1000)
            status = resp.status_code
            outcome = classify_status(status)
            body = redact_text(resp.text[:BODY_LIMIT])
            retry_after = parse_retry_after(resp.headers.get("Retry-After"), cap=self._max_retry_after)

            duplicate = (
                kind == "send"
                and status in self.spec.duplicate_statuses
                and is_duplicate_conflict(resp.text, self.spec, expect_key)
            )
            found: bool | None = None
            if kind == "lookup" and status == 404:
                outcome, found = Outcome.SUCCESS, False
            elif kind == "lookup" and status == 200:
                found = lookup_found(resp.text, self.spec.lookup_param, expect_key)
                outcome = Outcome.SUCCESS if found is not None else Outcome.RETRY
            elif duplicate:
                outcome = Outcome.SUCCESS
            calls.append(
                CallRecord(
                    kind,
                    started,
                    latency,
                    status,
                    "lookup_unreadable" if outcome is Outcome.RETRY and status == 200 else None,
                    outcome,
                    retry_after,
                    None if outcome is Outcome.SUCCESS else body[:500],
                )
            )

            if outcome is Outcome.AUTH and self.spec.auth.refreshable and not refreshed:
                refreshed = True  # one refresh per request: a second 401 is a real credential problem
                await self.spec.auth.invalidate(token)
                continue

            if outcome is Outcome.SUCCESS:
                await self._gate.success(self.name)
                return CallResult(outcome, status, body=body, found=found, duplicate=duplicate, calls=calls)
            if outcome is Outcome.THROTTLED:
                wait = retry_after if retry_after is not None else 1.0
                await self._gate.throttled(self.name, wait)
                return CallResult(outcome, status, body=body, retry_after_s=wait, calls=calls)
            if outcome is Outcome.RETRY:
                if status == 200:  # a 200 that is not a readable lookup answer: never read it as "found"
                    return CallResult(outcome, status, "lookup_unreadable", body, calls=calls)
                # 503 with Retry-After is "not now, nothing was processed"; any other 5xx may have landed.
                refused = status == 503 and retry_after is not None
                if retry_after:
                    await self._gate.hold(self.name, retry_after)
                tripped = await self._gate.failure(self.name)
                return CallResult(
                    outcome,
                    status,
                    body=body,
                    retry_after_s=retry_after,
                    ambiguous=not refused,
                    outage=tripped,
                    maybe_in_flight=status in (502, 504),
                    calls=calls,
                )
            if outcome is Outcome.AUTH:  # still 401 after a refresh (or API key rejected)
                return CallResult(Outcome.RETRY, status, "auth_rejected", body, calls=calls)
            return CallResult(Outcome.PERMANENT, status, body=body, calls=calls)
