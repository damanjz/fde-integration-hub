"""Test doubles: a scripted fake of all three destinations behind httpx.MockTransport, and event builders."""

from __future__ import annotations

import asyncio
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs

import httpx

from hub import outbox
from hub.client import DestinationClient
from hub.config import Settings
from hub.destinations import build_clients, destination_names
from hub.gate import GateConfig, MemoryGate
from hub.mapping import delivery_key
from hub.worker import Worker

_clock = datetime(2026, 11, 10, 9, 0, tzinfo=UTC)


def make_event(n: int, etype: str = "order.created", order: int | None = None, **order_overrides) -> dict:
    """Storefront envelope. created_at increases with n so per-order ordering is deterministic."""
    order = n if order is None else order
    body = {
        "id": f"evt_{n}",
        "type": etype,
        "created_at": (_clock + timedelta(seconds=n)).isoformat(),
        "data": {
            "order": {
                "id": f"ord_{order}",
                "number": f"LF-{order}",
                "currency": "USD",
                "total": "25.00",
                "customer": {"email": "a.buyer@example.test", "name": "A Buyer", "phone": "+91 9876543210"},
                "shipping_address": {
                    "line1": "1 Park St",
                    "city": "Hyderabad",
                    "postal_code": "500081",
                    "country": "IN",
                },
                "lines": [
                    {"sku": "LF-BEAN-250", "name": "House blend", "quantity": 2, "unit_price": "12.50"}
                ],
            }
        },
    }
    body["data"]["order"].update(order_overrides)
    return body


async def ingest(pool, settings: Settings, event: dict) -> None:
    order = event["data"].get("order", {})
    await outbox.ingest_event(
        pool,
        event_id=event["id"],
        event_type=event["type"],
        order_id=order.get("id") if isinstance(order, dict) else None,
        occurred_at=datetime.fromisoformat(event["created_at"]),
        raw_body=json.dumps(event),
        payload=event,
        body_sha256="x",
        destinations=[(d, delivery_key(event["id"], d)) for d in destination_names(settings)],
    )


class FakeProviders:
    """Scripted warehouse, helpdesk and accounting. Records committed requests like the real mocks do."""

    def __init__(self) -> None:
        self.committed: dict[str, list[str]] = defaultdict(list)  # destination -> refs in commit order
        self.script: dict[str, list[str]] = defaultdict(list)  # destination -> actions for upcoming POSTs
        self.calls: list[tuple[str, str, str]] = []  # (destination, method, ref)
        self.headers: list[tuple[str, dict]] = []
        self.tokens: set[str] = set()
        self.token_seq = 0
        self.idem: dict[str, bool] = {}
        self.on_commit = None  # optional hook(destination, ref)
        self.down: set[str] = set()  # destinations that answer 503 to everything
        self.pending_commits: list[
            tuple[str, str]
        ] = []  # slow requests still being processed at the destination
        self.get_filter = None  # optional hook(request) -> Response | None for lookups

    def expire_tokens(self) -> None:
        self.tokens.clear()

    def count(self, destination: str, ref: str) -> int:
        return Counter(self.committed[destination])[ref]

    def land_pending(self) -> None:
        """Time passes: requests the caller gave up on finish committing at the destination."""
        for dest, ref in self.pending_commits:
            self._commit(dest, ref)
        self.pending_commits.clear()

    def _commit(self, dest: str, ref: str) -> None:
        self.committed[dest].append(ref)
        if self.on_commit:
            self.on_commit(dest, ref)

    def handler(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.strip("/").split("/")
        dest, rest = parts[0], "/" + "/".join(parts[1:])
        if rest == "/oauth/token":
            self.token_seq += 1
            token = f"tok-{dest}-{self.token_seq}"
            self.tokens.add(token)
            assert parse_qs(request.content.decode())["grant_type"] == ["client_credentials"]
            return httpx.Response(200, json={"access_token": token, "expires_in": 60})
        if dest in ("warehouse", "accounting"):
            auth = request.headers.get("authorization", "")[7:]
            if auth not in self.tokens:
                return httpx.Response(401, json={"error": "invalid_token"})
        self.headers.append((dest, dict(request.headers)))
        if dest in self.down:  # a real outage fails lookups too
            self.calls.append((dest, request.method, "down"))
            return httpx.Response(503, json={"error": "down"})
        if request.method == "GET":  # warehouse lookup
            if self.get_filter and (custom := self.get_filter(request)) is not None:
                return custom
            ref = request.url.params["external_ref"]
            self.calls.append((dest, "GET", ref))
            items = [{"external_ref": ref}] if ref in self.committed[dest] else []
            return httpx.Response(200, json={"items": items})  # a search endpoint: empty list = not found
        payload = json.loads(request.content)
        ref = payload.get("external_ref") or payload["reference"]
        self.calls.append((dest, "POST", ref))
        action = self.script[dest].pop(0) if self.script[dest] else "ok"
        if action == "429":
            return httpx.Response(429, headers={"Retry-After": "0"}, json={})
        if action in ("500", "503"):
            return httpx.Response(int(action), json={"error": "boom"})
        if action == "502":
            return httpx.Response(502, json={"error": "gateway"})
        if action == "503_retry_after":
            return httpx.Response(503, headers={"Retry-After": "0"}, json={"error": "try later"})
        if action == "timeout_slow_commit":  # still processing when the caller gives up; commits later
            self.pending_commits.append((dest, ref))
            raise httpx.ReadTimeout("slow", request=request)
        if action == "422":
            return httpx.Response(422, json={"error": "validation_failed", "errors": [{"loc": "currency"}]})
        if action == "timeout_before":
            raise httpx.ReadTimeout("slow", request=request)
        if action == "conn_refused":
            raise httpx.ConnectError("refused", request=request)
        # ok / timeout_after_commit / lost_502 all commit; they differ in what the caller sees.
        key = request.headers.get("idempotency-key")
        if dest == "helpdesk" and key and key in self.idem:
            return httpx.Response(201, json={"replayed": True})
        if dest == "accounting" and ref in self.committed[dest]:
            return httpx.Response(409, json={"error": "duplicate_document_number", "number": ref})
        self._commit(dest, ref)
        if key:
            self.idem[key] = True
        if action == "timeout_after_commit":
            raise httpx.ReadTimeout("slow", request=request)
        if action == "lost_502":
            return httpx.Response(502, json={})
        return httpx.Response(201, json={"ok": True})


def build_worker(
    pool, settings: Settings, fake: FakeProviders, name: str = "w", rate: float = 1000.0
) -> tuple[Worker, httpx.AsyncClient, dict[str, DestinationClient]]:
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake.handler))
    gate = MemoryGate(
        {
            d: GateConfig(rate=rate, burst=rate, breaker_threshold=settings.breaker_threshold)
            for d in destination_names(settings)
        }
    )
    clients = build_clients(settings, gate, http)
    return Worker(name, pool, clients, settings), http, clients


async def drain(worker: Worker, pool, limit_s: float = 10.0) -> None:
    """Run the worker until no pending or in-flight rows are left (blocked and dead are terminal here)."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + limit_s
    while loop.time() < deadline:
        worked = await worker.run_once()
        if not worked:
            async with pool.connection() as conn:
                cur = await conn.execute(
                    "SELECT count(*) AS n FROM deliveries WHERE status IN ('pending', 'in_flight')"
                )
                if (await cur.fetchone())["n"] == 0:
                    return
            await asyncio.sleep(0.02)
    raise AssertionError("queue did not drain")


async def rows(pool, where: str = "true") -> list[dict]:
    async with pool.connection() as conn:
        cur = await conn.execute(f"SELECT * FROM deliveries WHERE {where} ORDER BY id")
        return await cur.fetchall()
