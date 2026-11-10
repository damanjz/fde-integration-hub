"""Storefront mock: emits signed order webhooks to the hub and can resend them (duplicates).

Per order the events are sent in sequence (a real storefront does the same), so a delivery order
violation seen at a destination is the hub's doing, not the emitter's. An emission run produces a
manifest the chaos test uses to know which events exist and which are deliberately invalid.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import random
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from mocks import core

router = APIRouter(prefix="/storefront")

CATALOG = [
    ("LF-BEAN-250", "House blend 250 g", "12.50"),
    ("LF-BEAN-1KG", "House blend 1 kg", "39.00"),
    ("LF-FILTER-V60", "Ceramic V60 dripper", "24.00"),
    ("LF-MUG-12", "Larkfield mug", "9.75"),
    ("LF-GRIND-HAND", "Hand grinder", "58.20"),
]
INVALID_KINDS = ("negative_price", "bad_currency", "total_mismatch", "malformed")


def sign(secret: str, timestamp: int, body: bytes) -> str:
    """Independent implementation of the signing scheme (the hub's verifier is tested against it)."""
    return "sha256=" + hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()


class EmitRequest(BaseModel):
    hub_url: str = "http://localhost:8100"
    events: int = 500
    seed: int = 1
    events_per_sec: float = 10.0
    duplicate_rate: float = 0.05
    cancel_fast_fraction: float = 0.12  # share of orders cancelled right after creation
    invalid: dict[str, int] = {}  # kind -> number of orders; each also gets a follow-up order.paid
    late: int = 0  # orders whose order.created reaches the hub AFTER their order.paid (storefront retry)
    forged: int = 0
    stale: int = 0
    secret: str | None = None  # defaults to MOCK_WEBHOOK_SECRET


@dataclass
class ManifestEntry:
    event_id: str
    type: str
    order_id: str
    order_number: str
    invalid: str | None
    index_in_order: int
    late: bool = False  # this event is sent after the order's next event although it happened first


@dataclass
class Status:
    running: bool = False
    events_total: int = 0
    events_sent: int = 0
    events_failed: int = 0
    post_retries: int = 0
    duplicates_sent: int = 0
    duplicates_answered_200: int = 0
    forged_sent: int = 0
    forged_rejected: int = 0
    stale_sent: int = 0
    stale_rejected: int = 0
    unexpected: list[str] = field(default_factory=list)
    started_at: float | None = None
    finished_at: float | None = None


class Emitter:
    def __init__(self) -> None:
        self.status = Status()
        self.manifest: list[ManifestEntry] = []
        self.bodies: dict[str, bytes] = {}
        self.late_orders: set[str] = set()
        self._task: asyncio.Task | None = None

    # ---- planning -------------------------------------------------------------------------------
    def _order(self, rng: random.Random, n: int, seed: int) -> dict:
        lines = []
        for sku, name, price in rng.sample(CATALOG, rng.randint(1, 3)):
            lines.append({"sku": sku, "name": name, "quantity": rng.randint(1, 3), "unit_price": price})
        total = sum((Decimal(ln["unit_price"]) * ln["quantity"] for ln in lines), Decimal(0))
        return {
            "id": f"ord_{seed}_{n:04d}",
            "number": f"LF-{seed}{n:04d}",
            "currency": "USD",
            "total": format(total, "f"),
            "customer": {
                "email": f"customer{n}@example.test",
                "name": f"Customer {n}",
                "phone": f"+91 98{rng.randint(10000000, 99999999)}",
            },
            "shipping_address": {
                "line1": f"{rng.randint(1, 99)} Park Street",
                "city": "Hyderabad",
                "postal_code": "500081",
                "country": "IN",
            },
            "lines": lines,
        }

    @staticmethod
    def _corrupt(order: dict, kind: str) -> dict:
        order = json.loads(json.dumps(order))
        if kind == "negative_price":
            order["lines"][0]["unit_price"] = "-5.00"
        elif kind == "bad_currency":
            order["currency"] = "US$"
        elif kind == "total_mismatch":
            order["total"] = "1.00"
        elif kind == "malformed":
            del order["customer"]
        return order

    def plan(self, req: EmitRequest) -> list[list[tuple[str, dict, str | None]]]:
        """Return per-order sequences of (event type, order data, invalid kind)."""
        rng = random.Random(req.seed)
        orders: list[list[tuple[str, dict, str | None]]] = []
        used = 0
        n = 0
        invalid_queue = [kind for kind in INVALID_KINDS for _ in range(req.invalid.get(kind, 0))]
        rng.shuffle(invalid_queue)
        # Spread invalid orders over the run instead of putting them first.
        slots = (
            sorted(
                rng.sample(
                    range(max(len(invalid_queue), 1), max(req.events // 3, len(invalid_queue) + 1)),
                    len(invalid_queue),
                )
            )
            if invalid_queue
            else []
        )
        while used < req.events:
            n += 1
            base = self._order(rng, n, req.seed)
            if slots and len(orders) >= slots[0]:
                slots.pop(0)
                kind = invalid_queue.pop(0)
                bad = self._corrupt(base, kind)
                seq = [("order.created", bad, kind), ("order.paid", bad, kind)]
            else:
                r = rng.random()
                if r < req.cancel_fast_fraction:
                    kinds = ["order.created", "order.cancelled"]
                elif r < 0.75:
                    kinds = ["order.created", "order.paid"]
                elif r < 0.85:
                    kinds = ["order.created", "order.paid", "order.cancelled"]
                else:
                    kinds = ["order.created"]
                seq = [(k, base, None) for k in kinds]
            seq = seq[: req.events - used]
            used += len(seq)
            orders.append(seq)
        pool = [
            seq[0][1]["id"]
            for seq in orders
            if len(seq) == 2 and seq[1][0] == "order.paid" and seq[0][2] is None
        ]
        self.late_orders = set(random.Random(req.seed + 7).sample(pool, min(req.late, len(pool))))
        return orders

    # ---- sending --------------------------------------------------------------------------------
    @staticmethod
    def _envelope(event_id: str, etype: str, order: dict) -> bytes:
        return json.dumps(
            {
                "id": event_id,
                "type": etype,
                "created_at": datetime.now(UTC).isoformat(timespec="microseconds"),
                "data": {"order": order},
            },
            separators=(",", ":"),
        ).encode()

    async def _post(
        self, client: httpx.AsyncClient, url: str, body: bytes, secret: str, *, ts: int | None = None
    ):
        ts = int(time.time()) if ts is None else ts
        return await client.post(
            url,
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Larkfield-Timestamp": str(ts),
                "X-Larkfield-Signature": sign(secret, ts, body),
            },
        )

    async def _deliver(self, client, url, body, secret, event_id) -> bool:
        """Post until the hub acknowledges with 2xx, like a real storefront's webhook retry."""
        for attempt in range(1, 13):
            try:
                resp = await self._post(client, url, body, secret)
                if 200 <= resp.status_code < 300:
                    return True
                self.status.unexpected.append(f"{event_id} got {resp.status_code}")
            except httpx.HTTPError as exc:
                self.status.unexpected.append(f"{event_id} {exc.__class__.__name__}")
            self.status.post_retries += 1
            await asyncio.sleep(min(5.0, 0.5 * attempt))
        return False

    async def _run_late_order(self, client, url, secret, seq, gate) -> None:
        """order.paid reaches the hub first; order.created follows 3 s later although it happened first."""
        entries = [e for e in self.manifest if e.order_id == seq[0][1]["id"]]
        entries.sort(key=lambda e: e.index_in_order)
        bodies = [self._envelope(e.event_id, t, o) for e, (t, o, _) in zip(entries, seq, strict=True)]
        for e, body in zip(entries, bodies, strict=True):
            self.bodies[e.event_id] = body
        for pos, delay in ((1, 0.0), (0, 3.0)):
            await asyncio.sleep(delay)
            async with gate:
                ok = await self._deliver(client, url, bodies[pos], secret, entries[pos].event_id)
            self.status.events_sent += ok
            self.status.events_failed += not ok

    async def _run_order(self, client, url, secret, seq, gate, rng, req, dup_tasks) -> None:
        if seq[0][1]["id"] in self.late_orders:
            return await self._run_late_order(client, url, secret, seq, gate)
        for i, (etype, order, kind) in enumerate(seq):
            if i > 0:
                await asyncio.sleep(
                    0
                    if etype == "order.cancelled" and len(seq) == 2 and kind is None
                    else rng.uniform(0.2, 2.5)
                )
            entry = next(e for e in self.manifest if e.order_id == order["id"] and e.index_in_order == i)
            body = self._envelope(entry.event_id, etype, order)
            self.bodies[entry.event_id] = body
            async with gate:
                ok = await self._deliver(client, url, body, secret, entry.event_id)
            if ok:
                self.status.events_sent += 1
                if rng.random() < req.duplicate_rate:
                    dup_tasks.append(
                        asyncio.create_task(self._duplicate(client, url, secret, entry.event_id, rng))
                    )
            else:
                self.status.events_failed += 1

    async def _duplicate(self, client, url, secret, event_id, rng) -> None:
        await asyncio.sleep(rng.uniform(0.2, 8.0))
        try:
            resp = await self._post(client, url, self.bodies[event_id], secret)
        except httpx.HTTPError:
            return
        self.status.duplicates_sent += 1
        if resp.status_code == 200:
            self.status.duplicates_answered_200 += 1
        else:
            self.status.unexpected.append(f"duplicate {event_id} got {resp.status_code}")

    async def _bad_signature(self, client, url, secret, kind: str, n: int) -> None:
        body = self._envelope(f"evt_{kind}_{n}", "order.created", self._order(random.Random(n), n, 999))
        if kind == "forged":
            self.status.forged_sent += 1
            resp = await self._post(client, url, body, "not-the-secret")
            self.status.forged_rejected += resp.status_code == 401
        else:
            self.status.stale_sent += 1
            resp = await self._post(client, url, body, secret, ts=int(time.time()) - 3600)
            self.status.stale_rejected += resp.status_code == 401
        if resp.status_code != 401:
            self.status.unexpected.append(f"{kind} webhook got {resp.status_code}, expected 401")

    async def _run(self, req: EmitRequest, orders) -> None:
        secret = req.secret or os.environ.get("MOCK_WEBHOOK_SECRET", "dev-webhook-secret")
        url = req.hub_url.rstrip("/") + "/webhooks/storefront"
        rng = random.Random(req.seed + 1)
        gate = asyncio.Semaphore(16)
        dup_tasks: list[asyncio.Task] = []
        interval = (req.events / max(1, len(orders))) / max(req.events_per_sec, 0.1)  # seconds between orders
        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = []
            bad_total = req.forged + req.stale
            bad_at = sorted(rng.sample(range(len(orders)), min(bad_total, len(orders)))) if bad_total else []
            bad_kinds = ["forged"] * req.forged + ["stale"] * req.stale
            for idx, seq in enumerate(orders):
                tasks.append(
                    asyncio.create_task(self._run_order(client, url, secret, seq, gate, rng, req, dup_tasks))
                )
                while bad_at and bad_at[0] == idx:
                    bad_at.pop(0)
                    tasks.append(
                        asyncio.create_task(self._bad_signature(client, url, secret, bad_kinds.pop(), idx))
                    )
                await asyncio.sleep(interval)
            await asyncio.gather(*tasks)
            if dup_tasks:
                await asyncio.gather(*dup_tasks)
        self.status.running = False
        self.status.finished_at = time.time()

    def start(self, req: EmitRequest) -> None:
        if self.status.running:
            raise HTTPException(409, "an emission run is already in progress")
        orders = self.plan(req)
        self.manifest = []
        self.bodies = {}
        k = 0
        for seq in orders:
            for i, (etype, order, kind) in enumerate(seq):
                k += 1
                self.manifest.append(
                    ManifestEntry(
                        f"evt_{req.seed}_{k:05d}",
                        etype,
                        order["id"],
                        order["number"],
                        kind,
                        i,
                        late=(i == 0 and order["id"] in self.late_orders),
                    )
                )
        self.status = Status(running=True, events_total=len(self.manifest), started_at=time.time())
        self._task = asyncio.create_task(self._run(req, orders))

    async def resend(self, hub_url: str, event_id: str, secret: str | None = None) -> int:
        body = self.bodies.get(event_id)
        if body is None:
            raise HTTPException(404, "unknown event")
        secret = secret or os.environ.get("MOCK_WEBHOOK_SECRET", "dev-webhook-secret")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await self._post(client, hub_url.rstrip("/") + "/webhooks/storefront", body, secret)
        return resp.status_code


EMITTER = Emitter()


def _guard(request: Request) -> None:
    if not core.admin_authorized(request):
        raise HTTPException(401, "admin key required (MOCK_ADMIN_KEY)")


@router.post("/emit")
async def emit(request: Request, req: EmitRequest) -> dict:
    _guard(request)
    EMITTER.start(req)
    return {"started": True, "events": EMITTER.status.events_total}


@router.get("/status")
async def status(request: Request) -> dict:
    _guard(request)
    return asdict(EMITTER.status)


@router.get("/manifest")
async def manifest(request: Request) -> list[dict]:
    _guard(request)
    return [asdict(e) for e in EMITTER.manifest]


@router.post("/resend/{event_id}")
async def resend(request: Request, event_id: str, hub_url: str = "http://localhost:8100") -> dict:
    _guard(request)
    return {"hub_status": await EMITTER.resend(hub_url, event_id)}
