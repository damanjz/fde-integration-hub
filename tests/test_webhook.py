from __future__ import annotations

import json
import time

import httpx
import pytest

from hub.main import create_app
from hub.security import sign
from tests.helpers import make_event

pytestmark = pytest.mark.anyio


@pytest.fixture
async def client(pool, settings):
    app = create_app(settings, pool=pool, start_workers=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://hub") as c:
        yield c


def signed(body: bytes, secret="current-secret", ts=None):
    ts = int(time.time()) if ts is None else ts
    return {"X-Larkfield-Timestamp": str(ts), "X-Larkfield-Signature": sign(secret, ts, body)}


def raw(event: dict) -> bytes:
    return json.dumps(event).encode()


async def count(pool, table: str) -> int:
    async with pool.connection() as conn:
        return (await (await conn.execute(f"SELECT count(*) AS n FROM {table}")).fetchone())["n"]


async def test_valid_webhook_is_stored_with_one_delivery_per_destination(client, pool):
    body = raw(make_event(1))
    resp = await client.post("/webhooks/storefront", content=body, headers=signed(body))
    assert resp.status_code == 200 and resp.json() == {"status": "accepted", "event_id": "evt_1"}
    assert await count(pool, "events") == 1
    async with pool.connection() as conn:
        rows = await (
            await conn.execute("SELECT destination, status, order_id FROM deliveries ORDER BY 1")
        ).fetchall()
        event = await (await conn.execute("SELECT raw_body FROM events")).fetchone()
    assert [(r["destination"], r["status"], r["order_id"]) for r in rows] == [
        ("accounting", "pending", "ord_1"),
        ("helpdesk", "pending", "ord_1"),
        ("warehouse", "pending", "ord_1"),
    ]
    assert event["raw_body"] == body.decode()  # raw event kept verbatim


async def test_bad_signature_is_401_and_stores_nothing(client, pool):
    body = raw(make_event(1))
    resp = await client.post("/webhooks/storefront", content=body, headers=signed(body, secret="nope"))
    assert resp.status_code == 401 and resp.json()["error"] == "bad_signature"
    assert await count(pool, "events") == 0 and await count(pool, "deliveries") == 0


async def test_expired_timestamp_is_401(client, pool):
    body = raw(make_event(1))
    resp = await client.post(
        "/webhooks/storefront", content=body, headers=signed(body, ts=int(time.time()) - 600)
    )
    assert resp.status_code == 401 and resp.json()["error"] == "timestamp_outside_window"
    assert await count(pool, "events") == 0


async def test_missing_headers_is_401(client):
    resp = await client.post("/webhooks/storefront", content=raw(make_event(1)))
    assert resp.status_code == 401


async def test_rejections_are_counted(client, pool):
    body = raw(make_event(1))
    await client.post("/webhooks/storefront", content=body, headers=signed(body, secret="nope"))
    async with pool.connection() as conn:
        rows = await (await conn.execute("SELECT name, value FROM counters")).fetchall()
    assert {r["name"]: r["value"] for r in rows} == {"rejected_bad_signature": 1}


async def test_previous_secret_still_accepted_during_rotation(client, pool):
    body = raw(make_event(1))
    resp = await client.post(
        "/webhooks/storefront", content=body, headers=signed(body, secret="previous-secret")
    )
    assert resp.status_code == 200


async def test_duplicate_event_returns_200_and_creates_nothing_new(client, pool):
    body = raw(make_event(1))
    first = await client.post("/webhooks/storefront", content=body, headers=signed(body))
    second = await client.post("/webhooks/storefront", content=body, headers=signed(body))
    assert first.json()["status"] == "accepted"
    assert second.status_code == 200 and second.json()["status"] == "duplicate"
    assert await count(pool, "events") == 1 and await count(pool, "deliveries") == 3
    async with pool.connection() as conn:
        dup = (await (await conn.execute("SELECT duplicate_count FROM events")).fetchone())["duplicate_count"]
    assert dup == 1


async def test_concurrent_duplicates_store_once(client, pool):
    import asyncio

    body = raw(make_event(1))
    results = await asyncio.gather(
        *[client.post("/webhooks/storefront", content=body, headers=signed(body)) for _ in range(8)]
    )
    assert all(r.status_code == 200 for r in results)
    assert sorted(r.json()["status"] for r in results).count("accepted") == 1
    assert await count(pool, "events") == 1 and await count(pool, "deliveries") == 3


async def test_malformed_json_after_valid_signature_is_400(client, pool):
    body = b"{not json"
    resp = await client.post("/webhooks/storefront", content=body, headers=signed(body))
    assert resp.status_code == 400 and await count(pool, "events") == 0


async def test_missing_event_id_is_400(client):
    body = json.dumps({"type": "order.created"}).encode()
    assert (await client.post("/webhooks/storefront", content=body, headers=signed(body))).status_code == 400


async def test_unknown_event_type_is_stored_but_creates_no_deliveries(client, pool):
    body = raw(make_event(1, "order.refunded"))
    resp = await client.post("/webhooks/storefront", content=body, headers=signed(body))
    assert resp.json()["status"] == "ignored"
    assert await count(pool, "events") == 1 and await count(pool, "deliveries") == 0


async def test_oversized_body_is_413(client, settings):
    body = b"x" * (settings.webhook_max_body + 1)
    assert (await client.post("/webhooks/storefront", content=body, headers=signed(body))).status_code == 413


async def test_unmappable_event_is_accepted_not_bounced(client, pool):
    """Poison messages are the hub's problem: ack to the storefront, dead-letter later with a reason."""
    ev = make_event(1)
    del ev["data"]["order"]["customer"]
    body = raw(ev)
    resp = await client.post("/webhooks/storefront", content=body, headers=signed(body))
    assert resp.status_code == 200 and await count(pool, "deliveries") == 3


async def test_new_event_behind_a_dead_one_is_born_blocked(client, pool):
    first = raw(make_event(1, "order.created", order=7))
    await client.post("/webhooks/storefront", content=first, headers=signed(first))
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE deliveries SET status = 'dead', dead_at = now() WHERE destination = 'accounting'"
        )
    second = raw(make_event(2, "order.paid", order=7))
    await client.post("/webhooks/storefront", content=second, headers=signed(second))
    async with pool.connection() as conn:
        rows = await (
            await conn.execute(
                "SELECT destination, status FROM deliveries WHERE event_id = 'evt_2' ORDER BY 1"
            )
        ).fetchall()
    assert {r["destination"]: r["status"] for r in rows} == {
        "accounting": "blocked",
        "helpdesk": "pending",
        "warehouse": "pending",
    }


async def test_webhook_logs_contain_no_customer_contact_details(client, caplog):
    import logging

    body = raw(make_event(1))
    with caplog.at_level(logging.DEBUG):
        await client.post("/webhooks/storefront", content=body, headers=signed(body))
    text = " ".join(str(getattr(r, "fields", "")) + r.getMessage() for r in caplog.records)
    assert "a.buyer@example.test" not in text and "9876543210" not in text


def test_app_refuses_to_start_without_a_webhook_secret(settings):
    from hub.config import Settings

    empty = Settings(_env_file=None, database_url=settings.database_url, webhook_secrets="")
    with pytest.raises(RuntimeError, match="HUB_WEBHOOK_SECRETS"):
        create_app(empty)
