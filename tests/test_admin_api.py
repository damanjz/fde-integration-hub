from __future__ import annotations

import httpx
import pytest

from hub.main import create_app
from tests.helpers import FakeProviders, build_worker, drain, ingest, make_event

pytestmark = pytest.mark.anyio
ADMIN = {"X-Admin-Key": "admin-key-for-tests"}


@pytest.fixture
async def client(pool, settings):
    app = create_app(settings, pool=pool, start_workers=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://hub") as c:
        yield c


async def populate(pool, settings):
    """Order 1: accounting create dies (422), paid is blocked. Order 2: all delivered."""
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    for ev in (make_event(1, order=1), make_event(2, "order.paid", order=1), make_event(3, order=2)):
        await ingest(pool, settings, ev)
    fake.script["accounting"] = ["422"]
    await drain(worker, pool)
    await http.aclose()
    return fake


async def test_admin_endpoints_need_the_key(client):
    for path in ("/deliveries", "/deliveries/1", "/metrics"):
        assert (await client.get(path)).status_code == 401
        assert (await client.get(path, headers={"X-Admin-Key": "wrong"})).status_code == 401
    assert (await client.post("/deliveries/1/replay")).status_code == 401


async def test_admin_endpoints_are_disabled_when_no_key_is_configured(pool, settings):
    settings.admin_key = type(settings.admin_key)("")
    app = create_app(settings, pool=pool, start_workers=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://hub") as c:
        assert (await c.get("/deliveries", headers=ADMIN)).status_code == 503


async def test_healthz_is_open(client):
    assert (await client.get("/healthz")).json() == {"ok": True}


async def test_list_dead_and_blocked(client, pool, settings):
    await populate(pool, settings)
    dead = (await client.get("/deliveries", params={"status": "dead"}, headers=ADMIN)).json()
    assert dead["count"] == 1
    item = dead["items"][0]
    assert item["destination"] == "accounting" and item["dead_reason"] == "http_422"
    assert "validation_failed" in item["dead_body"] and item["order_id"] == "ord_1"
    blocked = (await client.get("/deliveries", params={"status": "blocked"}, headers=ADMIN)).json()
    assert [i["event_id"] for i in blocked["items"]] == ["evt_2"] and blocked["items"][0][
        "destination"
    ] == "accounting"
    by_dest = (await client.get("/deliveries", params={"destination": "warehouse"}, headers=ADMIN)).json()
    assert by_dest["count"] == 3


async def test_list_rejects_unknown_status(client):
    assert (await client.get("/deliveries", params={"status": "bogus"}, headers=ADMIN)).status_code == 422


async def test_delivery_detail_includes_the_attempt_log(client, pool, settings):
    await populate(pool, settings)
    dead = (await client.get("/deliveries", params={"status": "dead"}, headers=ADMIN)).json()["items"][0]
    detail = (await client.get(f"/deliveries/{dead['id']}", headers=ADMIN)).json()
    assert [(a["kind"], a["status_code"]) for a in detail["attempt_log"]] == [("send", 422)]
    assert (await client.get("/deliveries/99999", headers=ADMIN)).status_code == 404


async def test_replay_dead_delivery_via_api_and_blocked_rows_follow(client, pool, settings):
    fake = await populate(pool, settings)
    dead = (await client.get("/deliveries", params={"status": "dead"}, headers=ADMIN)).json()["items"][0]
    resp = await client.post(f"/deliveries/{dead['id']}/replay", headers=ADMIN)
    assert resp.status_code == 200 and resp.json() == {"replayed": [dead["id"]]}
    worker, http, _ = build_worker(pool, settings, fake)
    await drain(worker, pool)
    await http.aclose()
    statuses = (await client.get("/deliveries", params={"destination": "accounting"}, headers=ADMIN)).json()[
        "items"
    ]
    assert {i["status"] for i in statuses} == {"delivered"}
    assert len(fake.committed["accounting"]) == 3


async def test_replay_of_a_delivered_row_is_refused_with_409(client, pool, settings):
    await populate(pool, settings)
    delivered = (await client.get("/deliveries", params={"status": "delivered"}, headers=ADMIN)).json()[
        "items"
    ][0]
    resp = await client.post(f"/deliveries/{delivered['id']}/replay", headers=ADMIN)
    assert resp.status_code == 409 and "delivered" in resp.json()["detail"]
    blocked = (await client.get("/deliveries", params={"status": "blocked"}, headers=ADMIN)).json()["items"][
        0
    ]
    assert (await client.post(f"/deliveries/{blocked['id']}/replay", headers=ADMIN)).status_code == 409


async def test_replay_unknown_id_is_404(client):
    assert (await client.post("/deliveries/424242/replay", headers=ADMIN)).status_code == 404


async def test_metrics(client, pool, settings):
    await populate(pool, settings)
    # Leave one row pending so the pending count and oldest-pending age are non-trivial.
    await ingest(pool, settings, make_event(9, order=9))
    m = (await client.get("/metrics", headers=ADMIN)).json()
    acc, wh = m["destinations"]["accounting"], m["destinations"]["warehouse"]
    assert (acc["delivered"], acc["dead"], acc["blocked"], acc["pending"]) == (1, 1, 1, 1)
    assert (wh["delivered"], wh["dead"], wh["blocked"], wh["pending"]) == (3, 0, 0, 1)
    assert wh["oldest_pending_age_s"] >= 0 and wh["p50_s"] is not None and wh["p95_s"] >= wh["p50_s"]
    assert acc["send_calls"] >= 2 and m["events"]["received"] == 4
    assert set(m["destinations"]) == {"accounting", "helpdesk", "warehouse"}


async def test_metrics_report_retries_and_429s(client, pool, settings):
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    await ingest(pool, settings, make_event(1))
    fake.script["helpdesk"] = ["500", "429"]
    await drain(worker, pool)
    await http.aclose()
    h = (await client.get("/metrics", headers=ADMIN)).json()["destinations"]["helpdesk"]
    assert h["retried"] == 2 and h["http_429"] == 1 and h["http_5xx"] == 1


async def test_metrics_for_destinations_without_traffic_are_zero(client):
    m = (await client.get("/metrics", headers=ADMIN)).json()
    assert m["destinations"]["warehouse"]["pending"] == 0 and m["events"]["received"] == 0


async def test_replay_cli_function(pool, settings):
    from datetime import UTC, datetime, timedelta

    from hub import outbox

    await populate(pool, settings)
    future = datetime.now(UTC) + timedelta(hours=1)
    assert (await outbox.replay(pool, destination="accounting", since=future)).replayed == []
    assert (await outbox.replay(pool, destination="warehouse")).replayed == []
    result = await outbox.replay(pool, destination="accounting", since=datetime.now(UTC) - timedelta(hours=1))
    assert len(result.replayed) == 1
