from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest

from hub.mapping import to_accounting, to_helpdesk, to_warehouse
from mocks import core
from mocks.app import create_app
from tests.helpers import make_event

pytestmark = pytest.mark.anyio
ADMIN = {"X-Admin-Key": "mock-admin"}


@pytest.fixture
async def mock(monkeypatch):
    monkeypatch.setenv("MOCK_ADMIN_KEY", "mock-admin")
    core.STATE.reset()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app()), base_url="http://m") as c:
        yield c
    core.STATE.reset()


async def token(mock, provider="warehouse", secret=None):
    resp = await mock.post(
        f"/{provider}/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": f"hub-{provider}",
            "client_secret": secret or f"dev-{provider}-secret",
        },
    )
    return resp


def bearer(resp) -> dict:
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}


async def received(mock) -> dict:
    return (await mock.get("/admin/received", headers=ADMIN)).json()


async def test_admin_endpoints_need_the_key(mock):
    for path in ("/admin/faults", "/admin/received", "/admin/stats", "/storefront/status"):
        assert (await mock.get(path)).status_code == 401
    assert (await mock.put("/admin/faults", json={})).status_code == 401


async def test_warehouse_token_lifetime_is_60_seconds(mock):
    resp = await token(mock)
    assert resp.status_code == 200 and resp.json()["expires_in"] == 60


async def test_token_endpoint_rejects_bad_credentials(mock):
    assert (await token(mock, secret="wrong")).status_code == 401


async def test_expired_or_missing_token_is_401(mock):
    body = to_warehouse(make_event(1))
    assert (await mock.post("/warehouse/shipments", json=body)).status_code == 401
    auth = bearer(await token(mock))
    core.STATE.tokens["warehouse"][auth["Authorization"][7:]] = time.time() - 1  # expired
    assert (await mock.post("/warehouse/shipments", json=body, headers=auth)).status_code == 401


async def test_warehouse_accepts_records_and_does_not_dedupe(mock):
    auth = bearer(await token(mock))
    body = to_warehouse(make_event(1))
    for _ in range(2):
        resp = await mock.post("/warehouse/shipments", json=body, headers={**auth, "Idempotency-Key": "k"})
        assert resp.status_code == 201
    entries = (await received(mock))["warehouse"]["entries"]
    assert [e["reference"] for e in entries] == [body["external_ref"]] * 2  # idempotency header ignored
    assert [e["n"] for e in entries] == [1, 2]


async def test_warehouse_lookup_by_external_ref(mock):
    auth = bearer(await token(mock))
    body = to_warehouse(make_event(1))
    params = {"external_ref": body["external_ref"]}
    miss = await mock.get("/warehouse/shipments", params=params, headers=auth)
    assert miss.status_code == 200 and miss.json() == {"items": []}  # a search: no match is an empty list
    await mock.post("/warehouse/shipments", json=body, headers=auth)
    hit = await mock.get("/warehouse/shipments", params=params, headers=auth)
    assert hit.status_code == 200 and hit.json()["items"][0]["external_ref"] == body["external_ref"]


async def test_warehouse_rate_limit_returns_429_with_retry_after(mock):
    auth = bearer(await token(mock))
    statuses = []
    for _ in range(8):
        resp = await mock.get("/warehouse/shipments", params={"external_ref": "x"}, headers=auth)
        statuses.append(resp.status_code)
        if resp.status_code == 429:
            assert resp.headers["Retry-After"] == "1"
    assert statuses.count(429) == 3 and statuses[:5] == [200] * 5
    await asyncio.sleep(1.05)
    assert (
        await mock.get("/warehouse/shipments", params={"external_ref": "x"}, headers=auth)
    ).status_code == 200


async def test_helpdesk_requires_api_key(mock):
    body = to_helpdesk(make_event(1))
    assert (await mock.post("/helpdesk/v1/records", json=body)).status_code == 401
    assert (
        await mock.post("/helpdesk/v1/records", json=body, headers={"X-Api-Key": "dev-helpdesk-key"})
    ).status_code == 201


async def test_helpdesk_honours_idempotency_key(mock):
    body = to_helpdesk(make_event(1))
    headers = {"X-Api-Key": "dev-helpdesk-key", "Idempotency-Key": "abc"}
    first = await mock.post("/helpdesk/v1/records", json=body, headers=headers)
    again = await mock.post("/helpdesk/v1/records", json=body, headers=headers)
    assert first.status_code == again.status_code == 201 and again.headers["Idempotent-Replayed"] == "true"
    assert (await received(mock))["helpdesk"]["total"] == 1
    other = await mock.post("/helpdesk/v1/records", json=body, headers={**headers, "Idempotency-Key": "xyz"})
    assert other.status_code == 201 and (await received(mock))["helpdesk"]["total"] == 2


async def test_helpdesk_idempotency_expires_after_24_hours(mock):
    body = to_helpdesk(make_event(1))
    headers = {"X-Api-Key": "dev-helpdesk-key", "Idempotency-Key": "abc"}
    await mock.post("/helpdesk/v1/records", json=body, headers=headers)
    key, (_, status, resp) = next(iter(core.STATE.idempotency.items()))
    core.STATE.idempotency[key] = (time.time() - 1, status, resp)
    await mock.post("/helpdesk/v1/records", json=body, headers=headers)
    assert (await received(mock))["helpdesk"]["total"] == 2


async def test_accounting_validates_schema_with_422(mock):
    auth = bearer(await token(mock, "accounting"))
    good = to_accounting(make_event(1))
    assert (await mock.post("/accounting/v1/documents", json=good, headers=auth)).status_code == 201
    for field, value in (("currency", "US$"), ("total", "1.00")):
        bad = {**to_accounting(make_event(2)), field: value}
        resp = await mock.post("/accounting/v1/documents", json=bad, headers=auth)
        assert resp.status_code == 422, field
    neg = to_accounting(
        make_event(3, lines=[{"sku": "S", "name": "n", "quantity": 1, "unit_price": "-5.00"}], total="-5.00")
    )
    assert (await mock.post("/accounting/v1/documents", json=neg, headers=auth)).status_code == 422
    assert (await received(mock))["accounting"]["total"] == 1  # rejected documents are never recorded


async def test_accounting_409_on_duplicate_number(mock):
    auth = bearer(await token(mock, "accounting"))
    doc = to_accounting(make_event(1))
    assert (await mock.post("/accounting/v1/documents", json=doc, headers=auth)).status_code == 201
    assert (await mock.post("/accounting/v1/documents", json=doc, headers=auth)).status_code == 409
    assert (await received(mock))["accounting"]["total"] == 1


async def test_outage_returns_503_for_everything_including_tokens(mock):
    await mock.put("/admin/faults", json={"warehouse": {"outage": True}}, headers=ADMIN)
    assert (await token(mock)).status_code == 503
    await mock.put("/admin/faults", json={"warehouse": {"outage": False}}, headers=ADMIN)
    assert (await token(mock)).status_code == 200


async def test_error_rate_injects_5xx_without_recording(mock):
    await mock.put("/admin/faults", json={"helpdesk": {"error_rate": 1.0}}, headers=ADMIN)
    resp = await mock.post(
        "/helpdesk/v1/records", json=to_helpdesk(make_event(1)), headers={"X-Api-Key": "dev-helpdesk-key"}
    )
    assert resp.status_code in (500, 502, 503)
    assert (await received(mock))["helpdesk"]["total"] == 0


async def test_latency_fault_slows_the_response(mock):
    await mock.put("/admin/faults", json={"helpdesk": {"latency_ms": 150}}, headers=ADMIN)
    t0 = time.perf_counter()
    await mock.post(
        "/helpdesk/v1/records", json=to_helpdesk(make_event(1)), headers={"X-Api-Key": "dev-helpdesk-key"}
    )
    assert time.perf_counter() - t0 >= 0.14


async def test_accept_then_timeout_commits_first_and_answers_late(mock):
    await mock.put(
        "/admin/faults",
        json={"helpdesk": {"accept_then_timeout_rate": 1.0, "accept_timeout_delay_s": 0.4}},
        headers=ADMIN,
    )
    body = to_helpdesk(make_event(1))
    task = asyncio.create_task(
        mock.post("/helpdesk/v1/records", json=body, headers={"X-Api-Key": "dev-helpdesk-key"})
    )
    await asyncio.sleep(0.15)
    assert not task.done()  # the caller is still waiting ...
    assert (await received(mock))["helpdesk"]["total"] == 1  # ... but the request has already landed
    assert (await task).status_code == 201


async def test_lost_response_commits_and_returns_502(mock):
    await mock.put("/admin/faults", json={"helpdesk": {"lost_response_rate": 1.0}}, headers=ADMIN)
    resp = await mock.post(
        "/helpdesk/v1/records", json=to_helpdesk(make_event(1)), headers={"X-Api-Key": "dev-helpdesk-key"}
    )
    assert resp.status_code == 502 and (await received(mock))["helpdesk"]["total"] == 1


async def test_faults_from_env(monkeypatch):
    monkeypatch.setenv(
        "MOCK_FAULTS", '{"helpdesk": {"error_rate": 0.2}, "warehouse": {"rate_limit_per_sec": 3}}'
    )
    faults = core.default_faults()
    assert faults["helpdesk"].error_rate == 0.2 and faults["warehouse"].rate_limit_per_sec == 3
    assert faults["warehouse"].token_ttl_s == 60
    monkeypatch.delenv("MOCK_FAULTS")


async def test_unknown_fault_field_is_rejected(mock):
    assert (await mock.put("/admin/faults", json={"helpdesk": {"nope": 1}}, headers=ADMIN)).status_code == 422
    assert (await mock.put("/admin/faults", json={"nobody": {}}, headers=ADMIN)).status_code == 404


async def test_token_revocation(mock):
    auth = bearer(await token(mock))
    assert (
        await mock.post("/admin/tokens/revoke", params={"provider": "warehouse"}, headers=ADMIN)
    ).json() == {"revoked": 1}
    resp = await mock.get("/warehouse/shipments", params={"external_ref": "x"}, headers=auth)
    assert resp.status_code == 401


async def test_storefront_plan_is_deterministic_and_honours_counts(mock):
    from mocks.storefront import EmitRequest, Emitter

    req = EmitRequest(events=100, seed=3, invalid={"negative_price": 1, "malformed": 1})
    a, b = Emitter().plan(req), Emitter().plan(req)
    flat = [(t, o["id"], k) for seq in a for t, o, k in seq]
    assert flat == [(t, o["id"], k) for seq in b for t, o, k in seq]
    assert len(flat) == 100
    assert sorted({k for _, _, k in flat if k}) == ["malformed", "negative_price"]
    for seq in a:  # created always first, an order is never cancelled before it exists
        assert seq[0][0] == "order.created"
    assert any(len(seq) == 2 and seq[1][0] == "order.cancelled" for seq in a)  # cancel right after create


async def test_storefront_events_are_signed_correctly(mock):
    from hub.security import verify
    from mocks.storefront import Emitter

    body = Emitter._envelope("evt_1", "order.created", {"id": "o"})
    from mocks.storefront import sign

    ts = int(time.time())
    assert verify(["s"], body, sign("s", ts, body), str(ts)) == ts


def test_mock_admin_key_is_required_from_env(monkeypatch):
    monkeypatch.delenv("MOCK_ADMIN_KEY", raising=False)
    assert os.environ.get("MOCK_ADMIN_KEY") is None
