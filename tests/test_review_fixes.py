"""Regression tests for the holes an independent review found. Each one fails if its hole is back.

H1 late earlier event, H2 any 409, H3 lookup trusts a 200, H4 outage burns the retry budget, H5 the
documented window, H6 ingest racing a dead-lettering, S1 non-ASCII signature, plus the operator tools
(corrected replay, skip, helpdesk idempotency window) and the security settings.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import httpx
import pytest
from psycopg.types.json import Jsonb

from hub import outbox
from hub.config import Settings
from hub.destinations import build_clients
from hub.gate import GateConfig, MemoryGate
from hub.main import create_app
from hub.mapping import delivery_key
from hub.security import SignatureError, verify
from hub.worker import Worker
from tests.helpers import FakeProviders, build_worker, drain, ingest, make_event, rows

pytestmark = pytest.mark.anyio
DESTS = ("warehouse", "helpdesk", "accounting")
ADMIN = {"X-Admin-Key": "admin-key-for-tests"}


def key(n, dest):
    return delivery_key(f"evt_{n}", dest)


def gate(**kw):
    return MemoryGate({d: GateConfig(rate=1000, burst=1000, **kw) for d in DESTS})


async def wh_rows(pool):
    return {r["event_id"]: r for r in await rows(pool, "destination = 'warehouse'")}


# ---- H1: a late earlier event must never be sent after a later one ---------------------------------


async def test_H1_late_earlier_event_is_superseded_not_sent_after_the_later_one(pool, settings):
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    await ingest(pool, settings, make_event(5, "order.cancelled", order=1))
    await drain(worker, pool)
    await ingest(pool, settings, make_event(2, "order.created", order=1))  # storefront retry, arrives late
    await drain(worker, pool)
    for dest in DESTS:
        assert fake.committed[dest] == [key(5, dest)], dest  # the stale created was never sent
        late = [r for r in await rows(pool, f"destination = '{dest}' AND event_id = 'evt_2'")][0]
        assert late["status"] == "superseded"
    async with pool.connection() as conn:
        audit = await (await conn.execute("SELECT action, actor, detail FROM delivery_audit")).fetchall()
        log = await (
            await conn.execute("SELECT kind, outcome FROM delivery_attempts WHERE kind = 'superseded'")
        ).fetchall()
    assert {a["action"] for a in audit} == {"superseded"} and len(audit) == 3  # audited per destination
    assert audit[0]["detail"]["later_event_id"] == "evt_5"
    assert len(log) == 3
    await http.aclose()


async def test_H1_supersede_is_visible_in_metrics_and_api(pool, settings):
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    await ingest(pool, settings, make_event(5, "order.paid", order=1))
    await drain(worker, pool)
    await ingest(pool, settings, make_event(2, "order.created", order=1))
    await drain(worker, pool)
    app = create_app(settings, pool=pool, start_workers=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://hub") as c:
        m = (await c.get("/metrics", headers=ADMIN)).json()["destinations"]
        listed = (await c.get("/deliveries", params={"status": "superseded"}, headers=ADMIN)).json()
        one = listed["items"][0]
        detail = (await c.get(f"/deliveries/{one['id']}", headers=ADMIN)).json()
        replay = await c.post(f"/deliveries/{one['id']}/replay", headers=ADMIN)
    assert all(m[d]["superseded"] == 1 for d in DESTS)
    assert listed["count"] == 3
    assert detail["audit"][0]["action"] == "superseded"
    assert replay.status_code == 409 and "later event was already delivered" in replay.json()["detail"]
    await http.aclose()


async def test_H1_older_event_does_not_overtake_a_newer_one_that_is_in_flight(pool, settings):
    """If the newer event is being sent right now, the older one waits: sending both could reorder them."""
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    await ingest(pool, settings, make_event(5, "order.paid", order=1))
    newer = await outbox.claim(pool, 30)
    while newer.destination != "warehouse":
        newer = await outbox.claim(pool, 30)
    await ingest(pool, settings, make_event(2, "order.created", order=1))
    older = [r for r in await rows(pool, "event_id = 'evt_2' AND destination = 'warehouse'")][0]
    for _ in range(10):
        c = await outbox.claim(pool, 30)
        assert c is None or c.id != older["id"]  # held back while evt_5 is in flight
    assert await outbox.mark_delivered(pool, newer, [], "send")
    got = None
    while (c := await outbox.claim(pool, 30)) is not None:
        if c.id == older["id"]:
            got = c
    assert got is not None  # claimable now (and the worker will supersede it)
    await http.aclose()


async def test_H1_a_late_earlier_event_still_goes_out_in_order_if_the_later_one_is_not_delivered_yet(
    pool, settings
):
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    await ingest(pool, settings, make_event(5, "order.paid", order=1))  # not delivered yet
    await ingest(pool, settings, make_event(2, "order.created", order=1))  # arrives second, happened first
    await drain(worker, pool)
    for dest in DESTS:
        assert fake.committed[dest] == [key(2, dest), key(5, dest)]
    await http.aclose()


# ---- H2: only the duplicate-number 409 for OUR key counts as delivered -----------------------------


async def accounting_with_409(pool, settings, body):
    fake = FakeProviders()
    orig = fake.handler

    def handler(request):
        if request.url.path == "/accounting/v1/documents" and request.method == "POST":
            if request.headers.get("authorization", "")[7:] in fake.tokens:
                return httpx.Response(409, json=body(request))
        return orig(request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    worker = Worker("w", pool, build_clients(settings, gate(), http), settings)
    await ingest(pool, settings, make_event(1))
    await drain(worker, pool)
    await http.aclose()
    return fake, [r for r in await rows(pool) if r["destination"] == "accounting"][0]


async def test_H2_a_409_that_is_not_a_duplicate_is_dead_not_delivered(pool, settings):
    fake, acc = await accounting_with_409(pool, settings, lambda r: {"error": "accounting_period_closed"})
    assert acc["status"] == "dead" and acc["dead_reason"] == "http_409"
    assert "accounting_period_closed" in acc["dead_body"]
    assert fake.count("accounting", key(1, "accounting")) == 0


async def test_H2_a_duplicate_409_for_someone_elses_number_is_dead(pool, settings):
    _, acc = await accounting_with_409(
        pool, settings, lambda r: {"error": "duplicate_document_number", "number": "not-our-key"}
    )
    assert acc["status"] == "dead" and acc["dead_reason"] == "http_409"


async def test_H2_the_real_duplicate_409_still_counts_as_delivered(pool, settings):
    _, acc = await accounting_with_409(
        pool, settings, lambda r: {"error": "duplicate_document_number", "number": key(1, "accounting")}
    )
    assert acc["status"] == "delivered" and acc["delivered_via"] == "duplicate_ack"


# ---- H3: a lookup must find a record carrying our key, a 200 alone proves nothing -------------------


async def warehouse_lookup_returns(pool, settings, body):
    fake = FakeProviders()
    fake.script["warehouse"] = ["timeout_before"]
    fake.get_filter = lambda request: httpx.Response(200, json=body)
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake.handler))
    worker = Worker("w", pool, build_clients(settings, gate(), http), settings)
    await ingest(pool, settings, make_event(1))
    await drain(worker, pool)
    await http.aclose()
    return fake, (await wh_rows(pool))["evt_1"]


@pytest.mark.parametrize(
    "body",
    [{"items": []}, {"items": [], "found": False}, {"found": False}, {"items": [{"external_ref": "other"}]}],
)
async def test_H3_an_empty_or_foreign_lookup_answer_is_not_found_so_the_shipment_is_sent(
    pool, settings, body
):
    fake, wh = await warehouse_lookup_returns(pool, settings, body)
    assert wh["status"] == "delivered" and wh["delivered_via"] == "send"
    assert fake.count("warehouse", key(1, "warehouse")) == 1  # was silently lost before


async def test_H3_a_garbage_lookup_answer_never_means_delivered(pool, settings):
    fake = FakeProviders()
    fake.script["warehouse"] = ["timeout_before"]
    state = {"n": 0}

    def garbage_then_truth(request):
        state["n"] += 1
        if state["n"] == 1:
            return httpx.Response(200, text="<html>maintenance</html>")
        return None

    fake.get_filter = garbage_then_truth
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake.handler))
    worker = Worker("w", pool, build_clients(settings, gate(), http), settings)
    await ingest(pool, settings, make_event(1))
    await drain(worker, pool)
    wh = (await wh_rows(pool))["evt_1"]
    assert wh["status"] == "delivered" and fake.count("warehouse", key(1, "warehouse")) == 1
    await http.aclose()


# ---- H4: an outage must not dead-letter rows ---------------------------------------------------------


async def test_H4_an_outage_of_any_length_does_not_dead_letter_a_lone_delivery(pool, settings):
    """Probes while the breaker is open or half-open spend no attempts: the row outlives a long outage."""
    settings.max_attempts = 3
    fake = FakeProviders()
    fake.down = {"warehouse"}
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake.handler))
    cfg = gate(breaker_threshold=2, breaker_base_s=0.01, breaker_cap_s=0.03, probe_interval_s=0.005)
    worker = Worker("w", pool, build_clients(settings, cfg, http), settings)
    await ingest(pool, settings, make_event(1))
    loop = asyncio.get_running_loop()
    end = loop.time() + 30
    while len([c for c in fake.calls if c[0] == "warehouse"]) < 60 and loop.time() < end:  # 20x the budget
        await worker.run_once()
        await asyncio.sleep(0.01)
    wh = (await wh_rows(pool))["evt_1"]
    assert wh["status"] == "pending", (wh["status"], wh["dead_reason"])  # still alive after 60 failed calls
    assert wh["uncounted"] >= 20 and wh["attempts"] - wh["uncounted"] <= settings.max_attempts
    fake.down = set()  # the destination comes back
    await drain(worker, pool, limit_s=30)
    wh = (await wh_rows(pool))["evt_1"]
    assert wh["status"] == "delivered" and fake.count("warehouse", key(1, "warehouse")) == 1
    await http.aclose()


async def test_H4_failures_without_an_open_breaker_still_exhaust_the_budget(pool, settings):
    """The refund is only for outages: a destination that fails one request in a healthy period is still bounded."""
    settings.max_attempts = 3
    fake = FakeProviders()
    fake.script["warehouse"] = ["500"] * 20
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake.handler))
    worker = Worker("w", pool, build_clients(settings, gate(breaker_threshold=1000), http), settings)
    await ingest(pool, settings, make_event(1))
    await drain(worker, pool, limit_s=30)
    wh = (await wh_rows(pool))["evt_1"]
    assert wh["status"] == "dead" and wh["dead_reason"] == "max_attempts_exceeded" and wh["attempts"] == 3
    await http.aclose()


def test_H4_production_defaults_ride_out_a_ten_minute_outage():
    """With the shipped defaults the cycle budget covers hours of outage at the breaker's cooldown cap."""
    s = Settings(_env_file=None)
    assert s.max_total_attempts * s.breaker_cap_s >= 10 * 60 * 6  # an hour of probing, six times the ask
    assert s.max_attempts >= 5


# ---- H5: the documented window ----------------------------------------------------------------------


async def test_H5_a_slow_commit_that_lands_before_the_lookup_is_found_and_order_holds(pool, settings):
    """Within the bound (commit time < timeout + settle delay): the create is found, not resent, cancel follows."""
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    fake.script["warehouse"] = ["timeout_slow_commit"]
    await ingest(pool, settings, make_event(1, "order.created", order=1))
    await ingest(pool, settings, make_event(2, "order.cancelled", order=1))
    await worker.run_once()
    await worker.run_once()
    await worker.run_once()  # all three destinations tried the create once; warehouse timed out
    fake.land_pending()  # the settle delay passes; the slow request commits
    await drain(worker, pool)
    assert fake.committed["warehouse"] == [key(1, "warehouse"), key(2, "warehouse")]
    assert (await wh_rows(pool))["evt_1"]["delivered_via"] == "reconcile"
    await http.aclose()


async def test_H5_documented_residual_window_beyond_the_bound(pool, settings):
    """KNOWN LIMIT, pinned on purpose. If the warehouse needs longer than timeout + settle to commit, the lookup
    finds nothing, the hub resends, and the original lands later: prepare, cancel, prepare. The hub cannot
    prevent it (no fence at the destination); the chaos verdict reports it as a failure, never a pass."""
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    k1, k2 = key(1, "warehouse"), key(2, "warehouse")
    fake.script["warehouse"] = ["timeout_slow_commit"]
    await ingest(pool, settings, make_event(1, "order.created", order=1))
    await ingest(pool, settings, make_event(2, "order.cancelled", order=1))
    await drain(worker, pool)  # lookup 404 (the original has not committed), resend, cancel
    assert fake.committed["warehouse"] == [k1, k2]
    fake.land_pending()  # the original finally commits, after the cancel
    assert fake.committed["warehouse"] == [k1, k2, k1]
    await http.aclose()


# ---- settle and "not processed" classification --------------------------------------------------------


async def delay_after(pool, settings, action, dest="warehouse"):
    settings.ambiguous_settle_s = 5.0
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    fake.script[dest] = [action]
    await ingest(pool, settings, make_event(1))
    for _ in range(3):
        await worker.run_once()
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT extract(epoch from next_attempt_at - now()) AS wait, needs_reconcile FROM deliveries "
            "WHERE destination = %s",
            (dest,),
        )
        row = await cur.fetchone()
    await http.aclose()
    return fake, row


@pytest.mark.parametrize("action", ["502", "timeout_before"])
async def test_a_gateway_error_or_timeout_waits_the_settle_delay_before_the_lookup(pool, settings, action):
    _, row = await delay_after(pool, settings, action)
    assert row["wait"] > 4.0 and row["needs_reconcile"] is True


async def test_a_plain_500_does_not_wait_the_settle_delay(pool, settings):
    _, row = await delay_after(pool, settings, "500")
    assert row["wait"] < 1.0 and row["needs_reconcile"] is True


async def test_503_with_retry_after_is_not_processed_so_no_lookup_is_needed(pool, settings):
    fake, row = await delay_after(pool, settings, "503_retry_after")
    assert row["needs_reconcile"] is False and row["wait"] < 1.0


# ---- H6: ingest racing mark_dead -----------------------------------------------------------------------


async def test_H6_an_ingest_racing_a_dead_lettering_cannot_leave_a_row_pending_behind_a_dead_one(
    pool, settings, test_db_url
):
    import psycopg
    from psycopg.rows import dict_row

    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    await ingest(pool, settings, make_event(1, "order.created", order=1))
    c = await outbox.claim(pool, 30)
    while c.destination != "warehouse":
        c = await outbox.claim(pool, 30)
    ev2 = make_event(2, "order.paid", order=1)
    a = await psycopg.AsyncConnection.connect(test_db_url, row_factory=dict_row)
    try:
        async with a.transaction():
            await a.execute(
                "INSERT INTO events (event_id, event_type, order_id, occurred_at, raw_body, payload, body_sha256) "
                "VALUES ('evt_2', 'order.paid', 'ord_1', %s, '{}', %s, 'x')",
                (datetime.fromisoformat(ev2["created_at"]), Jsonb(ev2)),
            )
            await outbox.insert_delivery(
                a, "evt_2", "warehouse", "ord_1", key(2, "warehouse")
            )  # takes the lock
            dead = asyncio.create_task(
                outbox.mark_dead(pool, c, [], reason="http_422", body="x", status_code=422)
            )
            await asyncio.sleep(0.4)
            assert not dead.done()  # mark_dead waits for the ingest: they are serialised
        assert await asyncio.wait_for(dead, 5)
    finally:
        await a.close()
    r2 = (await wh_rows(pool))["evt_2"]
    assert r2["status"] == "blocked"  # not pending forever behind a dead row
    await http.aclose()


async def test_H6_ingest_after_the_dead_lettering_is_born_blocked(pool, settings):
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    await ingest(pool, settings, make_event(1, "order.created", order=1))
    c = await outbox.claim(pool, 30)
    while c.destination != "warehouse":
        c = await outbox.claim(pool, 30)
    assert await outbox.mark_dead(pool, c, [], reason="http_422", body="x", status_code=422)
    await ingest(pool, settings, make_event(2, "order.paid", order=1))
    assert (await wh_rows(pool))["evt_2"]["status"] == "blocked"
    await http.aclose()


async def test_H6_concurrent_ingests_and_dead_letterings_never_strand_a_pending_row(pool, settings):
    """Hammer the race: many events for one order arrive while the first is dead-lettered."""
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    await ingest(pool, settings, make_event(1, "order.created", order=1))
    c = await outbox.claim(pool, 30)
    while c.destination != "warehouse":
        c = await outbox.claim(pool, 30)
    jobs = [ingest(pool, settings, make_event(n, "order.paid", order=1)) for n in range(2, 12)]
    jobs.append(outbox.mark_dead(pool, c, [], reason="http_422", body="x", status_code=422))
    await asyncio.gather(*jobs)
    states = {r["event_id"]: r["status"] for r in await rows(pool, "destination = 'warehouse'")}
    assert states["evt_1"] == "dead"
    assert {s for e, s in states.items() if e != "evt_1"} == {"blocked"}
    await http.aclose()


# ---- S1: signature header edge cases --------------------------------------------------------------------


def test_S1_non_ascii_signature_is_a_signature_error_not_a_type_error():
    with pytest.raises(SignatureError) as exc:
        verify(["secret"], b"{}", "sha256=é", "1700000000", now=1700000000)
    assert exc.value.code == "bad_signature"


async def test_S1_non_ascii_signature_header_gets_401_not_500(pool, settings):
    app = create_app(settings, pool=pool, start_workers=False)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://hub") as c:
        resp = await c.post(
            "/webhooks/storefront",
            content=b"{}",
            headers=[
                (b"x-larkfield-timestamp", b"1700000000"),
                (b"x-larkfield-signature", "sha256=é".encode("latin-1")),
            ],
        )
    assert resp.status_code == 401


# ---- operator tools: corrected replay and skip ----------------------------------------------------------


async def dead_and_blocked(pool, settings, *, mapping_error=False):
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    first = make_event(1, "order.created", order=1)
    if mapping_error:
        del first["data"]["order"]["customer"]
    for ev in (first, make_event(2, "order.paid", order=1), make_event(3, "order.cancelled", order=1)):
        await ingest(pool, settings, ev)
    if not mapping_error:
        fake.script["accounting"] = ["422"]
    await drain(worker, pool)
    return fake, worker, http


async def api(pool, settings):
    app = create_app(settings, pool=pool, start_workers=False)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://hub")


async def test_replay_with_a_corrected_event_fixes_a_poison_message_and_releases_the_order(pool, settings):
    fake, worker, http = await dead_and_blocked(pool, settings, mapping_error=True)
    dead = [r for r in await rows(pool, "event_id = 'evt_1'")]
    assert {r["status"] for r in dead} == {"dead"} and {r["dead_reason"] for r in dead} == {"mapping_error"}
    fixed = make_event(1, "order.created", order=1)  # the corrected storefront event, same id and type
    async with await api(pool, settings) as c:
        target = next(r for r in dead if r["destination"] == "warehouse")
        resp = await c.post(
            f"/deliveries/{target['id']}/replay",
            json={"corrected_event": fixed, "reason": "customer record was missing"},
            headers={**ADMIN, "X-Actor": "dana"},
        )
        assert resp.status_code == 200
        detail = (await c.get(f"/deliveries/{target['id']}", headers=ADMIN)).json()
    await drain(worker, pool)
    wh = await wh_rows(pool)
    assert [wh[e]["status"] for e in ("evt_1", "evt_2", "evt_3")] == ["delivered"] * 3
    assert fake.committed["warehouse"] == [key(1, "warehouse"), key(2, "warehouse"), key(3, "warehouse")]
    assert detail["corrected"] is True
    assert (detail["audit"][0]["action"], detail["audit"][0]["actor"]) == ("replay_corrected", "dana")
    assert detail["audit"][0]["reason"] == "customer record was missing"
    await http.aclose()


async def test_a_corrected_event_must_keep_the_event_id_and_type(pool, settings):
    _, _, http = await dead_and_blocked(pool, settings, mapping_error=True)
    dead = next(r for r in await rows(pool, "event_id = 'evt_1' AND destination = 'warehouse'"))
    async with await api(pool, settings) as c:
        for bad in (make_event(99, "order.created", order=1), make_event(1, "order.paid", order=1)):
            resp = await c.post(
                f"/deliveries/{dead['id']}/replay", json={"corrected_event": bad}, headers=ADMIN
            )
            assert resp.status_code == 422, resp.text
    assert (await rows(pool, f"id = {dead['id']}"))[0]["status"] == "dead"
    await http.aclose()


async def test_skip_releases_the_blocked_rows_and_is_audited(pool, settings):
    fake, worker, http = await dead_and_blocked(pool, settings)
    acc = {r["event_id"]: r for r in await rows(pool, "destination = 'accounting'")}
    assert [acc[e]["status"] for e in ("evt_1", "evt_2", "evt_3")] == ["dead", "blocked", "blocked"]
    async with await api(pool, settings) as c:
        no_reason = await c.post(f"/deliveries/{acc['evt_1']['id']}/skip", json={}, headers=ADMIN)
        assert no_reason.status_code == 422  # a reason is mandatory
        resp = await c.post(
            f"/deliveries/{acc['evt_1']['id']}/skip",
            json={"reason": "invoice raised by hand"},
            headers={**ADMIN, "X-Actor": "dana"},
        )
        assert resp.status_code == 200
        detail = (await c.get(f"/deliveries/{acc['evt_1']['id']}", headers=ADMIN)).json()
        again = await c.post(
            f"/deliveries/{acc['evt_1']['id']}/skip", json={"reason": "again"}, headers=ADMIN
        )
        assert again.status_code == 409
        m = (await c.get("/metrics", headers=ADMIN)).json()["destinations"]["accounting"]
    await drain(worker, pool)
    acc = {r["event_id"]: r for r in await rows(pool, "destination = 'accounting'")}
    assert [acc[e]["status"] for e in ("evt_1", "evt_2", "evt_3")] == ["skipped", "delivered", "delivered"]
    assert fake.committed["accounting"] == [
        key(2, "accounting"),
        key(3, "accounting"),
    ]  # the skipped event never went
    assert (detail["audit"][0]["action"], detail["audit"][0]["actor"], detail["audit"][0]["reason"]) == (
        "skip",
        "dana",
        "invoice raised by hand",
    )
    assert m["skipped"] == 1
    await http.aclose()


async def test_skip_only_applies_to_dead_rows(pool, settings):
    fake, worker, http = await dead_and_blocked(pool, settings)
    blocked = next(r for r in await rows(pool, "status = 'blocked'"))
    delivered = next(r for r in await rows(pool, "status = 'delivered'"))
    async with await api(pool, settings) as c:
        for row in (blocked, delivered):
            resp = await c.post(f"/deliveries/{row['id']}/skip", json={"reason": "nope nope"}, headers=ADMIN)
            assert resp.status_code == 409
        assert (
            await c.post("/deliveries/9999/skip", json={"reason": "nope nope"}, headers=ADMIN)
        ).status_code == 404
    await http.aclose()


async def test_skip_does_not_release_rows_that_still_have_another_dead_row_ahead(pool, settings):
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    for n in (1, 2, 3):
        await ingest(pool, settings, make_event(n, order=1, **({"currency": "US$"} if n < 3 else {})))
    fake.script["accounting"] = ["422", "422"]
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")
    # evt_1 dies; force evt_2 dead as well by hand to model two dead rows ahead of evt_3
    await drain(worker, pool)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE deliveries SET status = 'dead', dead_at = now(), dead_reason = 'http_422', dead_body = 'x' "
            "WHERE destination = 'accounting' AND event_id = 'evt_2'"
        )
        await conn.execute(
            "UPDATE deliveries SET status = 'blocked' WHERE destination = 'accounting' AND event_id = 'evt_3'"
        )
    first = next(r for r in await rows(pool, "destination = 'accounting' AND event_id = 'evt_1'"))
    assert await outbox.skip(pool, first["id"], actor="t", reason="first one")
    still = next(r for r in await rows(pool, "destination = 'accounting' AND event_id = 'evt_3'"))
    assert still["status"] == "blocked"  # evt_2 is still dead ahead of it
    await http.aclose()


async def test_the_runbook_flow_for_a_blocked_order_works_end_to_end_with_the_cli_functions(pool, settings):
    """list blocked -> find the dead one ahead -> replay it: the blocked rows follow, in order."""
    fake, worker, http = await dead_and_blocked(pool, settings)
    async with await api(pool, settings) as c:
        blocked = (
            await c.get(
                "/deliveries", params={"status": "blocked", "destination": "accounting"}, headers=ADMIN
            )
        ).json()
        assert [i["event_id"] for i in blocked["items"]] == ["evt_2", "evt_3"]
        dead = (await c.get("/deliveries", params={"status": "dead"}, headers=ADMIN)).json()["items"][0]
        assert dead["order_id"] == blocked["items"][0]["order_id"]  # same order: that is what blocks them
        assert (await c.post(f"/deliveries/{dead['id']}/replay", headers=ADMIN)).status_code == 200
    await drain(worker, pool)
    assert fake.committed["accounting"] == [key(1, "accounting"), key(2, "accounting"), key(3, "accounting")]
    await http.aclose()


# ---- replay vs the helpdesk idempotency window -------------------------------------------------------------


async def aged_helpdesk_dead_row(pool, settings, hours):
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    await ingest(pool, settings, make_event(1))
    fake.script["helpdesk"] = ["422"]
    await drain(worker, pool)
    row = next(r for r in await rows(pool, "destination = 'helpdesk'"))
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE delivery_attempts SET started_at = started_at - make_interval(hours => %s) WHERE delivery_id = %s",
            (hours, row["id"]),
        )
    return fake, worker, http, row


async def test_replay_of_an_old_helpdesk_row_is_refused_because_the_idempotency_key_has_expired(
    pool, settings
):
    fake, worker, http, row = await aged_helpdesk_dead_row(pool, settings, 30)
    async with await api(pool, settings) as c:
        resp = await c.post(f"/deliveries/{row['id']}/replay", headers=ADMIN)
        assert (
            resp.status_code == 409
            and "idempotency" in resp.json()["detail"]
            and "force" in resp.json()["detail"]
        )
        assert (await rows(pool, f"id = {row['id']}"))[0]["status"] == "dead"
        forced = await c.post(
            f"/deliveries/{row['id']}/replay", json={"force": True, "reason": "known safe"}, headers=ADMIN
        )
        assert forced.status_code == 200
    await http.aclose()


async def test_replay_of_a_recent_helpdesk_row_is_allowed(pool, settings):
    fake, worker, http, row = await aged_helpdesk_dead_row(pool, settings, 2)
    async with await api(pool, settings) as c:
        assert (await c.post(f"/deliveries/{row['id']}/replay", headers=ADMIN)).status_code == 200
    await http.aclose()


async def test_bulk_replay_reports_refused_rows_separately(pool, settings):
    fake, worker, http, row = await aged_helpdesk_dead_row(pool, settings, 30)
    result = await outbox.replay(pool, destination="helpdesk", idempotency_ttl_s={"helpdesk": 24 * 3600.0})
    assert result.replayed == [] and result.refused[0][0] == row["id"]
    forced = await outbox.replay(
        pool, destination="helpdesk", force=True, idempotency_ttl_s={"helpdesk": 24 * 3600.0}
    )
    assert forced.replayed == [row["id"]]
    await http.aclose()


async def test_warehouse_and_accounting_replays_are_never_refused_by_age(pool, settings):
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    await ingest(pool, settings, make_event(1))
    fake.script["warehouse"] = ["422"]
    fake.script["accounting"] = ["422"]
    await drain(worker, pool)
    async with pool.connection() as conn:
        await conn.execute("UPDATE delivery_attempts SET started_at = started_at - interval '300 hours'")
    result = await outbox.replay(pool, idempotency_ttl_s={"helpdesk": 24 * 3600.0})
    assert len(result.replayed) == 2 and result.refused == []
    await http.aclose()


# ---- security settings -----------------------------------------------------------------------------------


ROOT = Path(__file__).resolve().parent.parent


def test_postgres_is_bound_to_loopback_with_a_password_from_env():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert '"127.0.0.1:5434:5432"' in compose
    assert "${HUB_DB_PASSWORD:?" in compose  # required, no default
    assert "POSTGRES_PASSWORD: hub" not in compose and "hub:hub@" not in compose


def test_no_default_database_password_in_code_or_examples():
    for path in ("hub/config.py", "tests/conftest.py", "chaos/run.py", ".env.example", "README.md"):
        text = (ROOT / path).read_text(encoding="utf-8")
        assert "hub:hub@" not in text, path
    assert Settings(_env_file=None).database_url == ""  # the hub refuses to start without one


async def test_empty_database_url_is_refused_clearly():
    from hub.db import open_pool

    with pytest.raises(RuntimeError, match="HUB_DATABASE_URL"):
        await open_pool("")
