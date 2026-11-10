from __future__ import annotations

import asyncio

import pytest

from hub import outbox
from hub.mapping import delivery_key
from hub.worker import SimulatedCrash
from tests.helpers import FakeProviders, build_worker, drain, ingest, make_event, rows

pytestmark = pytest.mark.anyio

DESTS = ["accounting", "helpdesk", "warehouse"]


def key(n: int, dest: str) -> str:
    return delivery_key(f"evt_{n}", dest)


async def setup(pool, settings, events=1, **kw):
    fake = FakeProviders()
    worker, http, clients = build_worker(pool, settings, fake, **kw)
    for ev in events if isinstance(events, list) else [make_event(n) for n in range(1, events + 1)]:
        await ingest(pool, settings, ev)
    return fake, worker, http


async def by_dest(pool, n: int = 1) -> dict[str, dict]:
    return {r["destination"]: r for r in await rows(pool, f"event_id = 'evt_{n}'")}


async def test_happy_path_delivers_once_to_every_destination(pool, settings):
    fake, worker, http = await setup(pool, settings, events=3)
    await drain(worker, pool)
    for n in (1, 2, 3):
        for dest in DESTS:
            assert fake.count(dest, key(n, dest)) == 1
    assert {r["status"] for r in await rows(pool)} == {"delivered"}
    assert all(r["delivered_via"] == "send" and r["attempts"] == 1 for r in await rows(pool))
    await http.aclose()


async def test_helpdesk_gets_the_delivery_key_as_idempotency_key_and_others_do_not(pool, settings):
    fake, worker, http = await setup(pool, settings)
    await drain(worker, pool)
    sent = {dest: h for dest, h in fake.headers if h.get("content-type")}
    assert sent["helpdesk"]["idempotency-key"] == key(1, "helpdesk")
    assert "idempotency-key" not in sent["warehouse"] and "idempotency-key" not in sent["accounting"]
    await http.aclose()


async def test_5xx_is_retried_with_backoff_then_delivered(pool, settings):
    fake, worker, http = await setup(pool, settings)
    fake.script["helpdesk"] = ["500", "503"]
    await drain(worker, pool)
    row = (await by_dest(pool))["helpdesk"]
    assert row["status"] == "delivered" and row["attempts"] == 3
    assert fake.count("helpdesk", key(1, "helpdesk")) == 1
    await http.aclose()


async def test_every_attempt_is_stored_with_status_latency_and_error_class(pool, settings):
    fake, worker, http = await setup(pool, settings)
    fake.script["helpdesk"] = ["500", "timeout_before"]
    await drain(worker, pool)
    row = (await by_dest(pool))["helpdesk"]
    async with pool.connection() as conn:
        log = await (
            await conn.execute(
                "SELECT * FROM delivery_attempts WHERE delivery_id = %s ORDER BY id", (row["id"],)
            )
        ).fetchall()
    assert [(a["kind"], a["status_code"], a["error_class"], a["outcome"]) for a in log] == [
        ("send", 500, None, "retry"),
        ("send", None, "timeout", "retry"),
        ("send", 201, None, "success"),
    ]
    assert all(a["latency_ms"] is not None for a in log)
    await http.aclose()


async def test_422_goes_straight_to_dead_with_the_response_body_and_no_retry(pool, settings):
    fake, worker, http = await setup(pool, settings)
    fake.script["accounting"] = ["422"]
    await drain(worker, pool)
    row = (await by_dest(pool))["accounting"]
    assert row["status"] == "dead" and row["attempts"] == 1
    assert row["dead_reason"] == "http_422" and "validation_failed" in row["dead_body"]
    assert fake.count("accounting", key(1, "accounting")) == 0
    assert len([c for c in fake.calls if c[0] == "accounting"]) == 1
    await http.aclose()


async def test_exhausted_retries_dead_letter_with_the_last_error(pool, settings):
    fake, worker, http = await setup(pool, settings)
    fake.script["helpdesk"] = ["500"] * 10
    await drain(worker, pool)
    row = (await by_dest(pool))["helpdesk"]
    assert row["status"] == "dead" and row["attempts"] == settings.max_attempts
    assert row["dead_reason"] == "max_attempts_exceeded" and "boom" in row["dead_body"]
    await http.aclose()


async def test_429_does_not_spend_the_retry_budget(pool, settings):
    fake, worker, http = await setup(pool, settings)
    fake.script["helpdesk"] = ["429"] * 6  # more than max_attempts
    await drain(worker, pool)
    row = (await by_dest(pool))["helpdesk"]
    assert row["status"] == "delivered" and row["uncounted"] == 6 and row["attempts"] == 7
    await http.aclose()


async def test_token_expiry_mid_retry_refreshes_and_still_delivers_once(pool, settings):
    fake, worker, http = await setup(pool, settings, events=2)
    await worker.run_once()  # warms the token cache
    fake.expire_tokens()
    await drain(worker, pool)
    for n in (1, 2):
        assert fake.count("warehouse", key(n, "warehouse")) == 1
        assert fake.count("accounting", key(n, "accounting")) == 1
    await http.aclose()


async def test_mapping_failure_is_a_poison_message_dead_at_once_without_a_network_call(pool, settings):
    ev = make_event(1)
    del ev["data"]["order"]["customer"]
    fake, worker, http = await setup(pool, settings, events=[ev])
    await drain(worker, pool)
    assert {r["status"] for r in await rows(pool)} == {"dead"}
    assert {r["dead_reason"] for r in await rows(pool)} == {"mapping_error"}
    assert fake.calls == []
    await http.aclose()


async def test_poison_message_does_not_block_the_queue(pool, settings):
    bad = make_event(1)
    del bad["data"]["order"]["customer"]
    fake, worker, http = await setup(pool, settings, events=[bad, make_event(2), make_event(3)])
    await drain(worker, pool)
    for n in (2, 3):
        assert fake.count("warehouse", key(n, "warehouse")) == 1
    await http.aclose()


# ---- exactly-once in effect: the ambiguous outcomes -----------------------------------------------


@pytest.mark.parametrize("action", ["timeout_after_commit", "lost_502"])
async def test_warehouse_accept_then_timeout_is_found_by_lookup_not_resent(pool, settings, action):
    fake, worker, http = await setup(pool, settings)
    fake.script["warehouse"] = [action]
    await drain(worker, pool)
    ref = key(1, "warehouse")
    assert fake.count("warehouse", ref) == 1
    row = (await by_dest(pool))["warehouse"]
    assert row["status"] == "delivered" and row["delivered_via"] == "reconcile"
    assert [c[1] for c in fake.calls if c[0] == "warehouse"] == ["POST", "GET"]
    await http.aclose()


async def test_warehouse_timeout_before_commit_is_resent_after_lookup_says_no(pool, settings):
    fake, worker, http = await setup(pool, settings)
    fake.script["warehouse"] = ["timeout_before"]
    await drain(worker, pool)
    assert fake.count("warehouse", key(1, "warehouse")) == 1
    assert [c[1] for c in fake.calls if c[0] == "warehouse"] == ["POST", "GET", "POST"]
    await http.aclose()


async def test_connection_refused_is_resent_without_a_lookup(pool, settings):
    """The request never left, so there is nothing to reconcile."""
    fake, worker, http = await setup(pool, settings)
    fake.script["warehouse"] = ["conn_refused"]
    await drain(worker, pool)
    assert [c[1] for c in fake.calls if c[0] == "warehouse"] == ["POST", "POST"]
    assert fake.count("warehouse", key(1, "warehouse")) == 1
    await http.aclose()


async def test_helpdesk_accept_then_timeout_is_deduped_by_idempotency_key(pool, settings):
    fake, worker, http = await setup(pool, settings)
    fake.script["helpdesk"] = ["timeout_after_commit"]
    await drain(worker, pool)
    assert fake.count("helpdesk", key(1, "helpdesk")) == 1
    assert len([c for c in fake.calls if c[0] == "helpdesk"]) == 2  # resent, destination ignored the repeat
    await http.aclose()


async def test_accounting_409_duplicate_counts_as_delivered(pool, settings):
    fake, worker, http = await setup(pool, settings)
    fake.script["accounting"] = ["timeout_after_commit"]
    await drain(worker, pool)
    row = (await by_dest(pool))["accounting"]
    assert row["status"] == "delivered" and row["delivered_via"] == "duplicate_ack"
    assert fake.count("accounting", key(1, "accounting")) == 1
    await http.aclose()


async def test_worker_dies_after_destination_accepted_and_recovery_does_not_double_send(pool, settings):
    """The worst window: committed at the destination, process gone before the hub recorded it."""
    settings.chaos_crash_rate = 1.0
    settings.lease_s = 5.0
    fake, worker, http = await setup(pool, settings)
    worker.rng.seed(1)
    with pytest.raises(SimulatedCrash):
        for _ in range(10):
            await worker.run_once()
    crashed = [r for r in await rows(pool) if r["status"] == "in_flight"]
    assert len(crashed) == 1
    dest = crashed[0]["destination"]
    assert fake.count(dest, crashed[0]["delivery_key"]) == 1  # accepted, but the hub never recorded it

    settings.chaos_crash_rate = 0.0
    async with pool.connection() as conn:  # the lease expires (we do not wait 5 s)
        await conn.execute(
            "UPDATE deliveries SET next_attempt_at = now() - interval '1 second' WHERE id = %s",
            (crashed[0]["id"],),
        )
    await drain(worker, pool)
    assert fake.count(dest, crashed[0]["delivery_key"]) == 1
    for d in DESTS:
        assert fake.count(d, key(1, d)) == 1
    recovered = [r for r in await rows(pool) if r["id"] == crashed[0]["id"]][0]
    assert recovered["status"] == "delivered"
    async with pool.connection() as conn:
        log = await (
            await conn.execute(
                "SELECT kind, outcome FROM delivery_attempts WHERE delivery_id = %s", (recovered["id"],)
            )
        ).fetchall()
    assert ("lease_expired", "crashed") in [(a["kind"], a["outcome"]) for a in log]
    await http.aclose()


async def test_expired_lease_without_a_lookup_destination_relies_on_idempotency(pool, settings):
    """Helpdesk has no lookup: a recovered row is resent with the same key and the destination dedupes."""
    fake, worker, http = await setup(pool, settings)
    claimed = await outbox.claim(pool, 5.0)
    while claimed and claimed.destination != "helpdesk":
        claimed = await outbox.claim(pool, 5.0)
    fake.committed["helpdesk"].append(claimed.delivery_key)  # it landed before the crash
    fake.idem[claimed.delivery_key] = True
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE deliveries SET next_attempt_at = now() - interval '1s' WHERE status = 'in_flight'"
        )
    await drain(worker, pool)
    assert fake.count("helpdesk", claimed.delivery_key) == 1
    await http.aclose()


async def test_stale_worker_cannot_overwrite_the_new_owners_result(pool, settings):
    fake, worker, http = await setup(pool, settings)
    old = await outbox.claim(pool, 5.0)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE deliveries SET next_attempt_at = now() - interval '1s' WHERE id = %s", (old.id,)
        )
    new = await outbox.claim(pool, 5.0)
    while new.id != old.id:
        new = await outbox.claim(pool, 5.0)
    assert await outbox.mark_delivered(pool, new, [], "send") is True
    assert await outbox.mark_dead(pool, old, [], reason="late", body="", status_code=None) is False
    row = [r for r in await rows(pool) if r["id"] == old.id][0]
    assert row["status"] == "delivered"
    await http.aclose()


async def test_crash_loop_is_stopped_by_the_attempt_budget(pool, settings):
    """A row whose attempts keep dying without an outcome must end up dead, not loop forever."""
    fake, worker, http = await setup(pool, settings)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE deliveries SET status = 'in_flight', attempts = %s, lease_token = gen_random_uuid(),"
            " next_attempt_at = now() - interval '1s', needs_reconcile = true WHERE destination = 'helpdesk'",
            (settings.max_attempts,),
        )
    await drain(worker, pool)
    row = (await by_dest(pool))["helpdesk"]
    assert row["status"] == "dead" and row["dead_reason"] == "max_attempts_exceeded"
    await http.aclose()


# ---- ordering and blocked rows ------------------------------------------------------------------


async def order_log(fake, dest):
    return fake.committed[dest]


async def test_cancel_is_never_sent_before_create_even_if_create_keeps_failing(pool, settings):
    events = [make_event(1, "order.created", order=1), make_event(2, "order.cancelled", order=1)]
    fake, worker, http = await setup(pool, settings, events=events)
    fake.script["warehouse"] = ["500", "500", "500"]  # create fails three times
    await drain(worker, pool)
    assert fake.committed["warehouse"] == [key(1, "warehouse"), key(2, "warehouse")]
    posts = [c for c in fake.calls if c[0] == "warehouse" and c[1] == "POST"]
    assert [p[2] for p in posts[:3]] == [key(1, "warehouse")] * 3  # cancel was not even attempted meanwhile
    await http.aclose()


async def test_later_events_move_to_blocked_behind_a_dead_one_and_are_never_sent(pool, settings):
    events = [
        make_event(1, "order.created", order=1),
        make_event(2, "order.paid", order=1),
        make_event(3, "order.cancelled", order=1),
    ]
    fake, worker, http = await setup(pool, settings, events=events)
    fake.script["accounting"] = ["422"]
    await drain(worker, pool)
    acc = {r["event_id"]: r["status"] for r in await rows(pool, "destination = 'accounting'")}
    assert acc == {"evt_1": "dead", "evt_2": "blocked", "evt_3": "blocked"}
    assert fake.committed["accounting"] == []
    for dest in ("warehouse", "helpdesk"):  # other destinations are unaffected
        assert len(fake.committed[dest]) == 3
    await http.aclose()


async def test_blocked_rows_resume_in_order_after_the_dead_one_is_replayed(pool, settings):
    events = [make_event(1, "order.created", order=1), make_event(2, "order.paid", order=1)]
    fake, worker, http = await setup(pool, settings, events=events)
    fake.script["accounting"] = ["422"]
    await drain(worker, pool)
    dead = (await rows(pool, "status = 'dead'"))[0]
    assert (await outbox.replay(pool, ids=[dead["id"]])).replayed == [dead["id"]]
    await drain(worker, pool)
    assert fake.committed["accounting"] == [key(1, "accounting"), key(2, "accounting")]
    assert {r["status"] for r in await rows(pool)} == {"delivered"}
    await http.aclose()


async def test_orders_do_not_block_each_other(pool, settings):
    events = [make_event(1, order=1), make_event(2, order=2), make_event(3, "order.paid", order=1)]
    fake, worker, http = await setup(pool, settings, events=events)
    fake.script["accounting"] = ["422"]  # order 1's create dies at accounting
    await drain(worker, pool)
    assert key(2, "accounting") in fake.committed["accounting"]
    assert {r["event_id"]: r["status"] for r in await rows(pool, "destination = 'accounting'")} == {
        "evt_1": "dead",
        "evt_2": "delivered",
        "evt_3": "blocked",
    }
    await http.aclose()


async def test_ordering_is_by_event_time_not_arrival(pool, settings):
    """A late-arriving earlier event is delivered before a later one that has not been sent yet."""
    fake = FakeProviders()
    worker, http, _ = build_worker(pool, settings, fake)
    await ingest(pool, settings, make_event(5, "order.cancelled", order=1))
    await ingest(pool, settings, make_event(2, "order.created", order=1))  # arrives second, happened first
    await drain(worker, pool)
    assert fake.committed["warehouse"] == [key(2, "warehouse"), key(5, "warehouse")]
    await http.aclose()


# ---- concurrency ----------------------------------------------------------------------------------


async def test_many_workers_never_claim_the_same_row_twice(pool, settings):
    fake = FakeProviders()
    for n in range(1, 31):
        await ingest(pool, settings, make_event(n))
    claimed: list[int] = []

    async def grab():
        while (c := await outbox.claim(pool, 30.0)) is not None:
            claimed.append(c.id)

    await asyncio.gather(*[grab() for _ in range(6)])
    assert len(claimed) == len(set(claimed)) == 90
    del fake


async def test_concurrent_workers_deliver_everything_exactly_once(pool, settings):
    fake = FakeProviders()
    workers = [build_worker(pool, settings, fake, name=f"w{i}")[0] for i in range(4)]
    for n in range(1, 16):
        await ingest(pool, settings, make_event(n))
    fake.script["warehouse"] = ["500", "timeout_after_commit", "lost_502"] * 3
    fake.script["helpdesk"] = ["timeout_after_commit", "500"] * 3

    async def run(w):
        stop = asyncio.Event()
        task = asyncio.create_task(w.run(stop))
        return stop, task

    handles = [await run(w) for w in workers]
    for _ in range(300):
        await asyncio.sleep(0.05)
        async with pool.connection() as conn:
            left = (
                await (
                    await conn.execute("SELECT count(*) AS n FROM deliveries WHERE status <> 'delivered'")
                ).fetchone()
            )["n"]
        if left == 0:
            break
    for stop, task in handles:
        stop.set()
        await task
    for n in range(1, 16):
        for d in DESTS:
            assert fake.count(d, key(n, d)) == 1, (n, d)


async def test_claim_skips_a_destination_with_no_capacity(pool, settings):
    from hub.gate import GateConfig, PgGate

    await ingest(pool, settings, make_event(1))
    gate = PgGate(pool, {d: GateConfig(rate=1.0, burst=1.0) for d in DESTS})
    await gate.ensure()
    await gate.throttled("warehouse", 60.0)
    got = set()
    while (c := await outbox.claim(pool, 30.0)) is not None:
        got.add(c.destination)
    assert got == {"accounting", "helpdesk"}


# ---- replay ----------------------------------------------------------------------------------------


async def test_replay_of_a_dead_row_reconciles_first_and_delivers_once(pool, settings):
    fake, worker, http = await setup(pool, settings)
    fake.script["warehouse"] = ["422"]
    await drain(worker, pool)
    dead = (await rows(pool, "status = 'dead'"))[0]
    fake.committed["warehouse"].append(dead["delivery_key"])  # someone fixed it by hand at the destination
    await outbox.replay(pool, ids=[dead["id"]])
    await drain(worker, pool)
    assert fake.count("warehouse", dead["delivery_key"]) == 1
    row = (await rows(pool, f"id = {dead['id']}"))[0]
    assert row["status"] == "delivered" and row["delivered_via"] == "reconcile" and row["replay_count"] == 1
    await http.aclose()


async def test_replay_refuses_rows_that_are_not_dead(pool, settings):
    fake, worker, http = await setup(pool, settings)
    await drain(worker, pool)
    delivered = (await rows(pool))[0]
    assert (await outbox.replay(pool, ids=[delivered["id"]])).replayed == []
    assert (await rows(pool, f"id = {delivered['id']}"))[0]["status"] == "delivered"
    await http.aclose()


async def test_after_a_timeout_the_lookup_waits_for_the_abandoned_request_to_settle(pool, settings):
    """Only destinations we must look up (warehouse) get the settle delay; a 5xx response does not."""
    settings.ambiguous_settle_s = 5.0
    fake, worker, http = await setup(pool, settings)
    fake.script["warehouse"] = ["timeout_before"]
    fake.script["helpdesk"] = ["timeout_before"]
    fake.script["accounting"] = ["500"]
    for _ in range(3):
        await worker.run_once()
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT destination, extract(epoch from next_attempt_at - now()) AS wait FROM deliveries"
        )
        waits = {r["destination"]: r["wait"] for r in await cur.fetchall()}
    assert waits["warehouse"] > 4.0
    assert waits["helpdesk"] < 1.0 and waits["accounting"] < 1.0
    await http.aclose()
