"""Control: ordering and exactly-once hold with four concurrent workers and injected faults."""

from __future__ import annotations

import asyncio
import random

import pytest

from hub.mapping import delivery_key
from tests.helpers import FakeProviders, build_worker, ingest, make_event, rows

pytestmark = pytest.mark.anyio
DESTS = ("warehouse", "helpdesk", "accounting")


def key(n, dest):
    return delivery_key(f"evt_{n}", dest)


async def test_ordering_holds_under_4_concurrent_workers_with_faults(pool, settings):
    settings.max_attempts = 30
    settings.max_total_attempts = 200
    fake = FakeProviders()
    workers = [build_worker(pool, settings, fake, name=f"w{i}")[0] for i in range(4)]
    n = 0
    order_of = {}
    for o in range(1, 21):
        for t in ("order.created", "order.paid", "order.cancelled"):
            n += 1
            await ingest(pool, settings, make_event(n, t, order=o))
            order_of[n] = o
    rng = random.Random(3)
    for d in DESTS:
        fake.script[d] = [
            rng.choice(["ok", "ok", "500", "timeout_after_commit", "lost_502", "timeout_before"])
            for _ in range(120)
        ]
    handles = []
    for w in workers:
        stop = asyncio.Event()
        handles.append((stop, asyncio.create_task(w.run(stop))))
    left = -1
    for _ in range(600):
        await asyncio.sleep(0.05)
        left = len(await rows(pool, "status <> 'delivered'"))
        if left == 0:
            break
    for stop, task in handles:
        stop.set()
        await task
    assert left == 0
    for d in DESTS:
        key_to_n = {key(i, d): i for i in order_of}
        seq = [key_to_n[k] for k in fake.committed[d]]
        for o in range(1, 21):
            mine = [i for i in seq if order_of[i] == o]
            assert mine == sorted(mine), (d, o, mine)
        assert len(seq) == len(set(seq)) == 60  # every event once, none twice
