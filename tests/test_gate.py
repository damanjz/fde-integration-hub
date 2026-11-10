from __future__ import annotations

import asyncio

import pytest

from hub.gate import (
    GateBlocked,
    GateConfig,
    GateState,
    MemoryGate,
    PgGate,
    new_state,
    on_failure,
    on_success,
    on_throttle,
    pass_gate,
    refill,
    try_acquire,
)

CFG = GateConfig(
    rate=5.0, burst=5.0, breaker_threshold=3, breaker_base_s=2.0, breaker_cap_s=10.0, probe_interval_s=1.0
)


def take(state, now, n, cfg=CFG):
    granted = 0
    for _ in range(n):
        state, wait = try_acquire(state, now, cfg)
        granted += wait == 0
    return state, granted


def test_burst_then_refill():
    s = new_state(CFG, 0.0)
    s, granted = take(s, 0.0, 8)
    assert granted == 5  # the burst
    s, wait = try_acquire(s, 0.0, CFG)
    assert wait == pytest.approx(0.2)  # next token in 1/rate
    s, granted = take(s, 1.0, 10)
    assert granted == 5  # refilled to the cap after a second, never beyond


def test_sustained_rate_never_exceeds_limit():
    s = new_state(CFG, 0.0)
    granted, t = 0, 0.0
    while t < 10.0:
        s, wait = try_acquire(s, t, CFG)
        granted += wait == 0
        t += 0.01
    assert granted <= CFG.burst + CFG.rate * 10.0 + 1
    assert granted >= CFG.rate * 10.0 * 0.95


def test_refill_never_goes_backwards():
    s = GateState(tokens=1.0, refilled_at=10.0)
    assert refill(s, 5.0, CFG).tokens == 1.0


def test_throttle_blocks_everyone_until_retry_after():
    s = new_state(CFG, 0.0)
    s = on_throttle(s, 0.0, 2.0, CFG)
    s, wait = try_acquire(s, 1.0, CFG)
    assert wait == pytest.approx(1.0)
    s, wait = try_acquire(s, 2.0, CFG)
    assert wait > 0  # bucket restarts empty after a 429
    s, wait = try_acquire(s, 2.5, CFG)
    assert wait == 0


def test_throttle_never_shortens_an_existing_block():
    s = on_throttle(new_state(CFG, 0.0), 0.0, 10.0, CFG)
    assert on_throttle(s, 1.0, 1.0, CFG).blocked_until == 10.0


def test_breaker_opens_after_threshold_with_growing_cooldown():
    s = new_state(CFG, 0.0)
    for _ in range(2):
        s = on_failure(s, 0.0, CFG)
    assert s.blocked_until == 0.0  # below threshold
    s = on_failure(s, 0.0, CFG)
    assert s.blocked_until == 2.0
    s = on_failure(s, 2.0, CFG)
    assert s.blocked_until == 2.0 + 4.0
    for _ in range(10):
        s = on_failure(s, 100.0, CFG)
    assert s.blocked_until == 100.0 + CFG.breaker_cap_s


def test_half_open_lets_one_probe_per_interval_and_success_closes():
    s = new_state(CFG, 0.0)
    for _ in range(3):
        s = on_failure(s, 0.0, CFG)
    s, wait = try_acquire(s, 1.0, CFG)
    assert wait == 1.0  # open
    s, wait = try_acquire(s, 2.0, CFG)
    assert wait == 0  # cooldown over: a probe goes out
    s, wait = try_acquire(s, 2.1, CFG)
    assert wait > 0  # the others wait for the probe
    s = on_success(s, CFG)
    assert s.consecutive_failures == 0
    s, wait = try_acquire(s, 3.2, CFG)
    assert wait == 0


def test_failed_probe_reopens_with_longer_cooldown():
    s = new_state(CFG, 0.0)
    for _ in range(3):
        s = on_failure(s, 0.0, CFG)
    s, _ = try_acquire(s, 2.0, CFG)
    s = on_failure(s, 2.0, CFG)
    assert s.blocked_until == 2.0 + 4.0


@pytest.mark.anyio
async def test_pass_gate_waits_briefly_and_raises_for_long_blocks():
    t = [0.0]
    gate = MemoryGate({"d": CFG}, clock=lambda: t[0])
    for _ in range(5):
        await pass_gate(gate, "d", 2.0)
    await gate.throttled("d", 30.0)
    with pytest.raises(GateBlocked) as exc:
        await pass_gate(gate, "d", 2.0)
    assert exc.value.wait_s == pytest.approx(30.0)


@pytest.mark.anyio
async def test_pg_gate_state_is_shared_between_gate_instances(pool):
    """Two PgGate objects stand in for two worker processes: one 429 blocks both."""
    cfgs = {"warehouse": CFG}
    a, b = PgGate(pool, cfgs), PgGate(pool, cfgs)
    await a.ensure()
    await a.throttled("warehouse", 5.0)
    wait = await b.acquire("warehouse")
    assert 3.0 < wait <= 5.0


@pytest.mark.anyio
async def test_pg_gate_limit_holds_across_concurrent_processes(pool):
    cfg = GateConfig(rate=20.0, burst=3.0)
    gates = [PgGate(pool, {"w": cfg}) for _ in range(4)]
    await gates[0].ensure()
    granted = 0

    async def hammer(gate):
        nonlocal granted
        for _ in range(60):
            if await gate.acquire("w") == 0:
                granted += 1
            await asyncio.sleep(0.005)

    start = asyncio.get_running_loop().time()
    await asyncio.gather(*[hammer(g) for g in gates])
    elapsed = asyncio.get_running_loop().time() - start
    assert granted <= cfg.burst + cfg.rate * elapsed + 1
    assert granted >= 3


def test_429_lowers_the_effective_rate_and_success_recovers_it_slowly():
    s = on_throttle(new_state(CFG, 0.0), 0.0, 1.0, CFG)
    assert s.rate_scale == pytest.approx(0.7)
    s = on_throttle(s, 5.0, 1.0, CFG)
    assert s.rate_scale == pytest.approx(0.49)
    for _ in range(10):
        s = on_success(s, CFG)
    assert s.rate_scale == pytest.approx(0.49 + 10 * CFG.increase)
    for _ in range(1000):
        s = on_success(s, CFG)
    assert s.rate_scale == 1.0  # never above the configured rate


def test_rate_scale_has_a_floor():
    s = new_state(CFG, 0.0)
    for _ in range(50):
        s = on_throttle(s, 0.0, 0.0, CFG)
    assert s.rate_scale == CFG.min_scale


def test_lower_scale_slows_refill_and_wait_estimates():
    s = GateState(tokens=0.0, refilled_at=0.0, rate_scale=0.5)
    s2, wait = try_acquire(s, 0.0, CFG)
    assert wait == pytest.approx(1.0 / (CFG.rate * 0.5))
    s3, wait = try_acquire(s, 0.2, CFG)
    assert wait > 0 and s3.tokens == pytest.approx(0.2 * CFG.rate * 0.5)


def test_adaptive_gate_converges_below_a_hidden_provider_limit():
    """Configured above the provider's real limit, the gate settles just under it instead of paying blocks."""
    cfg = GateConfig(rate=8.0, burst=2.0)
    limit, window, throttles = 5.0, [], 0
    s, t = new_state(cfg, 0.0), 0.0
    while t < 120.0:
        s, wait = try_acquire(s, t, cfg)
        if wait == 0:
            window = [x for x in window if t - x < 1.0]
            if len(window) >= limit:
                throttles += 1
                s = on_throttle(s, t, 1.0, cfg)
            else:
                window.append(t)
                s = on_success(s, cfg)
            t += 0.001
        else:
            t += max(wait, 1e-4)  # float dust: a sub-nanosecond wait would never advance the clock
    assert throttles < 40  # 120 s at a fixed 8/s would be a 429 every couple of seconds
    assert s.rate_scale < 1.0
