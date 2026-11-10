"""Per-destination gate: token bucket, Retry-After block and circuit breaker in one small state machine.

The transitions are pure functions of (state, now) so they are unit-tested without a clock or a
database. PgGate runs them inside a row lock on destination_state using the database clock, which is
what makes the limit hold across worker processes: five processes sharing 5 req/s is still 5 req/s.

Circuit breaker, in the same row: after `breaker_threshold` consecutive failures the destination is
blocked for an exponentially growing cooldown. When the cooldown ends the gate is half-open: one request
per probe interval is let through until one succeeds. This keeps a 60-second outage from burning every
queued delivery's retry budget.

Adaptive rate: a 429 means the configured rate is too high (the provider changed its limit, or the quota
is shared). Each 429 multiplies the effective rate by `decrease`; each success adds back `increase`, up to
the configured rate. The hub settles just under the real limit instead of paying a Retry-After block
every couple of seconds.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, replace
from typing import Protocol

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


@dataclass(frozen=True)
class GateConfig:
    rate: float
    burst: float
    breaker_threshold: int = 5
    breaker_base_s: float = 2.0
    breaker_cap_s: float = 30.0
    probe_interval_s: float = 1.0
    min_scale: float = 0.1
    decrease: float = 0.7
    increase: float = 0.005


@dataclass(frozen=True)
class GateState:
    tokens: float
    refilled_at: float
    blocked_until: float = 0.0
    consecutive_failures: int = 0
    rate_scale: float = 1.0  # adaptive multiplier on cfg.rate, between cfg.min_scale and 1


def new_state(cfg: GateConfig, now: float) -> GateState:
    return GateState(tokens=cfg.burst, refilled_at=now)


def refill(state: GateState, now: float, cfg: GateConfig) -> GateState:
    if now <= state.refilled_at:  # refill is paused (after a 429) or the clock went backwards
        return state
    elapsed = now - state.refilled_at
    rate = cfg.rate * state.rate_scale
    return replace(state, tokens=min(cfg.burst, state.tokens + elapsed * rate), refilled_at=now)


def try_acquire(state: GateState, now: float, cfg: GateConfig) -> tuple[GateState, float]:
    """Return (new_state, wait_seconds). wait_seconds == 0 means granted and a token was taken."""
    s = refill(state, now, cfg)
    if now < s.blocked_until:
        return s, s.blocked_until - now
    if s.tokens < 1.0:
        return s, (1.0 - s.tokens) / (cfg.rate * s.rate_scale)
    s = replace(s, tokens=s.tokens - 1.0)
    if s.consecutive_failures >= cfg.breaker_threshold:  # half-open: space the probes
        s = replace(s, blocked_until=now + cfg.probe_interval_s)
    return s, 0.0


def on_success(state: GateState, cfg: GateConfig) -> GateState:
    return replace(state, consecutive_failures=0, rate_scale=min(1.0, state.rate_scale + cfg.increase))


def on_failure(state: GateState, now: float, cfg: GateConfig) -> GateState:
    """Count an outage-type failure (5xx, timeout, connection error); open the breaker at threshold."""
    failures = state.consecutive_failures + 1
    blocked = state.blocked_until
    if failures >= cfg.breaker_threshold:
        cooldown = min(cfg.breaker_cap_s, cfg.breaker_base_s * 2 ** (failures - cfg.breaker_threshold))
        blocked = max(blocked, now + cooldown)
    return replace(state, consecutive_failures=failures, blocked_until=blocked)


def on_throttle(state: GateState, now: float, retry_after: float, cfg: GateConfig) -> GateState:
    """429: nobody sends to this destination until Retry-After has passed; the bucket restarts empty then,
    at a lower rate."""
    until = max(state.blocked_until, now + retry_after)
    scale = max(cfg.min_scale, state.rate_scale * cfg.decrease)
    return replace(state, tokens=0.0, refilled_at=until, blocked_until=until, rate_scale=scale)


def on_hold(state: GateState, now: float, seconds: float) -> GateState:
    """Retry-After on a 503: stay away for that long, without treating it as a rate-limit signal."""
    return replace(state, blocked_until=max(state.blocked_until, now + seconds))


class GateBlocked(Exception):
    """The destination is closed for longer than a worker should hold a claim."""

    def __init__(self, destination: str, wait_s: float):
        super().__init__(f"{destination} blocked for {wait_s:.1f}s")
        self.destination = destination
        self.wait_s = wait_s


class Gate(Protocol):
    async def acquire(self, destination: str) -> float: ...
    async def success(self, destination: str) -> None: ...
    async def failure(self, destination: str) -> bool: ...  # True: breaker is open or half-open
    async def throttled(self, destination: str, retry_after: float) -> None: ...
    async def hold(self, destination: str, seconds: float) -> None: ...


async def pass_gate(gate: Gate, destination: str, max_inline_wait_s: float) -> None:
    """Wait (briefly) for capacity, or raise GateBlocked if it is far away."""
    while True:
        wait = await gate.acquire(destination)
        if wait <= 0:
            return
        if wait > max_inline_wait_s:
            raise GateBlocked(destination, wait)
        await asyncio.sleep(wait + 0.005)


class MemoryGate:
    """In-process gate for tests and single-process use. Same state machine as PgGate."""

    def __init__(self, configs: dict[str, GateConfig], clock=time.time):
        self._cfg = configs
        self._clock = clock
        self._state = {name: new_state(cfg, clock()) for name, cfg in configs.items()}

    async def acquire(self, destination: str) -> float:
        self._state[destination], wait = try_acquire(
            self._state[destination], self._clock(), self._cfg[destination]
        )
        return wait

    async def success(self, destination: str) -> None:
        self._state[destination] = on_success(self._state[destination], self._cfg[destination])

    async def failure(self, destination: str) -> bool:
        cfg = self._cfg[destination]
        self._state[destination] = on_failure(self._state[destination], self._clock(), cfg)
        return self._state[destination].consecutive_failures >= cfg.breaker_threshold

    async def hold(self, destination: str, seconds: float) -> None:
        self._state[destination] = on_hold(self._state[destination], self._clock(), seconds)

    async def throttled(self, destination: str, retry_after: float) -> None:
        self._state[destination] = on_throttle(
            self._state[destination], self._clock(), retry_after, self._cfg[destination]
        )

    def state(self, destination: str) -> GateState:
        return self._state[destination]


_SELECT = (
    "SELECT tokens, refilled_at, blocked_until, consecutive_failures, rate_scale, "
    "extract(epoch from clock_timestamp())::float8 AS now "
    "FROM destination_state WHERE destination = %s FOR UPDATE"
)
_UPDATE = (
    "UPDATE destination_state SET tokens = %s, refilled_at = %s, blocked_until = %s, "
    "consecutive_failures = %s, rate_scale = %s WHERE destination = %s"
)


class PgGate:
    """Gate shared by every worker process through one row per destination."""

    def __init__(self, pool: AsyncConnectionPool, configs: dict[str, GateConfig]):
        self._pool = pool
        self._cfg = configs

    async def ensure(self) -> None:
        async with self._pool.connection() as conn:
            for name, cfg in self._cfg.items():
                await conn.execute(
                    "INSERT INTO destination_state (destination, rate, burst, tokens, refilled_at) "
                    "VALUES (%s, %s, %s, %s, extract(epoch from clock_timestamp())) "
                    "ON CONFLICT (destination) DO UPDATE SET rate = EXCLUDED.rate, burst = EXCLUDED.burst",
                    (name, cfg.rate, cfg.burst, cfg.burst),
                )

    async def _apply(self, destination: str, fn) -> float:
        async with self._pool.connection() as conn, conn.transaction():
            cur = await conn.cursor(row_factory=dict_row).execute(_SELECT, (destination,))
            row = await cur.fetchone()
            if row is None:
                raise KeyError(f"destination_state has no row for {destination}; call ensure() first")
            now = row["now"]
            state = GateState(
                row["tokens"],
                row["refilled_at"],
                row["blocked_until"],
                row["consecutive_failures"],
                row["rate_scale"],
            )
            state, result = fn(state, now, self._cfg[destination])
            await conn.execute(
                _UPDATE,
                (
                    state.tokens,
                    state.refilled_at,
                    state.blocked_until,
                    state.consecutive_failures,
                    state.rate_scale,
                    destination,
                ),
            )
            return result

    async def acquire(self, destination: str) -> float:
        return await self._apply(destination, lambda s, now, cfg: try_acquire(s, now, cfg))

    async def success(self, destination: str) -> None:
        await self._apply(destination, lambda s, now, cfg: (on_success(s, cfg), 0.0))

    async def failure(self, destination: str) -> bool:
        def step(s, now, cfg):
            s = on_failure(s, now, cfg)
            return s, 1.0 if s.consecutive_failures >= cfg.breaker_threshold else 0.0

        return await self._apply(destination, step) > 0

    async def hold(self, destination: str, seconds: float) -> None:
        await self._apply(destination, lambda s, now, cfg: (on_hold(s, now, seconds), 0.0))

    async def throttled(self, destination: str, retry_after: float) -> None:
        await self._apply(destination, lambda s, now, cfg: (on_throttle(s, now, retry_after, cfg), 0.0))
