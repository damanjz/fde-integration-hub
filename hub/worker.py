"""Delivery workers.

One attempt, in order:
  1. map the stored event to the destination payload (a mapping failure is a poison message: dead at once)
  2. if the previous outcome is unknown (timeout, 5xx, lost response, worker crash, replay): ask the
     destination whether it already has this delivery key. Found -> mark delivered, never send again.
  3. send; classify; record the outcome under the lease fence.

Failure modes this is built for are listed in docs/engagement.md. The one residual window (a request that
is still in flight at the destination when we look it up) is documented there too.
"""

from __future__ import annotations

import asyncio
import logging
import random
import traceback

import httpx
from psycopg_pool import AsyncConnectionPool

from hub import outbox
from hub.client import CallRecord, CallResult, DestinationClient
from hub.config import Settings
from hub.db import open_pool
from hub.destinations import build_clients, destination_names, gate_configs
from hub.gate import GateBlocked, PgGate
from hub.logging_ import configure_logging, log, set_known_secrets
from hub.mapping import MAPPERS
from hub.outbox import Claimed
from hub.retry import Outcome, backoff_delay

logger = logging.getLogger("hub.worker")


class SimulatedCrash(BaseException):
    """Test-only (HUB_CHAOS_CRASH_RATE): the worker dies after the destination accepted, before recording."""


class Worker:
    def __init__(
        self,
        name: str,
        pool: AsyncConnectionPool,
        clients: dict[str, DestinationClient],
        settings: Settings,
        *,
        rng: random.Random | None = None,
    ):
        self.name = name
        self.pool = pool
        self.clients = clients
        self.settings = settings
        self.rng = rng or random.Random()

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            worked = False
            try:
                worked = await self.run_once()
            except SimulatedCrash:
                log(logger, "worker_crashed", logging.WARNING, worker=self.name)
            except Exception:  # noqa: BLE001 - a worker must survive anything except cancellation
                log(logger, "worker_error", logging.ERROR, worker=self.name, tb=traceback.format_exc())
                await asyncio.sleep(1.0)
            if not worked:
                try:
                    await asyncio.wait_for(
                        stop.wait(), self.settings.poll_interval_s * (0.5 + self.rng.random())
                    )
                except TimeoutError:
                    pass

    async def run_once(self) -> bool:
        claimed = await outbox.claim(self.pool, self.settings.lease_s)
        if claimed is None:
            return False
        if claimed.recovered:
            log(
                logger,
                "lease_recovered",
                logging.WARNING,
                delivery_id=claimed.id,
                destination=claimed.destination,
                attempt=claimed.attempts,
            )
        calls: list[CallRecord] = []
        try:
            async with asyncio.timeout(self.settings.lease_s * 0.8):  # an attempt must end inside its lease
                await self._attempt(claimed, calls)
        except GateBlocked as blocked:
            await outbox.release(self.pool, claimed, calls, blocked.wait_s)
        except TimeoutError:
            await self._fail(
                claimed,
                calls,
                CallResult(Outcome.RETRY, None, "attempt_timeout", ambiguous=True, maybe_in_flight=True),
                True,
            )
        except SimulatedCrash:
            raise
        except Exception as exc:  # noqa: BLE001 - classify as a retryable internal error, never lose the row
            log(
                logger,
                "attempt_internal_error",
                logging.ERROR,
                delivery_id=claimed.id,
                tb=traceback.format_exc(),
            )
            await outbox.log_internal(self.pool, claimed, exc.__class__.__name__, str(exc))
            await self._fail(
                claimed, calls, CallResult(Outcome.RETRY, None, "internal_error", ambiguous=True), True
            )
        return True

    async def _attempt(self, c: Claimed, calls: list[CallRecord]) -> None:
        client = self.clients.get(c.destination)
        mapper = MAPPERS.get(c.destination)
        if client is None or mapper is None:
            await self._dead(c, calls, "unknown_destination", c.destination, None)
            return
        later_event = await outbox.supersede_if_stale(self.pool, c)
        if later_event is not None:  # never send a stale event: the destination already has a later one
            log(
                logger,
                "delivery_superseded",
                logging.WARNING,
                delivery_id=c.id,
                destination=c.destination,
                event_id=c.event_id,
                later_event_id=later_event,
            )
            return
        event = await outbox.load_event(self.pool, c)
        try:
            if event is None:
                raise ValueError("event row missing")
            request = mapper(event)
        except Exception as exc:  # noqa: BLE001 - any mapper failure is deterministic: a poison message
            await self._dead(c, calls, "mapping_error", f"{exc.__class__.__name__}: {exc}", None)
            return

        reconcile_pending = c.needs_reconcile and client.supports_lookup
        if reconcile_pending:
            found = await client.lookup(c.delivery_key)
            calls += found.calls
            if found.outcome is Outcome.SUCCESS and found.found:
                if await outbox.mark_delivered(self.pool, c, calls, "reconcile"):
                    self._log_result(c, "delivered", "reconcile", found)
                return
            if found.outcome is not Outcome.SUCCESS:
                if found.outcome is Outcome.PERMANENT:
                    found = CallResult(Outcome.RETRY, found.status_code, "reconcile_unavailable", found.body)
                await self._fail(c, calls, found, True)
                return
            reconcile_pending = False  # the destination does not have it: sending is safe

        failures_so_far = c.attempts - c.uncounted
        if failures_so_far > self.settings.max_attempts or c.attempts > self.settings.max_total_attempts:
            await self._dead(
                c,
                calls,
                "max_attempts_exceeded",
                "crash loop: attempts used up without an outcome",
                None,
                needs_reconcile=True,
            )
            return

        res = await client.send(request, c.delivery_key)
        calls += res.calls
        if res.outcome is Outcome.SUCCESS:
            if self.settings.chaos_crash_rate and self.rng.random() < self.settings.chaos_crash_rate:
                raise SimulatedCrash  # destination has it, we never recorded it: reconcile must save us
            via = "duplicate_ack" if res.duplicate else "send"
            if await outbox.mark_delivered(self.pool, c, calls, via):
                self._log_result(c, "delivered", via, res)
            else:
                log(logger, "lease_lost", logging.WARNING, delivery_id=c.id, destination=c.destination)
        elif res.outcome is Outcome.PERMANENT:
            await self._dead(c, calls, f"http_{res.status_code}", res.body, res.status_code)
        else:
            await self._fail(c, calls, res, reconcile_pending or res.ambiguous)

    async def _fail(
        self, c: Claimed, calls: list[CallRecord], res: CallResult, needs_reconcile: bool
    ) -> None:
        s = self.settings
        throttled = res.outcome is Outcome.THROTTLED
        # Cycles that spend no retry budget: a 429 (the destination asked us to wait), and a failure while the
        # circuit breaker is open or half-open (the destination is down; the breaker spaces the probes).
        uncounted = throttled or res.outage
        failures = c.attempts - c.uncounted - (1 if uncounted else 0)
        if (not uncounted and failures >= s.max_attempts) or c.attempts >= s.max_total_attempts:
            await self._dead(
                c,
                calls,
                "max_attempts_exceeded",
                res.body or res.error_class or "unknown",
                res.status_code,
                needs_reconcile=needs_reconcile,
            )
            return
        if throttled:
            retry_after = 1.0 if res.retry_after_s is None else res.retry_after_s
            delay = retry_after + self.rng.random() * 0.5  # spread the herd after a shared block
        else:
            delay = max(
                backoff_delay(max(failures, 1), s.backoff_base_s, s.backoff_cap_s, self.rng),
                res.retry_after_s or 0.0,
            )
        client = self.clients.get(c.destination)
        if res.maybe_in_flight and client is not None and client.supports_lookup:
            delay = max(delay, s.ambiguous_settle_s)  # let the abandoned request finish before we look it up
        ok = await outbox.mark_retry(
            self.pool,
            c,
            calls,
            delay_s=delay,
            error=res.error_class or f"http_{res.status_code}",
            status_code=res.status_code,
            needs_reconcile=needs_reconcile,
            uncounted=uncounted,
        )
        if ok:
            self._log_result(
                c, "throttled" if throttled else ("outage" if res.outage else "retry"), None, res, delay=delay
            )

    async def _dead(
        self,
        c: Claimed,
        calls: list[CallRecord],
        reason: str,
        body: str,
        status: int | None,
        *,
        needs_reconcile: bool = False,
    ) -> None:
        if await outbox.mark_dead(
            self.pool, c, calls, reason=reason, body=body, status_code=status, needs_reconcile=needs_reconcile
        ):
            log(
                logger,
                "delivery_dead",
                logging.WARNING,
                delivery_id=c.id,
                destination=c.destination,
                event_id=c.event_id,
                reason=reason,
                status=status,
                attempt=c.attempts,
            )

    def _log_result(
        self, c: Claimed, result: str, via: str | None, res: CallResult, delay: float | None = None
    ) -> None:
        log(
            logger,
            f"delivery_{result}",
            logging.INFO,
            delivery_id=c.id,
            destination=c.destination,
            event_id=c.event_id,
            attempt=c.attempts,
            via=via,
            status=res.status_code,
            error=res.error_class,
            delay_s=round(delay, 2) if delay is not None else None,
        )


class WorkerPool:
    """N workers sharing one DB pool, one HTTP client and one PgGate."""

    def __init__(self, settings: Settings, pool: AsyncConnectionPool, name: str = "w"):
        self.settings = settings
        self.pool = pool
        self.name = name
        self._http: httpx.AsyncClient | None = None
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def start(self, count: int) -> None:
        gate = PgGate(self.pool, gate_configs(self.settings))
        await gate.ensure()
        self._http = httpx.AsyncClient(limits=httpx.Limits(max_connections=100, max_keepalive_connections=20))
        clients = build_clients(self.settings, gate, self._http)
        for i in range(count):
            worker = Worker(f"{self.name}{i}", self.pool, clients, self.settings)
            self._tasks.append(asyncio.create_task(worker.run(self._stop), name=f"worker-{self.name}{i}"))
        log(logger, "workers_started", count=count, destinations=destination_names(self.settings))

    async def stop(self) -> None:
        self._stop.set()
        if self._tasks:
            await asyncio.wait(self._tasks, timeout=self.settings.lease_s * 0.8 + 5)
            for t in self._tasks:
                t.cancel()
        if self._http is not None:
            await self._http.aclose()


async def run_standalone(settings: Settings, count: int) -> None:
    """`python -m hub.worker`: workers without the API, e.g. a second process sharing the same queue."""
    configure_logging()
    set_known_secrets(settings.all_secret_values())
    pool = await open_pool(settings.database_url, settings.pool_size)
    workers = WorkerPool(settings, pool, name="sw")
    await workers.start(count)
    try:
        await asyncio.Event().wait()
    finally:
        await workers.stop()
        await pool.close()


def main() -> None:
    import argparse

    from hub.config import load_settings

    parser = argparse.ArgumentParser(description="Run hub delivery workers without the API.")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    settings = load_settings()
    with asyncio.Runner(
        loop_factory=asyncio.SelectorEventLoop
    ) as runner:  # psycopg async needs the selector loop
        try:
            runner.run(run_standalone(settings, args.workers))
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
