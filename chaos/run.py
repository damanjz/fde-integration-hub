"""`python -m chaos.run --events 500`: end-to-end chaos test against real processes.

Processes started (and always stopped): mock providers (9000), hub API (8100, no workers), worker
process A (killed hard three times), worker process B (steady, shares the queue and the rate limiter).

Faults: 20% 5xx on every destination, 5% accept-then-timeout, 2% lost responses, 5% of warehouse POSTs
processed slowly BEFORE they commit (longer than the hub's client timeout), helpdesk and warehouse latency,
warehouse 5 req/s limit (the hub is configured just above it on purpose so the 429 path is exercised), one
60 s warehouse outage mid-run, token revocations, 5% duplicate webhooks, forged and stale webhooks,
deliberately invalid orders, orders cancelled right after creation, orders whose created event arrives after
their paid event, three hard worker kills.

The hub runs with its production defaults (lease, attempts, backoff, timeouts, settle delay, breaker). The
only differences are listed in the report: more worker tasks and the warehouse rate.

`--probe` raises the slow-commit delay past timeout + settle delay. That deliberately breaks the documented
bound, so the run is EXPECTED to fail; it publishes what the residual at-least-once window produces.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
import psycopg
from psycopg.rows import dict_row

from chaos.verify import Verdict, evaluate
from hub.config import Settings
from hub.db import apply_schema, ensure_database, swap_database

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs" / "chaos"
REPORT = ROOT / "reports" / "chaos.md"
PROBE_REPORT = ROOT / "reports" / "chaos-window-probe.md"
MOCK_URL, HUB_URL = "http://127.0.0.1:9000", "http://127.0.0.1:8100"
DESTINATIONS = ["accounting", "helpdesk", "warehouse"]
PROD = Settings(_env_file=None)  # the production defaults the hub runs with in this test
WORKERS_A, WORKERS_B = 10, 6  # production default is 4 per process; see the settings table in the report
WAREHOUSE_RATE, WAREHOUSE_BURST = "5.5", "2"  # production default is 4 / 2; just above the 5 req/s quota

SECRETS = {
    "webhook_current": "chaos-webhook-current-7f3a",
    "webhook_previous": "chaos-webhook-previous-1b9c",
    "admin": "chaos-admin-key-55d0",
    "warehouse": "chaos-warehouse-secret-a41e",
    "accounting": "chaos-accounting-secret-9c72",
    "helpdesk": "chaos-helpdesk-key-e08d",
}
ADMIN = {"X-Admin-Key": SECRETS["admin"]}


def base_database_url() -> str:
    url = os.environ.get("HUB_CHAOS_DATABASE_URL") or Settings().database_url
    if not url:
        raise SystemExit("set HUB_DATABASE_URL (or HUB_CHAOS_DATABASE_URL) in .env: see .env.example")
    return url


def faults_env(slow_commit_s: float) -> str:
    timeout = PROD.warehouse.timeout_s
    common = {
        "error_rate": 0.20,
        "accept_then_timeout_rate": 0.05,
        "accept_timeout_delay_s": timeout + 1.0,  # answers after the hub gave up
        "lost_response_rate": 0.02,
    }
    return json.dumps(
        {
            "warehouse": {
                **common,
                "latency_ms": 20,
                "latency_jitter_ms": 150,
                "rate_limit_per_sec": 5,
                "slow_commit_rate": 0.05,
                "slow_commit_delay_s": slow_commit_s,
            },
            "helpdesk": {**common, "latency_ms": 50, "latency_jitter_ms": 600},
            "accounting": {**common, "latency_ms": 20, "latency_jitter_ms": 150},
        }
    )


class Proc:
    def __init__(self, name: str, args: list[str], env: dict[str, str]):
        self.name, self.args, self.env = name, args, env
        self.popen: subprocess.Popen | None = None
        self.starts = 0

    def start(self) -> None:
        LOGS.mkdir(parents=True, exist_ok=True)
        log = open(LOGS / f"{self.name}.log", "ab")  # noqa: SIM115 - handed to the child
        self.popen = subprocess.Popen(
            [sys.executable, *self.args], cwd=ROOT, env=self.env, stdout=log, stderr=subprocess.STDOUT
        )
        self.starts += 1

    def kill(self) -> None:
        """Hard kill: TerminateProcess on Windows, SIGKILL elsewhere. No cleanup, no graceful drain."""
        if self.popen and self.popen.poll() is None:
            self.popen.kill()
            self.popen.wait(timeout=10)

    stop = kill


def port_free(port: int) -> bool:
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


async def wait_http(url: str, limit_s: float = 30.0) -> None:
    deadline = time.time() + limit_s
    async with httpx.AsyncClient() as c:
        while time.time() < deadline:
            try:
                if (await c.get(url, timeout=2)).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.3)
    raise RuntimeError(f"{url} did not come up")


def query(db_url: str, sql: str, params=()) -> list[dict]:
    with psycopg.connect(db_url, row_factory=dict_row, autocommit=True, connect_timeout=5) as conn:
        return conn.execute(sql, params).fetchall()


def build_env(db_url: str, opts: argparse.Namespace) -> dict[str, str]:
    env = dict(os.environ)
    for key in [k for k in env if k.startswith(("HUB_", "MOCK_"))]:
        del env[key]
    env.update(
        {
            "PYTHONUNBUFFERED": "1",
            "MOCK_ADMIN_KEY": SECRETS["admin"],
            "MOCK_WEBHOOK_SECRET": SECRETS["webhook_current"],
            "MOCK_WAREHOUSE_CLIENT_SECRET": SECRETS["warehouse"],
            "MOCK_ACCOUNTING_CLIENT_SECRET": SECRETS["accounting"],
            "MOCK_HELPDESK_API_KEY": SECRETS["helpdesk"],
            "MOCK_FAULTS": faults_env(opts.slow_commit_s),
            "MOCK_SEED": str(opts.seed),
            "HUB_DATABASE_URL": db_url,
            "HUB_WEBHOOK_SECRETS": f"{SECRETS['webhook_current']},{SECRETS['webhook_previous']}",
            "HUB_ADMIN_KEY": SECRETS["admin"],
            "HUB_WORKERS": "0",
            # Credentials are secrets, not tuning. Lease, attempts, backoff, timeouts, settle delay and breaker
            # are deliberately NOT set here: the hub runs with its production defaults.
            "HUB_WAREHOUSE__CLIENT_SECRET": SECRETS["warehouse"],
            "HUB_WAREHOUSE__RATE": WAREHOUSE_RATE,
            "HUB_WAREHOUSE__BURST": WAREHOUSE_BURST,
            "HUB_HELPDESK__API_KEY": SECRETS["helpdesk"],
            "HUB_ACCOUNTING__CLIENT_SECRET": SECRETS["accounting"],
        }
    )
    return env


async def run(opts: argparse.Namespace) -> tuple[Verdict, dict]:
    for port in (9000, 8100):
        if not port_free(port):
            raise SystemExit(f"port {port} is in use; stop whatever is listening before the chaos run")
    base_url = base_database_url()
    try:
        db_url = ensure_database(base_url, "hub_chaos", recreate=True)
        apply_schema(db_url)
    except psycopg.OperationalError as exc:
        raise SystemExit(
            f"Postgres not reachable on {swap_database(base_url, 'postgres')}: {exc}\n"
            "start it with: docker compose up -d db"
        ) from exc
    for f in LOGS.glob("*.log"):
        f.unlink()

    env = build_env(db_url, opts)
    mock = Proc("mocks", ["-m", "mocks"], env)
    hub = Proc("hub", ["-m", "hub"], env)
    worker_a = Proc("worker-a", ["-m", "hub.worker", "--workers", str(WORKERS_A)], env)
    worker_b = Proc("worker-b", ["-m", "hub.worker", "--workers", str(WORKERS_B)], env)
    procs = [mock, hub, worker_a, worker_b]
    info: dict = {"kills": 0, "outage_s": opts.outage_seconds, "probe": opts.probe}
    try:
        mock.start()
        await wait_http(MOCK_URL + "/healthz")
        hub.start()
        await wait_http(HUB_URL + "/healthz")
        worker_a.start()
        worker_b.start()

        async with httpx.AsyncClient(headers=ADMIN, timeout=15) as api:
            invalid = {"negative_price": 1, "bad_currency": 1, "total_mismatch": 1, "malformed": 2}
            resp = await api.post(
                MOCK_URL + "/storefront/emit",
                json={
                    "hub_url": HUB_URL,
                    "events": opts.events,
                    "seed": opts.seed,
                    "events_per_sec": opts.events_per_sec,
                    "duplicate_rate": 0.05,
                    "invalid": invalid,
                    "late": 3,
                    "forged": 3,
                    "stale": 2,
                },
            )
            resp.raise_for_status()
            t0 = time.time()
            total = resp.json()["events"]

            async def sent() -> int:
                return (await api.get(MOCK_URL + "/storefront/status")).json()["events_sent"]

            async def when_sent(fraction: float) -> None:
                while await sent() < fraction * total:
                    await asyncio.sleep(0.5)

            async def outage() -> None:
                await when_sent(0.35)
                await api.put(MOCK_URL + "/admin/faults", json={"warehouse": {"outage": True}})
                info["outage_started_at_s"] = round(time.time() - t0, 1)
                await asyncio.sleep(opts.outage_seconds)
                await api.put(MOCK_URL + "/admin/faults", json={"warehouse": {"outage": False}})

            async def revoke() -> None:
                await when_sent(0.25)
                await api.post(MOCK_URL + "/admin/tokens/revoke", params={"provider": "warehouse"})
                await api.post(MOCK_URL + "/admin/tokens/revoke", params={"provider": "accounting"})

            async def kills() -> None:
                for fraction in (0.20, 0.50, 0.75)[: opts.kills]:
                    await when_sent(fraction)
                    for _ in range(40):  # kill while work is actually in flight
                        busy = query(
                            db_url, "SELECT count(*) AS n FROM deliveries WHERE status = 'in_flight'"
                        )[0]["n"]
                        if busy >= 3:
                            break
                        await asyncio.sleep(0.25)
                    worker_a.kill()
                    info["kills"] += 1
                    await asyncio.sleep(2.0)
                    worker_a.start()

            async def drained() -> None:
                deadline = time.time() + opts.max_minutes * 60
                while time.time() < deadline:
                    status = (await api.get(MOCK_URL + "/storefront/status")).json()
                    rows = query(db_url, "SELECT status, count(*) AS n FROM deliveries GROUP BY status")
                    counts = {r["status"]: r["n"] for r in rows}
                    live = counts.get("pending", 0) + counts.get("in_flight", 0)
                    if (
                        not status["running"]
                        and live == 0
                        and sum(counts.values()) == total * len(DESTINATIONS)
                    ):
                        return
                    await asyncio.sleep(1.0)
                info["timed_out"] = True

            await asyncio.gather(outage(), revoke(), kills(), drained())
            info["runtime_s"] = round(time.time() - t0, 1)
            # Let every slow request finish committing, so a late landing is counted, not missed.
            await asyncio.sleep(opts.slow_commit_s + 4.0)

            received = (await api.get(MOCK_URL + "/admin/received")).json()
            info["mock_stats"] = (await api.get(MOCK_URL + "/admin/stats")).json()
            emitter = (await api.get(MOCK_URL + "/storefront/status")).json()
            manifest = (await api.get(MOCK_URL + "/storefront/manifest")).json()
            info["hub_metrics"] = (await api.get(HUB_URL + "/metrics")).json()

        deliveries = query(
            db_url,
            "SELECT d.event_id, d.destination, d.delivery_key, d.status, d.dead_reason, d.dead_body, "
            "d.delivered_via, d.attempts, extract(epoch from d.delivered_at - e.received_at)::float8 AS latency_s "
            "FROM deliveries d JOIN events e USING (event_id)",
        )
        attempts = query(
            db_url,
            "SELECT d.destination, d.delivery_key, a.kind, a.started_at, a.latency_ms, a.status_code, "
            "a.error_class, a.retry_after_s FROM delivery_attempts a JOIN deliveries d ON d.id = a.delivery_id",
        )
        hub_ids = {r["event_id"] for r in query(db_url, "SELECT event_id FROM events")}
        info["duplicates_suppressed"] = query(
            db_url, "SELECT coalesce(sum(duplicate_count), 0)::int AS n FROM events"
        )[0]["n"]
        info["rejected"] = {r["name"]: r["value"] for r in query(db_url, "SELECT name, value FROM counters")}
        log_text = "\n".join(
            p.read_text(encoding="utf-8", errors="replace") for p in LOGS.glob("*.log") if p.stem != "mocks"
        )
    finally:
        for p in reversed(procs):
            p.stop()

    verdict = evaluate(
        manifest=manifest,
        destinations=DESTINATIONS,
        received={d: received[d]["entries"] for d in DESTINATIONS},
        deliveries=deliveries,
        hub_event_ids=hub_ids,
        attempts=attempts,
        emitter=emitter,
        forged_expected=3,
        stale_expected=2,
        kills=info["kills"],
        log_text=log_text,
        secrets=list(SECRETS.values()),
        pii_markers=["@example.test", "+91 98"],
    )
    verdict.add("run drained inside the time limit", not info.get("timed_out"), f"{info.get('runtime_s')} s")
    info["emitter"] = emitter
    info["config"] = {"events": opts.events, "seed": opts.seed, "events_per_sec": opts.events_per_sec}
    info["slow_commit_s"] = opts.slow_commit_s
    return verdict, info


def fmt(x, digits: int = 2) -> str:
    return "n/a" if x is None else f"{x:.{digits}f}"


def render(verdict: Verdict, info: dict) -> str:
    n, stats, metrics = verdict.numbers, info["mock_stats"], info["hub_metrics"]["destinations"]
    emitter = info["emitter"]

    def total(key: str) -> int:
        return sum(stats[p].get(key, 0) for p in DESTINATIONS)

    timeout, settle = PROD.warehouse.timeout_s, PROD.ambiguous_settle_s
    bound = timeout + settle
    absorbed = total("duplicate_409") + total("idempotent_replays")
    wh_calls = n["calls_by_destination"]["warehouse"]
    quota = 5.0
    emission_s = info["config"]["events"] / info["config"]["events_per_sec"]
    status = "PASS" if verdict.passed else "FAIL"
    title = "Chaos window probe (expected to fail)" if info["probe"] else "Chaos run report"
    per = n["per_destination"]
    lines = [
        f"# {title}",
        "",
        f"Result: **{status}**. Generated {datetime.now(UTC).strftime('%Y-%m-%d %H:%M')} UTC by "
        f"`python -m chaos.run --events {info['config']['events']}"
        f"{' --probe' if info['probe'] else ''}` (seed {info['config']['seed']}). "
        "Simulated Larkfield Coffee Co. data; all providers are local mocks.",
        "",
        "## Numbers",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Events emitted (distinct) | {n['events']} across {n['orders']} orders |",
        f"| Valid deliveries expected (events x 3 destinations, minus invalid and superseded) | "
        f"{n['expected_delivered'] - n['superseded']} |",
        f"| Lost (count 0 at the mock) | {n['lost']} |",
        f"| Double-delivered keys / extra copies (counted at the mocks) | {n['doubled']} / {n['extra_copies']} "
        f"({n['doubled_in_window']} in the documented warehouse window) |",
        f"| Ordering violations on any arrival at any mock (copies included) | {n['order_violations']} |",
        f"| Wrong order number or action at a mock | {n['payload_mismatches']} |",
        f"| Warehouse latency p50 / p95 (webhook received to delivered) | {fmt(per['warehouse']['p50_s'])} s / "
        f"{fmt(per['warehouse']['p95_s'])} s |",
        f"| Helpdesk latency p50 / p95 | {fmt(per['helpdesk']['p50_s'])} s / {fmt(per['helpdesk']['p95_s'])} s |",
        f"| Accounting latency p50 / p95 | {fmt(per['accounting']['p50_s'])} s / "
        f"{fmt(per['accounting']['p95_s'])} s |",
        f"| Attempts: HTTP calls to destinations | {n['send_calls'] + n['lookup_calls']} "
        f"({n['send_calls']} sends, {n['lookup_calls']} lookups) |",
        f"| Retries (delivery cycles beyond the first) | {n['retries']} |",
        f"| 429s received / Retry-After honoured | {n['http_429']} / {n['http_429_honoured']} |",
        f"| 5xx responses seen by the hub | {n['http_5xx']} |",
        f"| Timeouts and connection errors seen by the hub | {n['transport_errors']} |",
        f"| 401s answered by a token refresh | {n['http_401']} |",
        f"| Warehouse requests slow to commit (longer than the {timeout:.0f} s timeout) | "
        f"{stats['warehouse'].get('slow_commit', 0)} |",
        f"| Warehouse repeats avoided by lookup (it had landed already) | {n['reconciled']} |",
        f"| Accounting and helpdesk repeats absorbed by the destination's own idempotency | {absorbed} "
        f"({total('duplicate_409')} accounting 409s, {total('idempotent_replays')} helpdesk key replays) |",
        f"| Duplicate webhooks sent / suppressed | {emitter['duplicates_sent']} / {info['duplicates_suppressed']} |",
        f"| Forged and stale webhooks rejected (401) | {emitter['forged_rejected'] + emitter['stale_rejected']} of "
        f"{emitter['forged_sent'] + emitter['stale_sent']} |",
        f"| Late events (created sent after paid): superseded / delivered in order | "
        f"{n['superseded']} / {n['late_events'] * len(DESTINATIONS) - n['superseded']} (of {n['late_events']} x 3) |",
        f"| Hard worker kills / leases recovered afterwards | {info['kills']} / {n['leases_recovered']} |",
        f"| Dead letters | {n['dead']} "
        f"({', '.join(f'{k}: {v}' for k, v in sorted(n['dead_reasons'].items())) or 'none'}) |",
        f"| Blocked behind a dead letter (never sent) | {n['blocked']} |",
        f"| End-to-end latency p50 / p95, all destinations | {fmt(n['p50_s'])} s / {fmt(n['p95_s'])} s |",
        f"| Total runtime | {info['runtime_s']} s |",
        "",
        "## Reading these numbers",
        "",
        "- **The double-delivery check only has teeth at the warehouse.** The helpdesk and accounting mocks "
        f"absorb repeats themselves ({absorbed} repeats were absorbed here), so a repeat sent there is invisible "
        "at the mock and zero doubles says little about them. The warehouse has no idempotency: every repeat "
        f"it receives is recorded, and {n['reconciled']} repeats were avoided only because the hub looked the "
        "shipment up first.",
        f"- **Capacity.** The storefront emits {info['config']['events_per_sec']:.0f} events/s and each event needs "
        f"at least one warehouse call; the warehouse quota is {quota:.0f} req/s. The run made {wh_calls} warehouse "
        f"calls (sends, lookups, retries), which needs at least {wh_calls / quota:.0f} s of quota against "
        f"{emission_s:.0f} s of emission, plus the {info['outage_s']} s outage. The warehouse queue therefore builds "
        "by design, and its latency tail is capacity, not a defect.",
        f"- **The remaining window.** A repeat can still reach the warehouse if it takes longer than the hub's "
        f"timeout plus settle delay ({timeout:.0f} s + {settle:.0f} s = {bound:.0f} s) to commit one request. "
        f"This run's slow commits take {info['slow_commit_s']:.0f} s, "
        + (
            "which is past that bound on purpose, so failures here are the window itself."
            if info["slow_commit_s"] > bound
            else "inside that bound, so a pass shows the hub covers it. The probe run in "
            "`reports/chaos-window-probe.md` breaks the bound on purpose and shows what happens."
        ),
        "- **Never send a stale event.** A row whose order has a later event already delivered at that "
        "destination is marked `superseded` and not sent. The remaining limit: a request that is still running "
        "inside the destination when the hub gives up cannot be recalled.",
        "",
        "## Settings (the hub ran with its production defaults except where listed)",
        "",
        "| Setting | Production default | This run | Why it differs |",
        "|---|---|---|---|",
        f"| Client timeout per call | {PROD.warehouse.timeout_s:.0f} s | {PROD.warehouse.timeout_s:.0f} s | same |",
        f"| Lease | {PROD.lease_s:.0f} s | {PROD.lease_s:.0f} s | same |",
        f"| Max counted attempts | {PROD.max_attempts} | {PROD.max_attempts} | same |",
        f"| Backoff base / cap | {PROD.backoff_base_s:.0f} s / {PROD.backoff_cap_s:.0f} s | same | same |",
        f"| Settle delay after an unanswered request | {PROD.ambiguous_settle_s:.0f} s | same | same |",
        f"| Breaker threshold / cooldown cap | {PROD.breaker_threshold} / {PROD.breaker_cap_s:.0f} s | same | same |",
        f"| Worker tasks | {PROD.workers} per process | {WORKERS_A} and {WORKERS_B} (two processes) | "
        f"{timeout:.0f} s timeouts park a task for {timeout:.0f} s; more tasks keep the quota busy |",
        f"| Warehouse rate / burst | {PROD.warehouse.rate} / {PROD.warehouse.burst} req/s | "
        f"{WAREHOUSE_RATE} / {WAREHOUSE_BURST} | set just above the 5 req/s quota to exercise the 429 path |",
        "",
        "## Per destination (from `GET /metrics`)",
        "",
        "| Destination | Delivered | Dead | Blocked | Superseded | Pending | Retried | 429 | 5xx | Lookups | p50 | p95 |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for d in DESTINATIONS:
        m = metrics[d]
        lines.append(
            f"| {d} | {m['delivered']} | {m['dead']} | {m['blocked']} | {m['superseded']} | {m['pending']} | "
            f"{m['retried']} | {m['http_429']} | {m['http_5xx']} | {m['lookup_calls']} | {fmt(m['p50_s'])} s | "
            f"{fmt(m['p95_s'])} s |"
        )
    lines += [
        "",
        "## Faults injected (counted at the mocks)",
        "",
        "| Fault | Count |",
        "|---|---|",
        f"| Random 5xx before processing (target 20% per destination) | {total('injected_5xx')} |",
        f"| Accepted, then answered after the hub's {timeout:.0f} s timeout | {total('accept_then_timeout')} |",
        f"| Accepted, then answered 502 | {total('lost_responses')} |",
        f"| Warehouse: processed slowly before committing ({info['slow_commit_s']:.0f} s) | "
        f"{stats['warehouse'].get('slow_commit', 0)} |",
        f"| Rate-limit 429s issued by the warehouse | {total('rate_limited_429')} |",
        f"| Requests refused during the {info['outage_s']} s warehouse outage (started at "
        f"{info.get('outage_started_at_s', 'n/a')} s) | {total('outage_503')} |",
        f"| OAuth tokens issued (60 s warehouse lifetime, plus forced revocations) | {total('tokens_issued')} |",
        f"| Requests refused with an invalid or expired token | {total('unauthorized_401')} |",
        f"| Accounting 409s (duplicate invoice number) | {total('duplicate_409')} |",
        "",
        "## Checks",
        "",
        "| Check | Result | Detail |",
        "|---|---|---|",
    ]
    for c in verdict.checks:
        detail = str(c.detail).replace("|", "/").replace("\n", " ")[:200]
        lines.append(f"| {c.name} | {'pass' if c.ok else 'FAIL'} | {detail} |")
    lines += [
        "",
        "How this is counted: duplicates, losses, ordering and payload content are read from each mock's own "
        "record of accepted requests (`GET /admin/received`), never from the hub's tables. Expected outcomes come "
        "from the storefront emitter's manifest. Any key received twice fails the run; ordering is judged on every "
        "arrival with no exemption.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end chaos test for the integration hub")
    parser.add_argument("--events", type=int, default=500)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--events-per-sec", type=float, default=5.0)
    parser.add_argument("--outage-seconds", type=int, default=60)
    parser.add_argument("--kills", type=int, default=3)
    parser.add_argument(
        "--slow-commit-s", type=float, default=12.0, help="warehouse pre-commit delay for 5% of POSTs"
    )
    parser.add_argument(
        "--probe", action="store_true", help="exceed timeout + settle on purpose (expected to fail)"
    )
    parser.add_argument("--max-minutes", type=float, default=14.0)
    parser.add_argument("--report", default=None)
    opts = parser.parse_args()
    if opts.probe:
        opts.slow_commit_s = max(
            opts.slow_commit_s, PROD.warehouse.timeout_s + PROD.ambiguous_settle_s + 12.0
        )
    if opts.slow_commit_s <= PROD.warehouse.timeout_s:
        raise SystemExit("--slow-commit-s must exceed the hub's client timeout, or the fault never bites")
    report = Path(opts.report) if opts.report else (PROBE_REPORT if opts.probe else REPORT)
    verdict, info = asyncio.run(run(opts))
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(render(verdict, info), encoding="utf-8")
    for c in verdict.checks:
        print(("PASS " if c.ok else "FAIL ") + c.name + (f"  [{c.detail}]" if not c.ok else ""))
    print(f"\n{'PASSED' if verdict.passed else 'FAILED'}: report written to {report}")
    raise SystemExit(0 if verdict.passed else 1)


if __name__ == "__main__":
    main()
