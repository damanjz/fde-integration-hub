# FDE Project 2: Integration hub

Spec v1, 2026-11-10. Part of the FDE Portfolio (vault: `Projects/In progress/FDE Portfolio`). Same fictional customer as project 1: Larkfield Coffee Co. (disclose as simulated).

## The customer problem
Larkfield's storefront emits order events. Three downstream systems must receive them: the warehouse (3PL) to ship, the helpdesk to keep customer records, and the accounting system for invoices. Today a script calls each API directly; when an API is slow, rate-limited or down, orders are lost or sent twice. Goal: a hub that delivers every event to every destination exactly once in effect, survives outages, and shows what failed and why.

## Scope (local only, no installs beyond the existing venv)
Python 3.13, FastAPI, httpx, psycopg 3, Postgres (Docker, image `pgvector/pgvector:pg17` already pulled, port 5434), pytest, ruff. Venv: `.venv` (reuse; install nothing).

### Mock providers (`mocks/`, one FastAPI app, routers per provider, port 9000)
| Provider | Auth | Behaviour to simulate |
|---|---|---|
| Storefront | Sends webhooks signed with HMAC-SHA256 (`X-Larkfield-Signature`, timestamp header, 5-minute replay window) | Emits `order.created`, `order.paid`, `order.cancelled`; can resend the same event (duplicate) |
| Warehouse (3PL) | OAuth2 client credentials; tokens expire after 60 s | Rate limit 5 req/s, returns 429 with `Retry-After`; idempotency via `Idempotency-Key` header is NOT supported (the hub must dedupe itself) |
| Helpdesk | API key | Random 500s and slow responses (configurable); honours `Idempotency-Key` for 24 h (audit 2026-11-10) |
| Accounting | OAuth2 client credentials | Validates payload schema; 422 on bad data (permanent failure, must not retry); 409 on a duplicate invoice number, which the hub treats as already delivered (audit 2026-11-10) |
Fault injection is configurable per provider via env or a `/admin/faults` endpoint: error rate, latency, outage on/off, and (audit 2026-11-10) "accept then time out": the mock commits the request, then answers after the hub's client timeout, so the hub sees a failure for a request that actually landed.
(audit 2026-11-10) Warehouse also exposes `GET /shipments?external_ref=...` so the hub can check whether an ambiguous send landed. Every mock records each accepted request (`GET /admin/received`); the chaos test counts duplicates and order from this record, never from the hub's own tables.

### The hub (`hub/`, port 8100)
- `POST /webhooks/storefront`: verify HMAC and timestamp, reject bad signatures (401), store the raw event once (unique event id), return 200 fast. (audit 2026-11-10) Constant-time signature compare; accept the current and the previous secret so a rotation drops nothing; a duplicate event id returns 200 (already stored), not an error, so the storefront stops resending.
- Outbox: one delivery row per (event, destination) in Postgres. Workers claim rows with `SELECT ... FOR UPDATE SKIP LOCKED`.
- (audit 2026-11-10) Ordering: deliveries for the same order and destination go out in event order. A row is claimable only when every earlier row for that order and destination is delivered; if an earlier one is dead-lettered, later ones move to `blocked` (never sent, visible in `GET /deliveries?status=blocked`). Otherwise `order.cancelled` can reach the warehouse before `order.created` and a cancelled order ships.
- Mapping layer: storefront payload to each destination's schema (pure functions, unit-tested).
- Delivery: per-destination client with token cache and refresh on 401, exponential backoff with jitter, honours `Retry-After`, client-side rate limiter (token bucket) per destination, timeout per call.
- Retry policy: retry 429, 5xx, timeouts, connection errors up to N attempts; 4xx other than 401/429 is permanent and goes straight to the dead-letter state with the response body.
- Exactly-once in effect: destinations without idempotency get a deterministic delivery key; the hub records success before acknowledging and checks before sending (document the remaining at-least-once window honestly).
  - (audit 2026-11-10) Per destination: helpdesk gets the delivery key as `Idempotency-Key`; accounting's 409 duplicate counts as delivered; warehouse has no idempotency, so after an ambiguous outcome (timeout, connection reset after send, or a worker that died holding the row) the hub calls the `external_ref` lookup before resending. The README states which crash window, if any, can still double-send.
- Dead letters: `GET /deliveries?status=dead`, `POST /deliveries/{id}/replay`, CLI `python -m hub.replay --destination warehouse --since ...`. (audit 2026-11-10) Replay of a row already delivered is refused (409). `/deliveries`, replay and `/admin/*` need an admin API key from env.
- Failure log: every attempt stored (status code, latency, error class). `GET /metrics`: delivered, retried, dead, p50/p95 end-to-end delivery latency per destination; (audit 2026-11-10) also pending count, blocked count and age of the oldest pending row per destination (the number an on-call alert would watch).
- Structured JSON logs. No secrets in logs. (audit 2026-11-10) No customer email or phone in logs (mask like project 1).

### Proof
- Unit tests: HMAC verify (good, bad, expired), mapping, backoff schedule, retry classification, token refresh, rate limiter.
- End-to-end chaos test (`python -m chaos.run --events 500`): mocks at 20% 5xx, rate limits on, one 60-second warehouse outage mid-run, 5% duplicate webhooks, a few deliberately invalid orders. (audit 2026-11-10) Also: 5% "accept then time out" on every destination, the worker process killed (hard kill, no cleanup) and restarted 3 times mid-run, and some orders that get `order.cancelled` right after `order.created`. Without these, "zero double-delivered" holds by construction and proves nothing. Assert: every valid event delivered to every destination exactly once (count at the mock), invalid ones dead-lettered with reason, zero lost; (audit 2026-11-10) per-order event order preserved at each mock; any double delivery fails the run unless it falls in the window the README documents, and then it is counted and reported, not hidden. Write `reports/chaos.md` with the numbers (events, attempts, retries, 429s honoured, dead letters, duplicates suppressed, p50/p95 delivery latency, total runtime).
- `docker-compose.yml` for Postgres; `README.md` with the chaos numbers on top, architecture, how to run; `docs/runbook.md` (replay, rotate secrets, add a destination); `docs/engagement.md` short write-up (audit 2026-11-10: it opens with discovery: who asked, the success metric agreed, assumptions made, what was left out).

## Done when
`ruff check .` clean, `pytest -q` green, chaos run passes with zero lost and zero double-delivered (audit 2026-11-10: with worker kills and accept-then-time-out on, counted at the mocks, per-order order preserved), README numbers match `reports/chaos.md`. No emoji anywhere. No git commit (owner approves commits).
