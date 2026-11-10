# Runbook

All commands run from the project root with the project venv. `$ADMIN` is the value of `HUB_ADMIN_KEY`. Configuration lives in `.env` (copy `.env.example`; it is gitignored).

## Start and stop

| Task | Command |
|---|---|
| Postgres (loopback port 5434, password `HUB_DB_PASSWORD`) | `docker compose up -d db` |
| Mock providers (port 9000; reads `MOCK_*` from `.env`) | `python -m mocks` |
| Hub API with embedded workers (port 8100) | `python -m hub` (needs `HUB_DATABASE_URL`, `HUB_WEBHOOK_SECRETS`) |
| Workers only (a second process on the same queue) | `python -m hub.worker --workers 4` |
| API without workers | `HUB_WORKERS=0 python -m hub` |

Do not use `uvicorn hub.main:app` on Windows: async psycopg needs the selector event loop, which `python -m hub` sets up.

Stopping a worker mid-attempt is safe. Rows it held are re-claimed after `HUB_LEASE_S` (60 s default) and reconciled before any resend.

The schema is applied when the database volume is first created. There is no migration tool: after a schema change, recreate the volume (`docker compose down -v`) or apply the change by hand.

## What to watch

`GET /metrics` with `X-Admin-Key`. Per destination:

| Field | Alert when |
|---|---|
| `oldest_pending_age_s` | Above your delivery objective (start with 300 s). This is the on-call number. |
| `dead` | Above 0. Each one needs a decision. |
| `blocked` | Above 0. Rows waiting behind a dead one. They clear when the dead one is replayed or skipped. |
| `superseded` | Rising unexpectedly. A late event was dropped because a newer one was already delivered. |
| `pending` | Growing steadily. Workers are down or the destination is slow. |
| `effective_rate` | Well below the configured `rate`: the destination keeps answering 429. |
| `http_429`, `http_5xx`, `transport_errors` | Rising together with `pending`: destination trouble, nothing to do on our side. |

During a destination outage the circuit breaker holds sends back after five consecutive failures and probes once per cooldown (2 s doubling to 30 s). Failures while the breaker is open or half-open do not spend a row's retry attempts, so an outage does not dead-letter rows by itself. The limit is `HUB_MAX_TOTAL_ATTEMPTS` (500 cycles; at the 30 s cooldown that is about four hours of outage). A row that runs past it is dead-lettered as `max_attempts_exceeded` and can be replayed.

## Dead letters, blocked rows and replay

A delivery ends `dead` when the destination rejects it permanently (`http_4xx`), when it cannot be mapped (`mapping_error`), or when it runs out of retries (`max_attempts_exceeded`). Every later event for the same order and destination then becomes `blocked` and is never sent, because sending a cancel before its create would ship a cancelled order.

1. List dead rows: `curl -H "X-Admin-Key: $ADMIN" "http://localhost:8100/deliveries?status=dead"`. Each has `dead_reason` and `dead_body` (the destination's response or the mapping error).
2. List what is stuck behind them: `.../deliveries?status=blocked&destination=accounting`. Blocked rows share the `order_id` of the dead row ahead of them.
3. Detail with every attempt and every operator action: `GET /deliveries/{id}`.
4. Decide, then do exactly one of:

| Decision | How | What happens |
|---|---|---|
| The destination or the cause is fixed | `POST /deliveries/{id}/replay` | Row goes back to pending, reconciles first, is sent. Blocked rows behind it follow in order once it is delivered. |
| The event itself was wrong | `POST /deliveries/{id}/replay` with `{"corrected_event": {...}, "reason": "..."}` | Same, but the mapper runs on your corrected storefront event for this one delivery. It must keep the original `id` and `type`. The original stays stored; the correction is audited. |
| The event should never go, but the later ones should | `POST /deliveries/{id}/skip` with `{"reason": "..."}` | Row becomes `skipped` (never sent, audited). Blocked rows behind it are released and sent in order. Skipping while another dead row is still ahead leaves the later rows blocked. |

Add `X-Actor: <your name>` to record who acted. Bulk: `python -m hub.replay --destination warehouse --since 2026-11-10T00:00:00Z --actor dana --reason "provider fixed"` (`--dry-run` first; `--until`, `--limit`). One skip: `python -m hub.replay --skip 42 --reason "invoice raised by hand" --actor dana`.

Rules:
- Only dead rows can be replayed or skipped. Delivered, pending, blocked and superseded rows return 409.
- **Replay reconciles before it sends, where the destination allows it.** Warehouse: a lookup by `external_ref`. Accounting: a repeat answers 409 duplicate and counts as delivered. Helpdesk: the delivery key is sent again as `Idempotency-Key`. This makes a replay safe except inside the warehouse window described in the README, and for the helpdesk after 24 hours.
- **Helpdesk replays older than 24 hours are refused**, because the provider has forgotten the key and a repeat can create a second record. The API answers 409 with the reason; the CLI prints `REFUSED`. Override with `{"force": true}` or `--force-past-idempotency-window` only after checking the helpdesk by hand.
- `max_attempts_exceeded` after a timeout may mean the destination did get it. Replay settles that.
- A `superseded` row is an older event that arrived after a newer one for the same order had been delivered. It is audited (`delivery_audit`, attempt log) and deliberately not replayable: sending it now would put the destination back in an older state. If the destination really needs it, fix it at the destination.

## Rotate secrets

**Webhook secret (storefront to hub), no dropped events:**
1. Set `HUB_WEBHOOK_SECRETS=new,old` and restart the hub. Both are accepted.
2. Switch the storefront to sign with `new`.
3. After the storefront's retry window has passed, set `HUB_WEBHOOK_SECRETS=new` and restart.
Watch `rejected_bad_signature` in `GET /metrics` (`events.webhooks_rejected`) between steps 2 and 3: it should stay flat.

**Admin key:** change `HUB_ADMIN_KEY` and restart the hub. Clients using the old key get 401.

**Database password:** `docker exec fde-hub-db-1 psql -U hub -d hub -c "ALTER USER hub PASSWORD '<new>'"`, update `HUB_DB_PASSWORD` and `HUB_DATABASE_URL` in `.env`, restart the hub and workers. The database listens on `127.0.0.1` only.

**Destination credentials (warehouse, accounting client secret; helpdesk API key):**
1. Add the new credential at the provider while the old one still works, if the provider allows two.
2. Set `HUB_WAREHOUSE__CLIENT_SECRET` (or `HUB_ACCOUNTING__CLIENT_SECRET`, `HUB_HELPDESK__API_KEY`) in the environment of the API and every worker process.
3. Restart them. Cached tokens are dropped; a rejected credential shows as `token_rejected` or `auth_rejected` attempts and is retried, not dead-lettered, until `HUB_MAX_ATTEMPTS`.
4. Revoke the old credential.

Secrets never appear in logs: the log formatter redacts sensitive keys and scrubs every configured secret value from text. Customer emails and phone numbers are masked the same way.

## Add a destination

Example: a fourth system called `crm`.

1. **Mapper.** Add `to_crm(event) -> dict` in `hub/mapping.py` and register it in `MAPPERS`. Pure function. Include `delivery_key(event_id, "crm")` in the payload as the reference. Raise `MappingError` only when the event cannot be translated at all; leave business validation to the destination.
2. **Config.** Add a settings class in `hub/config.py` (`OAuthDestination` or `ApiKeyDestination` subclass with defaults) and a field `crm` on `Settings`. Set `rate` below the provider's limit, `timeout_s` above its normal processing time.
3. **Registry.** Add `"crm": settings.crm` to `destination_settings()` in `hub/destinations.py`.
4. **Dedupe.** Pick the mechanism the provider offers:
   - Idempotency header: `idempotency_header=True` and `idempotency_ttl_s` (how long the provider remembers keys; replay refuses older rows).
   - Duplicate status: `duplicate_statuses=[409]` plus `duplicate_error` (the body's `error` value) and `duplicate_number_field` (the body field that echoes the delivery key). Any other 409 stays a permanent failure.
   - Lookup by reference: `lookup_path` and `lookup_param`. The response must contain a record whose `lookup_param` field equals the delivery key; an empty or foreign result means "not found", an unreadable one means "try again".
   Without any of these the hub can only offer at-least-once; say so to the customer.
5. **Existing events.** The destination applies to events received after the restart. For history, insert delivery rows for old events (same columns as `ingest_event` writes) and let the workers drain them.
6. **Tests.** Add mapping tests, then extend `tests/helpers.py` `FakeProviders` and the chaos runner's destination list.
7. Restart the API and workers.

## Failure cheat sheet

| Symptom | Likely cause | Action |
|---|---|---|
| Webhooks answered 401 `timestamp_outside_window` | Storefront clock off by more than 5 minutes, or very late resend | Fix the storefront clock. Counted in `rejected_timestamp_outside_window`. |
| Webhooks answered 401 `bad_signature` | Secret not rotated on both sides | See rotation above. |
| `pending` growing, `http_5xx` or `transport_errors` rising | Destination outage | Wait. The breaker protects the queue and the retry budget. Watch `oldest_pending_age_s`. |
| `pending` growing, no errors | No workers running | Start `python -m hub.worker`. |
| Rows stuck `in_flight` | Worker died | They are re-claimed after the lease; nothing to do. |
| `effective_rate` far below `rate` | Destination keeps answering 429 | Lower the destination's `rate` to match its real quota. |
| Dead with `http_422` | The destination rejected the data | Replay with a corrected event, or skip. |
| Dead with `http_409` | A conflict that is not a duplicate of our delivery (for example a closed accounting period) | Resolve at the destination, then replay. |
| Dead with `mapping_error` | Event lacks required fields | Replay with a corrected event, or skip. |
| `blocked` rows | A dead row ahead of them for the same order | See "Dead letters, blocked rows and replay". |
