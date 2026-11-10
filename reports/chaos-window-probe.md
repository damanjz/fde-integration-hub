# Chaos window probe (expected to fail)

Result: **FAIL**. Generated 2026-11-09 23:13 UTC by `python -m chaos.run --events 500 --probe` (seed 1). Simulated Larkfield Coffee Co. data; all providers are local mocks.

## Numbers

| Metric | Value |
|---|---|
| Events emitted (distinct) | 500 across 257 orders |
| Valid deliveries expected (events x 3 destinations, minus invalid and superseded) | 1476 |
| Lost (count 0 at the mock) | 0 |
| Double-delivered keys / extra copies (counted at the mocks) | 5 / 5 (5 in the documented warehouse window) |
| Ordering violations on any arrival at any mock (copies included) | 1 |
| Wrong order number or action at a mock | 0 |
| Warehouse latency p50 / p95 (webhook received to delivered) | 126.06 s / 269.26 s |
| Helpdesk latency p50 / p95 | 0.64 s / 11.72 s |
| Accounting latency p50 / p95 | 0.24 s / 11.32 s |
| Attempts: HTTP calls to destinations | 2179 (1935 sends, 244 lookups) |
| Retries (delivery cycles beyond the first) | 572 |
| 429s received / Retry-After honoured | 13 / 13 |
| 5xx responses seen by the hub | 460 |
| Timeouts and connection errors seen by the hub | 80 |
| 401s answered by a token refresh | 4 |
| Warehouse requests slow to commit (longer than the 10 s timeout) | 25 |
| Warehouse repeats avoided by lookup (it had landed already) | 55 |
| Accounting and helpdesk repeats absorbed by the destination's own idempotency | 62 (32 accounting 409s, 30 helpdesk key replays) |
| Duplicate webhooks sent / suppressed | 22 / 22 |
| Forged and stale webhooks rejected (401) | 5 of 5 |
| Late events (created sent after paid): superseded / delivered in order | 6 / 3 (of 3 x 3) |
| Hard worker kills / leases recovered afterwards | 3 / 19 |
| Dead letters | 9 (http_422: 3, mapping_error: 6) |
| Blocked behind a dead letter (never sent) | 9 |
| End-to-end latency p50 / p95, all destinations | 1.24 s / 225.74 s |
| Total runtime | 351.7 s |

## Reading these numbers

- **The double-delivery check only has teeth at the warehouse.** The helpdesk and accounting mocks absorb repeats themselves (62 repeats were absorbed here), so a repeat sent there is invisible at the mock and zero doubles says little about them. The warehouse has no idempotency: every repeat it receives is recorded, and 55 repeats were avoided only because the hub looked the shipment up first.
- **Capacity.** The storefront emits 5 events/s and each event needs at least one warehouse call; the warehouse quota is 5 req/s. The run made 894 warehouse calls (sends, lookups, retries), which needs at least 179 s of quota against 100 s of emission, plus the 60 s outage. The warehouse queue therefore builds by design, and its latency tail is capacity, not a defect.
- **The remaining window.** A repeat can still reach the warehouse if it takes longer than the hub's timeout plus settle delay (10 s + 3 s = 13 s) to commit one request. This run's slow commits take 25 s, which is past that bound on purpose, so failures here are the window itself.
- **Never send a stale event.** A row whose order has a later event already delivered at that destination is marked `superseded` and not sent. The remaining limit: a request that is still running inside the destination when the hub gives up cannot be recalled.

## Settings (the hub ran with its production defaults except where listed)

| Setting | Production default | This run | Why it differs |
|---|---|---|---|
| Client timeout per call | 10 s | 10 s | same |
| Lease | 60 s | 60 s | same |
| Max counted attempts | 10 | 10 | same |
| Backoff base / cap | 1 s / 30 s | same | same |
| Settle delay after an unanswered request | 3 s | same | same |
| Breaker threshold / cooldown cap | 5 / 30 s | same | same |
| Worker tasks | 4 per process | 10 and 6 (two processes) | 10 s timeouts park a task for 10 s; more tasks keep the quota busy |
| Warehouse rate / burst | 4.0 / 2.0 req/s | 5.5 / 2 | set just above the 5 req/s quota to exercise the 429 path |

## Per destination (from `GET /metrics`)

| Destination | Delivered | Dead | Blocked | Superseded | Pending | Retried | 429 | 5xx | Lookups | p50 | p95 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| accounting | 487 | 5 | 5 | 3 | 0 | 174 | 0 | 149 | 0 | 0.24 s | 11.32 s |
| helpdesk | 493 | 2 | 2 | 3 | 0 | 142 | 0 | 115 | 0 | 0.64 s | 11.72 s |
| warehouse | 496 | 2 | 2 | 0 | 0 | 256 | 13 | 196 | 244 | 126.06 s | 269.26 s |

## Faults injected (counted at the mocks)

| Fault | Count |
|---|---|
| Random 5xx before processing (target 20% per destination) | 423 |
| Accepted, then answered after the hub's 10 s timeout | 69 |
| Accepted, then answered 502 | 31 |
| Warehouse: processed slowly before committing (25 s) | 25 |
| Rate-limit 429s issued by the warehouse | 13 |
| Requests refused during the 60 s warehouse outage (started at 37.0 s) | 9 |
| OAuth tokens issued (60 s warehouse lifetime, plus forced revocations) | 22 |
| Requests refused with an invalid or expired token | 4 |
| Accounting 409s (duplicate invoice number) | 32 |

## Checks

| Check | Result | Detail |
|---|---|---|
| every emitted event stored by the hub | pass | 500/500 |
| no forged or stale webhook was stored | pass | unexpected stored ids: [] |
| forged and stale webhooks all rejected with 401 | pass | forged 3/3, stale 2/2 |
| storefront gave up on no event | pass |  |
| every duplicate webhook answered 200 | pass | 22/22 |
| one delivery row per (event, destination) | pass | 1500 rows |
| nothing left pending or in flight | pass | 0 live rows |
| hub status matches the manifest for every delivery | pass | 0 mismatches: [] |
| every dead letter carries a reason and the response body or error | pass | 9 dead |
| zero lost: every valid delivery reached its destination | pass | 0 lost of 1482 |
| exactly once: zero double-delivered (counted at the mocks) | FAIL | 5 keys received more than once, 5 extra copies, 5 of them in the documented warehouse window: [(('evt_1_00003', 'warehouse'), 2), (('evt_1_00001', 'warehouse'), 2), (('evt_1_00008', 'warehouse'), 2)] |
| rejected or blocked deliveries never reached a destination | pass | [] |
| nothing delivered that was not expected | pass | [] |
| every request at a mock carries the right order number and action for its event | pass | 0 mismatches: [] |
| per-order event order preserved on every arrival at every mock | FAIL | 1 violations: [('warehouse', 'ord_1_0005', [0, 1, 0])] |
| every 429 Retry-After honoured | pass | 13/13; violations [] |
| hard worker kills exercised recovery (rows found with an expired lease) | pass | 3 kills, 19 leases recovered |
| no secret in any hub log | pass | 0 secrets found |
| no customer email or phone in any hub log | pass | [] |
| run drained inside the time limit | pass | 351.7 s |

How this is counted: duplicates, losses, ordering and payload content are read from each mock's own record of accepted requests (`GET /admin/received`), never from the hub's tables. Expected outcomes come from the storefront emitter's manifest. Any key received twice fails the run; ordering is judged on every arrival with no exemption.
