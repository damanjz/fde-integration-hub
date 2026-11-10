# Engagement write-up: Larkfield integration hub

Simulated customer: Larkfield Coffee Co. is fictional, and so are its storefront, warehouse, helpdesk and accounting systems. Everything here runs locally against mock providers. Numbers are in `reports/chaos.md`.

## Discovery

**Who asked.** Larkfield's operations lead. A script calls three APIs directly after each order event. When one API is slow, rate-limited or down, orders are lost or sent twice. Someone reconciles by hand every Monday.

**The success metric agreed.**
- Every order event reaches every destination exactly once in effect. Measured at the destination, not in the hub's own tables.
- An outage of one destination delays only that destination.
- Anyone on call can see what failed and why, and can replay, correct or skip it, without reading code.
- Alert number: age of the oldest undelivered row per destination.

**Assumptions made.**
- Event volume is a few hundred an hour, with bursts. One Postgres and a handful of workers is enough. No queue broker.
- The storefront sends events for one order sequentially and retries on non-2xx. Its clock is within 5 minutes of ours.
- Event time in the payload (`created_at`) orders events within an order.
- The accounting system treats the delivery key as the invoice number, answers a repeat with 409 and the error `duplicate_document_number` echoing that number. The helpdesk keeps `Idempotency-Key` for 24 hours. The warehouse has no idempotency but offers a search by `external_ref` that returns the matching records.
- The warehouse finishes processing one request within the client timeout plus the settle delay (13 s by default). This is the assumption the one remaining window rests on.
- A dead-lettered delivery is a human decision. The hub never retries a permanent failure by itself.
- Stored raw events contain customer contact details. Retention is the customer's policy call; the hub keeps them until told otherwise.

**Left out, on purpose.**
- Real provider APIs, a UI, multi-tenant support, a broker (SQS, Kafka), per-destination field-level schema versioning, a migration tool.
- Exactly-once across the customer's own systems. The hub can only promise effect at the three destinations.
- Hot reload of destination credentials (restart the process; tokens are re-fetched).
- Ordering across different orders (not needed).

## Remaining limits

Read these before promising anything to the customer.

| Limit | What can happen | What the hub does about it |
|---|---|---|
| Warehouse commits slower than timeout + settle delay | The hub times out, looks the shipment up before it exists, resends, and the original commits later: a repeat, and if a cancel went in between, `prepare` after `cancel`. | Narrows it (settle delay, generous timeout) but cannot close it: the warehouse offers no way to cancel or fence a request in flight. The chaos verdict counts such repeats as a failure. `reports/chaos-window-probe.md` shows what a run that breaks the bound produces. |
| An older event arrives after a newer one for the same order was delivered | Sending it would move the destination backwards. | It is never sent: marked `superseded`, audited, counted in `/metrics`. Trade-off: a late `order.created` after `order.paid` is dropped, so a destination that needed the invoice does not get it. Fix at the destination. |
| Storefront sends one order's events concurrently | They can commit out of order. | Per-order locks keep the outbox consistent, but event order is taken from the payload time and arrival order. |
| Helpdesk replay after 24 hours | The provider forgot the key; a repeat can create a second record. | Replay refuses such rows unless forced. |
| Raw events in the database | Customer contact details at rest. | Logs mask them; the database does not. Retention is the customer's call. |

## What was built

Storefront webhook, then a Postgres outbox (one row per event and destination), then workers that deliver through a per-destination client. Mock providers reproduce the failures. A chaos runner kills things and checks the result from the destinations' side.

## Failure modes and the decisions they forced

| Failure | What goes wrong without a fix | Decision |
|---|---|---|
| Storefront resends an event | Two outbox rows, two deliveries | Event id is the primary key. A duplicate returns 200 and bumps a counter. |
| Webhook replay or forgery | Attacker or stale resend creates orders | HMAC over `timestamp.body`, constant-time compare (on bytes, so a non-ASCII header is a 401, not a 500), 5-minute window both directions, current and previous secret accepted. |
| Clock skew on webhook timestamps | Valid events rejected, or a wide window invites replay | Window is symmetric and fixed at 300 s. Rejections say `timestamp_outside_window` and are counted, so skew shows up as a metric, not as silent loss. |
| Destination 5xx, timeout, reset | Lost event | Retry with exponential backoff and jitter up to N attempts, then dead letter. |
| Outage lasting minutes or hours | Every queued row burns its retry budget in the first minute, or one row dies on probe failures | One circuit breaker per destination in shared state: after 5 consecutive failures nobody sends for an exponentially growing cooldown, then one probe at a time. A failure while the breaker is open or half-open spends no attempt (it is counted in `uncounted`), so a lone row survives an outage of about four hours at the 30 s cooldown cap. |
| Rate limit bursts across workers | Five worker processes at 5 req/s each is 25 req/s | The token bucket lives in a Postgres row per destination, updated under a row lock with the database clock. A 429 blocks every worker until `Retry-After` and lowers the effective rate; successes restore it. Workers do not claim rows for a destination with no capacity. |
| Token expires mid-retry | 401 loops or lost event | Token cached with a margin of min(10 s, 20% of lifetime). A 401 forces one refresh and one resend inside the same attempt. Invalidation only drops the token that failed. |
| Destination accepted, answer lost (timeout, 502, 504) | Resend creates a duplicate | Per destination, see below. After a timeout, reset or gateway error the hub waits a settle delay before the lookup. A 503 with `Retry-After` and a 429 are treated as not processed. |
| Lookup says "found" when it should not | A search endpoint answering 200 with an empty list reads as found: the shipment is silently never sent | The lookup must return a record whose `external_ref` equals the delivery key. Empty or foreign results mean not found; an unreadable body means try again, never found. |
| A 409 that is not a duplicate | A closed accounting period reads as delivered: the invoice is silently never booked | A 409 counts as delivered only if the error is `duplicate_document_number` and the echoed number equals this delivery key. Any other 409 is a permanent failure. |
| Worker killed after the destination accepted, before the hub recorded it | Same duplicate, and no response to learn from | The row keeps a lease. When it expires the next worker knows the outcome is unknown and reconciles first. Results are written under a lease token, so a slow worker cannot overwrite the new owner. |
| Poison message | One bad event wedges the queue or crash-loops workers | A mapper failure dead-letters that delivery at once with the reason. Any other worker exception is a retryable internal error. Claims count as attempts, so a crash loop ends in a dead letter. |
| Earlier event fails permanently | `order.cancelled` reaches the warehouse before `order.created`, so a cancelled order ships | A row is claimable only when every earlier row for that order and destination is settled and no later one is in flight. Behind a dead row, later rows become `blocked`. The decisions are taken under a per-(order, destination) advisory lock in ingest, dead-lettering, delivery and skip, so an ingest racing a dead-lettering cannot strand a pending row behind a dead one. |
| Older event arrives late | The destination moves backwards | Never send a stale event: if a later event for the order and destination was already delivered, the row is `superseded` (audited, in metrics). |
| Operator needs to fix a blocked order | Advice that cannot be carried out | Replay with a corrected event (same id and type, audited), or skip the dead row with a reason (audited). Both release the blocked rows in order. |
| Operator replays something already delivered, or too late | Second delivery | Replay accepts dead rows only (409 otherwise). A replayed row reconciles first. Helpdesk rows older than 24 h are refused unless forced. |

### Exactly once in effect, per destination

| Destination | Mechanism | Where it can still fail |
|---|---|---|
| Helpdesk | Delivery key sent as `Idempotency-Key`. A resend inside 24 hours is answered with the original result. | A replay more than 24 hours after the first attempt (refused unless forced). |
| Accounting | Delivery key is the invoice number. 409 `duplicate_document_number` with our number means already booked and counts as delivered. | None known. |
| Warehouse | No idempotency. After an ambiguous outcome (timeout, reset, 5xx, expired lease, replay) the hub looks the shipment up by `external_ref` and resends only if no record carries our key. After a timeout or gateway error it waits a settle delay first. | The window in "Remaining limits". |

## Result

The chaos run injects 20% 5xx, 5% accept-then-timeout, 5% slow-commit at the warehouse (processing for longer than the hub's timeout before committing), rate limits, a 60 s warehouse outage, duplicate and forged webhooks, invalid orders, early cancellations, late `order.created` events and three hard worker kills, against the hub's production defaults. It passes only with zero lost, every key received once at the mocks, order preserved on every arrival (copies included), the right order number and action on every request, and no secret or customer contact detail in any hub log. The verdict code is itself tested with doctored evidence, including each hole an independent review found.

The report is explicit about what the double-delivery check can and cannot show: it only has teeth at the warehouse, because the helpdesk and accounting mocks absorb repeats themselves.

## What I would do next

- Postgres `LISTEN/NOTIFY` to wake workers instead of polling every 250 ms.
- A per-order sequence number from the storefront, so ordering does not depend on timestamps and arrival order.
- Alert rules on `oldest_pending_age_s`, `blocked` and `superseded` per destination; a small dashboard.
- Credential reload without restart; encrypted raw-event retention with an expiry job; a migration tool.
