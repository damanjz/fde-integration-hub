"""The outbox: every SQL statement that touches events and deliveries lives here.

State machine of a delivery row:

    pending --claim--> in_flight --delivered--> delivered
       ^                  |  \\--dead--------> dead --replay--> pending
       |                  |                      \\--skip----> skipped (audited)
       |                  +--superseded-------> superseded (audited; a later event was already delivered)
       +-- retry/release / lease expired (worker crash) -> claimable again, marked needs_reconcile
    blocked: pending behind a dead row of the same (order, destination); returns to pending when that row
    is delivered or skipped.

in_flight rows keep their lease expiry in next_attempt_at, so one ordered index serves both fresh and
abandoned rows. Results are written under a lease_token fence: a worker that lost its lease (it was
too slow, the row was re-claimed) cannot overwrite the new owner's outcome.

Ordering is per (order, destination). Everything that decides "is there an earlier row that is not settled"
or "is a later row waiting" takes a transaction-level advisory lock on that pair first (ingest, mark_dead,
mark_delivered, skip), so an ingest racing a dead-lettering can never leave a row pending behind a dead one.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from hub.client import CallRecord
from hub.logging_ import redact_text

EPOCH_NOW = "extract(epoch from clock_timestamp())"
SETTLED = (
    "('delivered', 'superseded', 'skipped')"  # an earlier row in one of these no longer holds others back
)


async def lock_order(conn, destination: str, order_id: str | None) -> None:
    """Serialise everything that reasons about the ordering of one (order, destination)."""
    if order_id is not None:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"{destination}|{order_id}",)
        )


@dataclass
class IngestResult:
    inserted: bool
    body_mismatch: bool


# A new delivery starts blocked if an earlier one for the same order and destination is dead or blocked.
_INSERT_DELIVERY = """
INSERT INTO deliveries (event_id, destination, order_id, occurred_at, event_seq, delivery_key, status)
SELECT %(eid)s, %(dest)s, %(oid)s::text, e.occurred_at, e.seq, %(key)s,
  CASE WHEN %(oid)s::text IS NOT NULL AND EXISTS (
         SELECT 1 FROM deliveries p
         WHERE p.destination = %(dest)s AND p.order_id = %(oid)s::text AND p.status IN ('dead', 'blocked')
           AND (p.occurred_at, p.event_seq) < (e.occurred_at, e.seq))
       THEN 'blocked' ELSE 'pending' END
FROM events e WHERE e.event_id = %(eid)s
ON CONFLICT DO NOTHING
"""


async def insert_delivery(conn, event_id: str, destination: str, order_id: str | None, key: str) -> None:
    await lock_order(conn, destination, order_id)
    await conn.execute(_INSERT_DELIVERY, {"eid": event_id, "dest": destination, "oid": order_id, "key": key})


async def ingest_event(
    pool: AsyncConnectionPool,
    *,
    event_id: str,
    event_type: str,
    order_id: str | None,
    occurred_at: datetime,
    raw_body: str,
    payload: dict,
    body_sha256: str,
    destinations: list[tuple[str, str]],
) -> IngestResult:
    """Store the raw event once and create its delivery rows in the same transaction.

    destinations: (name, delivery_key) pairs. A duplicate event id only bumps duplicate_count, so a
    storefront retry can never create a second set of deliveries.
    """
    async with pool.connection() as conn, conn.transaction():
        cur = await conn.execute(
            "INSERT INTO events (event_id, event_type, order_id, occurred_at, raw_body, payload, "
            "body_sha256) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (event_id) DO UPDATE SET duplicate_count = events.duplicate_count + 1 "
            "RETURNING (xmax = 0) AS inserted, body_sha256",
            (event_id, event_type, order_id, occurred_at, raw_body, Jsonb(payload), body_sha256),
        )
        row = await cur.fetchone()
        inserted = bool(row["inserted"])
        if inserted:
            for name, key in sorted(destinations):  # fixed lock order: no deadlock between two ingests
                await insert_delivery(conn, event_id, name, order_id, key)
        return IngestResult(inserted, (not inserted) and row["body_sha256"] != body_sha256)


async def bump_counter(pool: AsyncConnectionPool, name: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO counters (name, value) VALUES (%s, 1) "
            "ON CONFLICT (name) DO UPDATE SET value = counters.value + 1",
            (name,),
        )


@dataclass
class Claimed:
    id: int
    event_id: str
    destination: str
    order_id: str | None
    delivery_key: str
    attempts: int  # including this claim
    uncounted: int  # cycles that do not spend the retry budget (429, failures while the breaker was open)
    needs_reconcile: bool
    lease_token: uuid.UUID
    recovered: bool  # the row was in_flight with an expired lease: a worker died mid-attempt
    replay_count: int = 0
    extra: dict = field(default_factory=dict)


# Capacity-aware claim: skip destinations that are blocked or out of tokens right now, so a slow
# (rate-limited) destination cannot park every worker and starve the others. Ordering: a row is claimable
# only when every earlier row for the same (order, destination) is settled and no later one is in flight
# (an older event must never overtake a newer one that is being sent right now).
_CLAIM = f"""
WITH picked AS (
  SELECT d.id, d.status AS prev_status
  FROM deliveries d
  LEFT JOIN destination_state s ON s.destination = d.destination
  WHERE d.status IN ('pending', 'in_flight')
    AND d.next_attempt_at <= now()
    AND (s.destination IS NULL OR (
          s.blocked_until <= {EPOCH_NOW}
          AND s.tokens + ({EPOCH_NOW} - s.refilled_at) * s.rate * s.rate_scale >= 1))
    AND (d.order_id IS NULL OR NOT EXISTS (
          SELECT 1 FROM deliveries p
          WHERE p.destination = d.destination AND p.order_id = d.order_id AND p.status NOT IN {SETTLED}
            AND (p.occurred_at, p.event_seq) < (d.occurred_at, d.event_seq)))
    AND (d.order_id IS NULL OR NOT EXISTS (
          SELECT 1 FROM deliveries l
          WHERE l.destination = d.destination AND l.order_id = d.order_id AND l.status = 'in_flight'
            AND (l.occurred_at, l.event_seq) > (d.occurred_at, d.event_seq)))
  ORDER BY d.next_attempt_at
  LIMIT 1
  FOR UPDATE OF d SKIP LOCKED
)
UPDATE deliveries d SET
  status = 'in_flight',
  attempts = d.attempts + 1,
  lease_token = %(token)s,
  next_attempt_at = now() + make_interval(secs => %(lease)s),
  needs_reconcile = d.needs_reconcile OR p.prev_status = 'in_flight',
  updated_at = now()
FROM picked p
WHERE d.id = p.id
RETURNING d.id, d.event_id, d.destination, d.order_id, d.delivery_key, d.attempts, d.uncounted,
          d.needs_reconcile, d.replay_count, p.prev_status
"""


async def claim(pool: AsyncConnectionPool, lease_s: float) -> Claimed | None:
    token = uuid.uuid4()
    async with pool.connection() as conn, conn.transaction():
        cur = await conn.execute(_CLAIM, {"token": token, "lease": lease_s})
        row = await cur.fetchone()
        if row is None:
            return None
        recovered = row["prev_status"] == "in_flight"
        if recovered:
            await conn.execute(
                "INSERT INTO delivery_attempts (delivery_id, attempt_no, kind, started_at, outcome, detail) "
                "VALUES (%s, %s, 'lease_expired', now(), 'crashed', %s)",
                (row["id"], row["attempts"] - 1, "lease expired: previous attempt outcome unknown"),
            )
    return Claimed(
        id=row["id"],
        event_id=row["event_id"],
        destination=row["destination"],
        order_id=row["order_id"],
        delivery_key=row["delivery_key"],
        attempts=row["attempts"],
        uncounted=row["uncounted"],
        needs_reconcile=row["needs_reconcile"],
        lease_token=token,
        recovered=recovered,
        replay_count=row["replay_count"],
    )


async def load_event(pool: AsyncConnectionPool, claimed: Claimed) -> dict | None:
    """The event to map: an operator's corrected version if there is one, else the stored original."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT coalesce(d.event_override, e.payload) AS payload FROM deliveries d "
            "JOIN events e USING (event_id) WHERE d.id = %s",
            (claimed.id,),
        )
        row = await cur.fetchone()
        return row["payload"] if row else None


async def _log_calls(conn, claimed: Claimed, calls: list[CallRecord]) -> None:
    for c in calls:
        await conn.execute(
            "INSERT INTO delivery_attempts (delivery_id, attempt_no, kind, started_at, latency_ms, "
            "status_code, error_class, outcome, retry_after_s, detail) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                claimed.id,
                claimed.attempts,
                c.kind,
                c.started_at,
                c.latency_ms,
                c.status_code,
                c.error_class,
                str(c.outcome),
                c.retry_after_s,
                c.detail,
            ),
        )


async def log_internal(pool: AsyncConnectionPool, claimed: Claimed, error_class: str, detail: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO delivery_attempts (delivery_id, attempt_no, kind, started_at, error_class, "
            "outcome, detail) "
            "VALUES (%s, %s, 'internal', now(), %s, 'retry', %s)",
            (claimed.id, claimed.attempts, error_class, detail[:1000]),
        )


async def _fenced_update(conn, claimed: Claimed, set_sql: str, params: dict[str, Any]) -> bool:
    cur = await conn.execute(
        f"UPDATE deliveries SET {set_sql}, lease_token = NULL, updated_at = now() "
        "WHERE id = %(id)s AND lease_token = %(token)s AND status = 'in_flight'",
        {**params, "id": claimed.id, "token": claimed.lease_token},
    )
    return cur.rowcount == 1


_BLOCK_LATER = """
UPDATE deliveries b SET status = 'blocked', updated_at = now() FROM deliveries x
WHERE x.id = %s AND b.destination = x.destination AND b.order_id = x.order_id AND b.status = 'pending'
  AND (b.occurred_at, b.event_seq) > (x.occurred_at, x.event_seq)
"""

# After a dead row is delivered (replay) or skipped, later rows return to pending unless another earlier
# one is still dead.
_UNBLOCK = """
UPDATE deliveries b SET status = 'pending', next_attempt_at = now(), updated_at = now() FROM deliveries x
WHERE x.id = %s AND b.destination = x.destination AND b.order_id = x.order_id AND b.status = 'blocked'
  AND (b.occurred_at, b.event_seq) > (x.occurred_at, x.event_seq)
  AND NOT EXISTS (
    SELECT 1 FROM deliveries p
    WHERE p.destination = b.destination AND p.order_id = b.order_id AND p.status = 'dead'
      AND (p.occurred_at, p.event_seq) < (b.occurred_at, b.event_seq))
"""


async def mark_delivered(
    pool: AsyncConnectionPool, claimed: Claimed, calls: list[CallRecord], via: str
) -> bool:
    """Record success. Returns False if the lease was lost (the new owner will reconcile)."""
    async with pool.connection() as conn, conn.transaction():
        await lock_order(conn, claimed.destination, claimed.order_id)
        await _log_calls(conn, claimed, calls)
        ok = await _fenced_update(
            conn,
            claimed,
            "status = 'delivered', delivered_at = now(), delivered_via = %(via)s, needs_reconcile = false, "
            "last_error = NULL",
            {"via": via},
        )
        if ok:
            await conn.execute(_UNBLOCK, (claimed.id,))
        return ok


async def mark_retry(
    pool: AsyncConnectionPool,
    claimed: Claimed,
    calls: list[CallRecord],
    *,
    delay_s: float,
    error: str,
    status_code: int | None,
    needs_reconcile: bool,
    uncounted: bool,
) -> bool:
    async with pool.connection() as conn, conn.transaction():
        await _log_calls(conn, claimed, calls)
        return await _fenced_update(
            conn,
            claimed,
            "status = 'pending', next_attempt_at = now() + make_interval(secs => %(delay)s), "
            "needs_reconcile = %(rec)s, uncounted = uncounted + %(unc)s, last_error = %(err)s, "
            "last_status_code = %(code)s",
            {
                "delay": delay_s,
                "rec": needs_reconcile,
                "unc": 1 if uncounted else 0,
                "err": error[:1000],
                "code": status_code,
            },
        )


async def mark_dead(
    pool: AsyncConnectionPool,
    claimed: Claimed,
    calls: list[CallRecord],
    *,
    reason: str,
    body: str,
    status_code: int | None,
    needs_reconcile: bool = False,
) -> bool:
    async with pool.connection() as conn, conn.transaction():
        await lock_order(conn, claimed.destination, claimed.order_id)
        await _log_calls(conn, claimed, calls)
        ok = await _fenced_update(
            conn,
            claimed,
            "status = 'dead', dead_at = now(), dead_reason = %(reason)s, dead_body = %(body)s, "
            "last_status_code = %(code)s, last_error = %(reason)s, needs_reconcile = %(rec)s",
            {"reason": reason, "body": redact_text(body)[:4000], "code": status_code, "rec": needs_reconcile},
        )
        if ok:
            await conn.execute(_BLOCK_LATER, (claimed.id,))
        return ok


async def release(
    pool: AsyncConnectionPool, claimed: Claimed, calls: list[CallRecord], wait_s: float
) -> bool:
    """Give the row back without spending an attempt (the gate was closed for longer than we hold claims)."""
    async with pool.connection() as conn, conn.transaction():
        await _log_calls(conn, claimed, calls)
        return await _fenced_update(
            conn,
            claimed,
            "status = 'pending', attempts = attempts - 1, "
            "next_attempt_at = now() + make_interval(secs => %(w)s)",
            {"w": wait_s},
        )


async def supersede_if_stale(pool: AsyncConnectionPool, claimed: Claimed) -> str | None:
    """Never send a stale event.

    If a later event for the same order and destination has already been delivered, this row is marked
    superseded instead of sent (audited, counted in /metrics). Returns the id of the later delivery's event,
    or None if the row is current.
    """
    if claimed.order_id is None:
        return None
    async with pool.connection() as conn, conn.transaction():
        await lock_order(conn, claimed.destination, claimed.order_id)
        cur = await conn.execute(
            "SELECT l.id, l.event_id FROM deliveries x JOIN deliveries l "
            "ON l.destination = x.destination AND l.order_id = x.order_id "
            "WHERE x.id = %s AND l.status = 'delivered' "
            "AND (l.occurred_at, l.event_seq) > (x.occurred_at, x.event_seq) "
            "ORDER BY l.occurred_at, l.event_seq LIMIT 1",
            (claimed.id,),
        )
        later = await cur.fetchone()
        if later is None:
            return None
        ok = await _fenced_update(
            conn,
            claimed,
            "status = 'superseded', needs_reconcile = false, last_error = 'superseded'",
            {},
        )
        if not ok:
            return None
        detail = (
            f"event {later['event_id']} (delivery {later['id']}) for the same order was already delivered"
        )
        await conn.execute(
            "INSERT INTO delivery_attempts (delivery_id, attempt_no, kind, started_at, outcome, detail) "
            "VALUES (%s, %s, 'superseded', now(), 'superseded', %s)",
            (claimed.id, claimed.attempts, detail),
        )
        await conn.execute(
            "INSERT INTO delivery_audit (delivery_id, action, actor, reason, detail) "
            "VALUES (%s, 'superseded', 'system', %s, %s)",
            (
                claimed.id,
                detail,
                Jsonb({"later_delivery_id": later["id"], "later_event_id": later["event_id"]}),
            ),
        )
        await conn.execute(_UNBLOCK, (claimed.id,))
        return later["event_id"]


@dataclass
class ReplayResult:
    replayed: list[int] = field(default_factory=list)
    refused: list[tuple[int, str]] = field(default_factory=list)  # (delivery id, why)


async def replay(
    pool: AsyncConnectionPool,
    *,
    ids: list[int] | None = None,
    destination: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int | None = None,
    actor: str = "admin",
    reason: str | None = None,
    corrected_event: dict | None = None,
    force: bool = False,
    idempotency_ttl_s: dict[str, float] | None = None,
) -> ReplayResult:
    """Move dead deliveries back to pending.

    Replayed rows are reconciled first where the destination allows it (warehouse lookup; accounting 409;
    helpdesk Idempotency-Key). The helpdesk only remembers a key for `idempotency_ttl_s`: a dead row whose
    first send is older than that is refused unless `force`, because the repeat could create a second record.
    `corrected_event` replaces the stored event for that delivery (one id only; same event id and type).
    """
    where = ["d.status = 'dead'"]
    params: list[Any] = []
    if ids is not None:
        where.append("d.id = ANY(%s)")
        params.append(ids)
    if destination:
        where.append("d.destination = %s")
        params.append(destination)
    if since:
        where.append("d.dead_at >= %s")
        params.append(since)
    if until:
        where.append("d.dead_at < %s")
        params.append(until)
    sql = (
        "SELECT d.id, d.destination, d.order_id, "
        "extract(epoch from now() - (SELECT min(a.started_at) FROM delivery_attempts a "
        "WHERE a.delivery_id = d.id AND a.kind = 'send'))::float8 AS since_first_send_s "
        f"FROM deliveries d WHERE {' AND '.join(where)} ORDER BY d.id"
    )
    if limit:
        sql += " LIMIT %s"
        params.append(limit)
    sql += " FOR UPDATE OF d"
    result = ReplayResult()
    ttl = idempotency_ttl_s or {}
    async with pool.connection() as conn, conn.transaction():
        cur = await conn.execute(sql, params)
        candidates = sorted(await cur.fetchall(), key=lambda r: (r["order_id"] or "", r["destination"]))
        if corrected_event is not None:
            if len(candidates) != 1:
                raise ValueError("a corrected event can only be applied to exactly one dead delivery")
            await _check_correction(conn, candidates[0]["id"], corrected_event)
        for row in candidates:
            limit_s = ttl.get(row["destination"])
            age = row["since_first_send_s"]
            if limit_s is not None and age is not None and age > limit_s and not force:
                result.refused.append(
                    (
                        row["id"],
                        f"first sent {age / 3600:.1f} h ago; {row['destination']} only remembers "
                        f"idempotency keys for {limit_s / 3600:.0f} h, so a repeat could create a second "
                        "record",
                    )
                )
                continue
            await lock_order(conn, row["destination"], row["order_id"])
            await conn.execute(
                "UPDATE deliveries SET status = 'pending', attempts = 0, uncounted = 0, "
                "next_attempt_at = now(), "
                "needs_reconcile = true, dead_reason = NULL, dead_body = NULL, dead_at = NULL, "
                "lease_token = NULL, replay_count = replay_count + 1, updated_at = now(), "
                "event_override = coalesce(%s, event_override) WHERE id = %s",
                (Jsonb(corrected_event) if corrected_event is not None else None, row["id"]),
            )
            await conn.execute(
                "INSERT INTO delivery_audit (delivery_id, action, actor, reason, detail) "
                "VALUES (%s, %s, %s, %s, %s)",
                (
                    row["id"],
                    "replay_corrected" if corrected_event is not None else "replay",
                    actor,
                    reason,
                    Jsonb({"forced": force, "corrected": corrected_event is not None}),
                ),
            )
            result.replayed.append(row["id"])
    result.replayed.sort()
    return result


async def _check_correction(conn, delivery_id: int, corrected: dict) -> None:
    cur = await conn.execute(
        "SELECT e.event_id, e.event_type FROM deliveries d JOIN events e USING (event_id) WHERE d.id = %s",
        (delivery_id,),
    )
    original = await cur.fetchone()
    if corrected.get("id") != original["event_id"] or corrected.get("type") != original["event_type"]:
        raise ValueError("the corrected event must keep the original event id and type")


async def skip(pool: AsyncConnectionPool, delivery_id: int, *, actor: str, reason: str) -> bool:
    """Deliberately abandon a dead delivery so the rows blocked behind it are released. Audited.

    The skipped event is never sent. Use it when the event is wrong and the later events should go.
    """
    async with pool.connection() as conn, conn.transaction():
        cur = await conn.execute(
            "SELECT destination, order_id FROM deliveries WHERE id = %s AND status = 'dead' FOR UPDATE",
            (delivery_id,),
        )
        row = await cur.fetchone()
        if row is None:
            return False
        await lock_order(conn, row["destination"], row["order_id"])
        await conn.execute(
            "UPDATE deliveries SET status = 'skipped', updated_at = now() WHERE id = %s AND status = 'dead'",
            (delivery_id,),
        )
        await conn.execute(
            "INSERT INTO delivery_audit (delivery_id, action, actor, reason) VALUES (%s, 'skip', %s, %s)",
            (delivery_id, actor, reason),
        )
        await conn.execute(_UNBLOCK, (delivery_id,))
        return True


_LIST_COLS = (
    "d.id, d.event_id, d.destination, d.delivery_key, d.status, d.attempts, d.uncounted, d.needs_reconcile, "
    "d.last_error, d.last_status_code, d.dead_reason, d.dead_body, d.delivered_via, d.replay_count, "
    "d.created_at, d.delivered_at, d.dead_at, e.event_type, e.order_id, "
    "d.event_override IS NOT NULL AS corrected"
)


async def list_deliveries(
    pool: AsyncConnectionPool,
    *,
    status: str | None,
    destination: str | None,
    since: datetime | None,
    limit: int,
    offset: int,
) -> list[dict]:
    where, params = ["true"], []
    if status:
        where.append("d.status = %s")
        params.append(status)
    if destination:
        where.append("d.destination = %s")
        params.append(destination)
    if since:
        where.append("d.created_at >= %s")
        params.append(since)
    params += [limit, offset]
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"SELECT {_LIST_COLS} FROM deliveries d JOIN events e USING (event_id) "
            f"WHERE {' AND '.join(where)} ORDER BY d.id LIMIT %s OFFSET %s",
            params,
        )
        return await cur.fetchall()


async def get_delivery(pool: AsyncConnectionPool, delivery_id: int) -> dict | None:
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"SELECT {_LIST_COLS} FROM deliveries d JOIN events e USING (event_id) WHERE d.id = %s",
            (delivery_id,),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        cur = await conn.execute(
            "SELECT attempt_no, kind, started_at, latency_ms, status_code, error_class, outcome, "
            "retry_after_s, detail FROM delivery_attempts WHERE delivery_id = %s ORDER BY id",
            (delivery_id,),
        )
        row["attempt_log"] = await cur.fetchall()
        cur = await conn.execute(
            "SELECT action, actor, reason, detail, at FROM delivery_audit WHERE delivery_id = %s ORDER BY id",
            (delivery_id,),
        )
        row["audit"] = await cur.fetchall()
        return row


_DELIVERY_METRICS = """
SELECT d.destination,
  count(*) FILTER (WHERE d.status = 'delivered') AS delivered,
  count(*) FILTER (WHERE d.status = 'dead') AS dead,
  count(*) FILTER (WHERE d.status = 'pending') AS pending,
  count(*) FILTER (WHERE d.status = 'in_flight') AS in_flight,
  count(*) FILTER (WHERE d.status = 'blocked') AS blocked,
  count(*) FILTER (WHERE d.status = 'superseded') AS superseded,
  count(*) FILTER (WHERE d.status = 'skipped') AS skipped,
  coalesce(extract(epoch from now()
    - min(d.created_at) FILTER (WHERE d.status IN ('pending', 'in_flight'))), 0)::float8
    AS oldest_pending_age_s,
  count(*) FILTER (WHERE d.status = 'delivered' AND d.delivered_via = 'reconcile') AS reconciled,
  count(*) FILTER (WHERE d.status = 'delivered' AND d.delivered_via = 'duplicate_ack') AS duplicate_acked,
  coalesce(sum(greatest(d.attempts - 1, 0)), 0)::bigint AS retried,
  (percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch from d.delivered_at - e.received_at))
     FILTER (WHERE d.status = 'delivered'))::float8 AS p50_s,
  (percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch from d.delivered_at - e.received_at))
     FILTER (WHERE d.status = 'delivered'))::float8 AS p95_s
FROM deliveries d JOIN events e USING (event_id)
GROUP BY d.destination
"""

_ATTEMPT_METRICS = """
SELECT d.destination,
  count(*) FILTER (WHERE a.kind = 'send') AS send_calls,
  count(*) FILTER (WHERE a.kind = 'lookup') AS lookup_calls,
  count(*) FILTER (WHERE a.status_code = 429) AS http_429,
  count(*) FILTER (WHERE a.status_code >= 500) AS http_5xx,
  count(*) FILTER (WHERE a.status_code = 401) AS http_401,
  count(*) FILTER (WHERE a.status_code IS NULL AND a.error_class IS NOT NULL AND a.kind IN ('send', 'lookup'))
    AS transport_errors,
  count(*) FILTER (WHERE a.kind = 'lease_expired') AS lease_expired
FROM delivery_attempts a JOIN deliveries d ON d.id = a.delivery_id
GROUP BY d.destination
"""

_ZERO = {
    "delivered": 0,
    "dead": 0,
    "pending": 0,
    "in_flight": 0,
    "blocked": 0,
    "superseded": 0,
    "skipped": 0,
    "oldest_pending_age_s": 0.0,
    "reconciled": 0,
    "duplicate_acked": 0,
    "retried": 0,
    "p50_s": None,
    "p95_s": None,
    "send_calls": 0,
    "lookup_calls": 0,
    "http_429": 0,
    "http_5xx": 0,
    "http_401": 0,
    "transport_errors": 0,
    "lease_expired": 0,
    "effective_rate": None,
}


async def metrics(pool: AsyncConnectionPool, destinations: list[str] | None = None) -> dict:
    async with pool.connection() as conn:
        dests: dict[str, dict] = {d: dict(_ZERO) for d in destinations or []}
        for query in (_DELIVERY_METRICS, _ATTEMPT_METRICS):
            for row in await (await conn.execute(query)).fetchall():
                dests.setdefault(row["destination"], dict(_ZERO)).update(
                    {k: v for k, v in row.items() if k != "destination"}
                )
        for row in await (
            await conn.execute(
                "SELECT destination, rate * rate_scale AS effective_rate FROM destination_state"
            )
        ).fetchall():
            if row["destination"] in dests:
                dests[row["destination"]]["effective_rate"] = row["effective_rate"]
        ev = await (
            await conn.execute(
                "SELECT count(*) AS received, coalesce(sum(duplicate_count), 0)::bigint "
                "AS duplicates_suppressed FROM events"
            )
        ).fetchone()
        counters = {
            r["name"]: r["value"]
            for r in await (await conn.execute("SELECT name, value FROM counters")).fetchall()
        }
    return {"events": {**ev, "webhooks_rejected": counters}, "destinations": dests}
