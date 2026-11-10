"""Chaos verdict: pure functions over what the mocks recorded and what the hub stored.

Counting rules that keep the test honest:
- Duplicates, losses, ordering and payload content come from the mocks' own record of accepted requests,
  never from the hub's tables.
- Expected outcomes come from the emitter's manifest (which events exist, which orders are deliberately
  invalid, which events were sent late), never from the hub's tables.
- Exactly once means exactly once. Any destination receiving a key twice fails the run. Doubles that match
  the one documented window (warehouse, after an ambiguous attempt) are classified and counted separately
  so the report can name them, but they are a failure, never a pass.
- Ordering is judged on every arrival at a mock, copies included, with no exemption for any window.
- The double check only has teeth where the destination does not deduplicate for us: the warehouse. The
  helpdesk and accounting mocks absorb repeats (Idempotency-Key, duplicate 409), so a repeat there is
  invisible at the mock. The report states how many repeats were absorbed.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import timedelta

from hub.mapping import delivery_key
from hub.retry import NO_RESPONSE_CLASSES

AMBIGUOUS_CLASSES = NO_RESPONSE_CLASSES

# What each mock records as the kind of request, per storefront event type.
EXPECTED_KIND = {
    "warehouse": {"order.created": "prepare", "order.paid": "release", "order.cancelled": "cancel"},
    "accounting": {"order.created": "invoice", "order.paid": "payment", "order.cancelled": "credit_note"},
    "helpdesk": {t: t for t in ("order.created", "order.paid", "order.cancelled")},
}


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class Verdict:
    checks: list[Check] = field(default_factory=list)
    numbers: dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(c.ok for c in self.checks)

    def add(self, name: str, ok: bool, detail: str = "") -> None:
        self.checks.append(Check(name, bool(ok), detail))


def expected_outcomes(
    manifest: list[dict], destinations: list[str]
) -> dict[tuple[str, str], tuple[str, str | None]]:
    """(event_id, destination) -> (status, dead_reason). Invalid orders die at their first event; the rest block.

    Late events (sent after the order's next event) may legitimately end `delivered` (the hub had not yet
    delivered the later one, so the ordering gate held it back) or `superseded` (it had); `evaluate` accepts both.
    """
    by_order: dict[str, list[dict]] = defaultdict(list)
    for entry in manifest:
        by_order[entry["order_id"]].append(entry)
    out: dict[tuple[str, str], tuple[str, str | None]] = {}
    for events in by_order.values():
        events.sort(key=lambda e: e["index_in_order"])
        kind = events[0]["invalid"]
        for dest in destinations:
            if kind == "malformed":
                reason = "mapping_error"
            elif kind and dest == "accounting":
                reason = "http_422"
            else:
                reason = None
            for i, ev in enumerate(events):
                if reason is None:
                    out[(ev["event_id"], dest)] = ("delivered", None)
                elif i == 0:
                    out[(ev["event_id"], dest)] = ("dead", reason)
                else:
                    out[(ev["event_id"], dest)] = ("blocked", None)
    return out


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def throttle_violations(attempts: list[dict], slack_before_s: float = 0.25, slack_after_s: float = 0.05):
    """429s whose Retry-After was not honoured: another call to the same destination started inside the window.

    The first `slack_before_s` after the response is ignored: a call may have been admitted by the gate in the
    few milliseconds before the 429 was written to the shared state (it was in flight when the 429 arrived).
    """
    by_dest: dict[str, list[dict]] = defaultdict(list)
    for a in attempts:
        if a["kind"] in ("send", "lookup"):
            by_dest[a["destination"]].append(a)
    throttled = honoured = 0
    violations = []
    for dest, calls in by_dest.items():
        for a in calls:
            if a["status_code"] != 429:
                continue
            throttled += 1
            end = a["started_at"] + timedelta(milliseconds=a["latency_ms"] or 0)
            window = float(a["retry_after_s"] or 1.0)
            lo, hi = end + timedelta(seconds=slack_before_s), end + timedelta(seconds=window - slack_after_s)
            bad = [c for c in calls if c is not a and lo < c["started_at"] < hi]
            if bad:
                violations.append((dest, a["started_at"], len(bad)))
            else:
                honoured += 1
    return throttled, honoured, violations


def ordering_violations(received: dict[str, list[dict]], key_to_pair: dict, by_event: dict) -> list[tuple]:
    """Per order at each mock, the event index must never go backwards across ALL arrivals, copies included.

    A copy of the order's first event landing after its second is a violation (prepare after release or cancel),
    whatever caused it.
    """
    bad = []
    for dest, entries in received.items():
        seqs: dict[str, list[tuple[int, int]]] = defaultdict(list)
        for e in sorted(entries, key=lambda x: x["n"]):
            pair = key_to_pair.get(e["reference"])
            if pair:
                ev = by_event[pair[0]]
                seqs[ev["order_id"]].append((e["n"], ev["index_in_order"]))
        for order_id, seq in seqs.items():
            indexes = [i for _, i in seq]
            if indexes != sorted(indexes):
                bad.append((dest, order_id, indexes))
    return bad


def evaluate(
    *,
    manifest: list[dict],
    destinations: list[str],
    received: dict[str, list[dict]],
    deliveries: list[dict],
    hub_event_ids: set[str],
    attempts: list[dict],
    emitter: dict,
    forged_expected: int,
    stale_expected: int,
    kills: int,
    log_text: str,
    secrets: list[str],
    pii_markers: list[str],
) -> Verdict:
    v = Verdict()
    manifest_ids = {m["event_id"] for m in manifest}
    expected = expected_outcomes(manifest, destinations)
    keys = {(m["event_id"], d): delivery_key(m["event_id"], d) for m in manifest for d in destinations}
    key_to_pair = {k: pair for pair, k in keys.items()}
    by_event = {m["event_id"]: m for m in manifest}
    late_ids = {m["event_id"] for m in manifest if m.get("late")}

    # --- intake -------------------------------------------------------------------------------------
    missing = manifest_ids - hub_event_ids
    extra = {e for e in hub_event_ids if e not in manifest_ids}
    v.add(
        "every emitted event stored by the hub",
        not missing,
        f"{len(manifest_ids) - len(missing)}/{len(manifest_ids)}",
    )
    v.add("no forged or stale webhook was stored", not extra, f"unexpected stored ids: {sorted(extra)[:5]}")
    v.add(
        "forged and stale webhooks all rejected with 401",
        emitter["forged_rejected"] == emitter["forged_sent"] == forged_expected
        and emitter["stale_rejected"] == emitter["stale_sent"] == stale_expected,
        f"forged {emitter['forged_rejected']}/{emitter['forged_sent']}, "
        f"stale {emitter['stale_rejected']}/{emitter['stale_sent']}",
    )
    v.add(
        "storefront gave up on no event",
        emitter["events_failed"] == 0 and emitter["events_sent"] == len(manifest),
    )
    v.add(
        "every duplicate webhook answered 200",
        emitter["duplicates_answered_200"] == emitter["duplicates_sent"],
        f"{emitter['duplicates_answered_200']}/{emitter['duplicates_sent']}",
    )

    # --- hub end state ------------------------------------------------------------------------------
    rows = {(r["event_id"], r["destination"]): r for r in deliveries}
    v.add(
        "one delivery row per (event, destination)",
        len(rows) == len(manifest) * len(destinations) == len(deliveries),
        f"{len(deliveries)} rows",
    )
    live = [r for r in deliveries if r["status"] in ("pending", "in_flight")]
    v.add("nothing left pending or in flight", not live, f"{len(live)} live rows")
    wrong = []
    for pair, (want_status, want_reason) in expected.items():
        row = rows.get(pair)
        got = (row["status"], row["dead_reason"]) if row else None
        if pair[0] in late_ids:
            ok = got is not None and got[0] in ("delivered", "superseded")
        else:
            ok = got == (want_status, want_reason)
        if not ok:
            wrong.append((pair, (want_status, want_reason), got))
    v.add(
        "hub status matches the manifest for every delivery",
        not wrong,
        f"{len(wrong)} mismatches: {wrong[:3]}",
    )
    dead = [r for r in deliveries if r["status"] == "dead"]
    v.add(
        "every dead letter carries a reason and the response body or error",
        all(r["dead_reason"] and r["dead_body"] for r in dead),
        f"{len(dead)} dead",
    )

    # --- what the mocks saw --------------------------------------------------------------------------
    counts = {d: Counter(e["reference"] for e in received[d]) for d in destinations}
    lost, doubled, unexpected = [], [], []
    for pair, (status, _) in expected.items():
        n = counts[pair[1]].get(keys[pair], 0)
        hub_status = rows[pair]["status"] if pair in rows else None
        should_arrive = status == "delivered" and not (pair[0] in late_ids and hub_status == "superseded")
        if should_arrive and n == 0:
            lost.append(pair)
        elif not should_arrive and n != 0:
            unexpected.append((pair, n))
    for dest in destinations:
        for ref, n in counts[dest].items():
            if n > 1 and ref in key_to_pair:
                doubled.append((key_to_pair[ref], n))
    extra_copies = sum(n - 1 for _, n in doubled)
    stray = [(d, ref) for d in destinations for ref in counts[d] if ref not in key_to_pair]
    expected_delivered = sum(1 for s, _ in expected.values() if s == "delivered")
    v.add(
        "zero lost: every valid delivery reached its destination",
        not lost,
        f"{len(lost)} lost of {expected_delivered}",
    )

    attempts_by_key: dict[str, list[dict]] = defaultdict(list)
    for a in attempts:
        attempts_by_key[a["delivery_key"]].append(a)
    in_window = []
    for pair, n in doubled:
        calls = attempts_by_key[keys[pair]]
        ambiguous = any(a["error_class"] in AMBIGUOUS_CLASSES or a["kind"] == "lease_expired" for a in calls)
        if pair[1] == "warehouse" and ambiguous:
            in_window.append((pair, n))
    v.add(
        "exactly once: zero double-delivered (counted at the mocks)",
        not doubled,
        f"{len(doubled)} keys received more than once, {extra_copies} extra copies, "
        f"{len(in_window)} of them in the documented warehouse window: {doubled[:3]}",
    )
    v.add("rejected or blocked deliveries never reached a destination", not unexpected, f"{unexpected[:3]}")
    v.add("nothing delivered that was not expected", not stray, f"{stray[:3]}")

    # --- payload content: what arrived is what the manifest says should have arrived ----------------
    mismatched = []
    for dest in destinations:
        for e in received[dest]:
            pair = key_to_pair.get(e["reference"])
            if not pair:
                continue
            ev = by_event[pair[0]]
            want_kind = EXPECTED_KIND[dest][ev["type"]]
            if e.get("order_number") != ev["order_number"] or e.get("kind") != want_kind:
                mismatched.append((dest, ev["event_id"], e.get("order_number"), e.get("kind")))
    v.add(
        "every request at a mock carries the right order number and action for its event",
        not mismatched,
        f"{len(mismatched)} mismatches: {mismatched[:3]}",
    )

    # --- per-order ordering at each mock, judged on every arrival ----------------------------------
    order_bad = ordering_violations(received, key_to_pair, by_event)
    v.add(
        "per-order event order preserved on every arrival at every mock",
        not order_bad,
        f"{len(order_bad)} violations: {order_bad[:3]}",
    )

    # --- throttling, recovery, logs -----------------------------------------------------------------
    throttled, honoured, violations = throttle_violations(attempts)
    v.add(
        "every 429 Retry-After honoured",
        not violations,
        f"{honoured}/{throttled}; violations {violations[:3]}",
    )
    recovered = sum(1 for a in attempts if a["kind"] == "lease_expired")
    v.add(
        "hard worker kills exercised recovery (rows found with an expired lease)",
        kills == 0 or recovered > 0,
        f"{kills} kills, {recovered} leases recovered",
    )
    leaked = [s for s in secrets if s and s in log_text]
    v.add("no secret in any hub log", not leaked, f"{len(leaked)} secrets found")
    pii = [m for m in pii_markers if m in log_text]
    v.add("no customer email or phone in any hub log", not pii, f"{pii[:3]}")

    # --- numbers for the report ---------------------------------------------------------------------
    send_calls = [a for a in attempts if a["kind"] == "send"]
    lookup_calls = [a for a in attempts if a["kind"] == "lookup"]
    delivered_rows = [r for r in deliveries if r["status"] == "delivered" and r.get("latency_s") is not None]
    latencies = [r["latency_s"] for r in delivered_rows]
    per_dest = {
        d: {
            "p50_s": percentile([r["latency_s"] for r in delivered_rows if r["destination"] == d], 0.5),
            "p95_s": percentile([r["latency_s"] for r in delivered_rows if r["destination"] == d], 0.95),
        }
        for d in destinations
    }
    v.numbers = {
        "events": len(manifest),
        "orders": len({m["order_id"] for m in manifest}),
        "expected_delivered": expected_delivered,
        "lost": len(lost),
        "doubled": len(doubled),
        "extra_copies": extra_copies,
        "doubled_in_window": len(in_window),
        "send_calls": len(send_calls),
        "lookup_calls": len(lookup_calls),
        "calls_by_destination": {
            d: sum(1 for a in attempts if a["destination"] == d and a["kind"] in ("send", "lookup"))
            for d in destinations
        },
        "http_429": throttled,
        "http_429_honoured": honoured,
        "http_5xx": sum(1 for a in attempts if (a["status_code"] or 0) >= 500),
        "transport_errors": sum(
            1 for a in attempts if a["status_code"] is None and a["kind"] in ("send", "lookup")
        ),
        "http_401": sum(1 for a in attempts if a["status_code"] == 401),
        "retries": sum(max(0, r["attempts"] - 1) for r in deliveries),
        "reconciled": sum(1 for r in deliveries if r["delivered_via"] == "reconcile"),
        "duplicate_acked": sum(1 for r in deliveries if r["delivered_via"] == "duplicate_ack"),
        "leases_recovered": recovered,
        "dead": len(dead),
        "dead_reasons": dict(Counter(r["dead_reason"] for r in dead)),
        "blocked": sum(1 for r in deliveries if r["status"] == "blocked"),
        "superseded": sum(1 for r in deliveries if r["status"] == "superseded"),
        "late_events": len(late_ids),
        "p50_s": percentile(latencies, 0.5),
        "p95_s": percentile(latencies, 0.95),
        "per_destination": per_dest,
        "order_violations": len(order_bad),
        "payload_mismatches": len(mismatched),
    }
    return v
