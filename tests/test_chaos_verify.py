"""The chaos verdict must itself be trustworthy: feed it doctored evidence and check it objects.

The second half are the holes an independent review found (a verdict that passed with a cancelled order
shipping, with four copies of one shipment, with someone else's order number). Each is a test that fails if
the hole comes back.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from chaos.verify import EXPECTED_KIND, evaluate, expected_outcomes, percentile, throttle_violations
from hub.mapping import delivery_key

DESTS = ["accounting", "helpdesk", "warehouse"]
EXACTLY_ONCE = "exactly once: zero double-delivered (counted at the mocks)"
ORDERING = "per-order event order preserved on every arrival at every mock"
PAYLOAD = "every request at a mock carries the right order number and action for its event"
LOST = "zero lost: every valid delivery reached its destination"


def _entry(event_id, order, invalid, index, etype="order.created"):
    return {
        "event_id": event_id,
        "type": etype,
        "order_id": order,
        "order_number": f"LF-{order}",
        "invalid": invalid,
        "index_in_order": index,
    }


def manifest():
    return [
        _entry("e1", "o1", None, 0),
        _entry("e2", "o1", None, 1, "order.cancelled"),
        _entry("e3", "o2", "bad_currency", 0),
        _entry("e4", "o2", "bad_currency", 1, "order.paid"),
        _entry("e5", "o3", "malformed", 0),
    ]


def _attempt(dest, key, kind="send", error_class=None, status=None, latency=5):
    return {
        "destination": dest,
        "delivery_key": key,
        "kind": kind,
        "started_at": datetime.now(UTC),
        "latency_ms": latency,
        "status_code": status,
        "error_class": error_class,
        "retry_after_s": None,
    }


def good_evidence():
    m = manifest()
    by_event = {e["event_id"]: e for e in m}
    exp = expected_outcomes(m, DESTS)
    received = {d: [] for d in DESTS}
    n = 0
    for (eid, dest), (status, _) in exp.items():
        if status == "delivered":
            n += 1
            ev = by_event[eid]
            received[dest].append(
                {
                    "n": n,
                    "reference": delivery_key(eid, dest),
                    "order_number": ev["order_number"],
                    "kind": EXPECTED_KIND[dest][ev["type"]],
                    "at": 0,
                }
            )
    # arrival order must follow event order within an order
    for d in DESTS:
        received[d].sort(key=lambda e: (by_event_of(m, e["reference"], d)["index_in_order"], e["n"]))
        for i, e in enumerate(received[d]):
            e["n"] = i + 1
    deliveries = [
        {
            "event_id": eid,
            "destination": d,
            "delivery_key": delivery_key(eid, d),
            "status": st,
            "dead_reason": reason,
            "dead_body": "body" if st == "dead" else None,
            "delivered_via": "send" if st == "delivered" else None,
            "attempts": 1,
            "latency_s": 1.0,
        }
        for (eid, d), (st, reason) in exp.items()
    ]
    emitter = {
        "forged_sent": 1,
        "forged_rejected": 1,
        "stale_sent": 1,
        "stale_rejected": 1,
        "events_failed": 0,
        "events_sent": len(m),
        "duplicates_sent": 2,
        "duplicates_answered_200": 2,
    }
    return dict(
        manifest=m,
        destinations=DESTS,
        received=received,
        deliveries=deliveries,
        hub_event_ids={e["event_id"] for e in m},
        attempts=[_attempt("warehouse", "k", kind="lease_expired")],
        emitter=emitter,
        forged_expected=1,
        stale_expected=1,
        kills=1,
        log_text="clean",
        secrets=["s3cret"],
        pii_markers=["@example.test"],
    )


def by_event_of(m, ref, dest):
    return next(e for e in m if delivery_key(e["event_id"], dest) == ref)


def failed(verdict) -> set[str]:
    return {c.name for c in verdict.checks if not c.ok}


def first_wh(ev, event_id="e1"):
    k = delivery_key(event_id, "warehouse")
    return k, next(e for e in ev["received"]["warehouse"] if e["reference"] == k)


def test_expected_outcomes():
    exp = expected_outcomes(manifest(), DESTS)
    assert exp[("e1", "warehouse")] == ("delivered", None) and exp[("e2", "accounting")] == (
        "delivered",
        None,
    )
    assert exp[("e3", "accounting")] == ("dead", "http_422") and exp[("e4", "accounting")] == (
        "blocked",
        None,
    )
    assert exp[("e3", "warehouse")] == ("delivered", None) and exp[("e4", "helpdesk")] == ("delivered", None)
    assert all(exp[("e5", d)] == ("dead", "mapping_error") for d in DESTS)


def test_clean_evidence_passes():
    verdict = evaluate(**good_evidence())
    assert verdict.passed, failed(verdict)
    assert verdict.numbers["lost"] == 0 and verdict.numbers["doubled"] == 0


def test_a_lost_delivery_fails_the_run():
    ev = good_evidence()
    ev["received"]["warehouse"].pop(0)
    assert LOST in failed(evaluate(**ev))


def test_a_double_delivery_fails_the_run():
    ev = good_evidence()
    k, first = first_wh(ev)
    ev["received"]["helpdesk"].append(dict(ev["received"]["helpdesk"][0], n=99))
    assert EXACTLY_ONCE in failed(evaluate(**ev))


def test_a_warehouse_double_after_an_ambiguous_attempt_is_classified_but_still_fails():
    ev = good_evidence()
    k, first = first_wh(ev)
    ev["received"]["warehouse"].append(dict(first, n=99))
    ev["attempts"].append(_attempt("warehouse", k, error_class="timeout", latency=2000))
    verdict = evaluate(**ev)
    # A window double is a known failure: classified and named, never a pass.
    assert EXACTLY_ONCE in failed(verdict)
    assert verdict.numbers["doubled"] == 1 and verdict.numbers["doubled_in_window"] == 1


def test_a_warehouse_double_without_any_ambiguity_is_not_in_the_window():
    ev = good_evidence()
    ev["received"]["warehouse"].append(dict(first_wh(ev)[1], n=99))
    verdict = evaluate(**ev)
    assert EXACTLY_ONCE in failed(verdict) and verdict.numbers["doubled_in_window"] == 0


def test_order_violation_is_detected():
    ev = good_evidence()
    wh = ev["received"]["warehouse"]
    rank = {delivery_key("e1", "warehouse"): 0, delivery_key("e2", "warehouse"): 1}
    first, second = sorted((e for e in wh if e["reference"] in rank), key=lambda e: rank[e["reference"]])
    first["n"], second["n"] = second["n"], first["n"]  # cancel arrives before create
    assert ORDERING in failed(evaluate(**ev))


def test_invalid_order_reaching_the_destination_fails():
    ev = good_evidence()
    ev["received"]["accounting"].append(
        {
            "n": 98,
            "reference": delivery_key("e3", "accounting"),
            "order_number": "LF-o2",
            "kind": "invoice",
            "at": 0,
        }
    )
    assert "rejected or blocked deliveries never reached a destination" in failed(evaluate(**ev))


def test_stray_reference_fails():
    ev = good_evidence()
    ev["received"]["helpdesk"].append({"n": 97, "reference": "unknown", "order_number": "?", "at": 0})
    assert "nothing delivered that was not expected" in failed(evaluate(**ev))


def test_status_mismatch_and_live_rows_fail():
    ev = good_evidence()
    ev["deliveries"][0]["status"] = "pending"
    names = failed(evaluate(**ev))
    assert "nothing left pending or in flight" in names
    assert "hub status matches the manifest for every delivery" in names


def test_stored_forged_event_fails():
    ev = good_evidence()
    ev["hub_event_ids"].add("evt_forged_1")
    assert "no forged or stale webhook was stored" in failed(evaluate(**ev))


def test_kills_that_recovered_nothing_fail():
    ev = good_evidence()
    ev["attempts"] = []
    assert "hard worker kills exercised recovery (rows found with an expired lease)" in failed(evaluate(**ev))


def test_leaked_secret_or_pii_in_logs_fails():
    ev = good_evidence()
    ev["log_text"] = "boom s3cret and bob@example.test"
    names = failed(evaluate(**ev))
    assert {"no secret in any hub log", "no customer email or phone in any hub log"} <= names


def test_throttle_violation_detection():
    t0 = datetime(2026, 11, 10, tzinfo=UTC)

    def call(offset, status=None, ra=None):
        return {
            "destination": "w",
            "kind": "send",
            "started_at": t0 + timedelta(seconds=offset),
            "latency_ms": 10,
            "status_code": status,
            "retry_after_s": ra,
        }

    throttled, honoured, violations = throttle_violations([call(0, 429, 1.0), call(0.5), call(1.2)])
    assert (throttled, honoured, len(violations)) == (1, 0, 1)  # 0.5 s is inside the 1 s window
    assert (
        throttle_violations([call(0, 429, 1.0), call(0.1), call(1.2)])[1] == 1
    )  # 0.1 s was already in flight


def test_percentile():
    assert percentile([], 0.5) is None
    assert percentile([1, 2, 3, 4], 0.5) == 2.5
    assert percentile(list(range(101)), 0.95) == 95


# ---- the review's holes -----------------------------------------------------------------------------


def test_late_copy_of_create_after_cancel_fails_ordering_whatever_the_cause():
    """prepare, cancel, prepare: a cancelled order ships. Judged on every arrival, no window exemption."""
    ev = good_evidence()
    k, first = first_wh(ev)
    ev["received"]["warehouse"].append(dict(first, n=1000))
    ev["attempts"].append(
        _attempt("warehouse", k, error_class="timeout", latency=2000)
    )  # even "in the window"
    verdict = evaluate(**ev)
    assert ORDERING in failed(verdict)
    assert verdict.numbers["order_violations"] == 1
    assert not verdict.passed


def test_ordering_is_judged_on_every_arrival_not_the_first_per_key():
    """The first copy of each key is in order; only the later copy is not. A first-per-key judge misses it."""
    ev = good_evidence()
    k, first = first_wh(ev)
    ev["received"]["warehouse"].append(dict(first, n=5000))
    assert ORDERING in failed(evaluate(**ev))


def test_extra_copies_are_counted_not_just_keys():
    """Four copies of one shipment is three extra copies, not '1 doubled'."""
    ev = good_evidence()
    first = first_wh(ev)[1]
    for i in range(3):
        ev["received"]["warehouse"].append(dict(first, n=2000 + i))
    verdict = evaluate(**ev)
    assert verdict.numbers["doubled"] == 1 and verdict.numbers["extra_copies"] == 3
    assert EXACTLY_ONCE in failed(verdict)


def test_one_ambiguous_attempt_does_not_excuse_unlimited_copies():
    """Four copies with one timeout anywhere in the history used to PASS."""
    ev = good_evidence()
    k, first = first_wh(ev)
    for i in range(3):
        ev["received"]["warehouse"].append(dict(first, n=2000 + i))
    ev["attempts"].append(_attempt("warehouse", k, error_class="timeout", latency=2000))
    assert not evaluate(**ev).passed


def test_wrong_order_number_at_a_mock_fails():
    ev = good_evidence()
    for d in ev["received"]:
        for e in ev["received"][d]:
            e["order_number"] = "SOMEONE-ELSES-ORDER"
    assert PAYLOAD in failed(evaluate(**ev))


def test_wrong_action_at_a_mock_fails():
    ev = good_evidence()
    ev["received"]["accounting"][0]["kind"] = "credit_note"  # right order, wrong document
    assert PAYLOAD in failed(evaluate(**ev))
    ev = good_evidence()
    ev["received"]["warehouse"][0]["kind"] = "cancel"  # a shipment prepared as a cancel
    assert PAYLOAD in failed(evaluate(**ev))


def _late(ev, event_id="e1"):
    for m in ev["manifest"]:
        if m["event_id"] == event_id:
            m["late"] = True


def test_late_event_delivered_in_order_is_fine():
    ev = good_evidence()
    _late(ev)
    assert evaluate(**ev).passed


def test_late_event_superseded_and_not_sent_is_fine_and_counted():
    ev = good_evidence()
    _late(ev)
    k = delivery_key("e1", "warehouse")
    ev["received"]["warehouse"] = [e for e in ev["received"]["warehouse"] if e["reference"] != k]
    for r in ev["deliveries"]:
        if (r["event_id"], r["destination"]) == ("e1", "warehouse"):
            r["status"], r["delivered_via"] = "superseded", None
    verdict = evaluate(**ev)
    assert verdict.passed, failed(verdict)
    assert verdict.numbers["superseded"] == 1


def test_late_event_reported_superseded_but_sent_anyway_fails():
    ev = good_evidence()
    _late(ev)
    for r in ev["deliveries"]:
        if (r["event_id"], r["destination"]) == ("e1", "warehouse"):
            r["status"], r["delivered_via"] = "superseded", None
    assert not evaluate(**ev).passed  # the warehouse still has it


def test_late_event_reported_delivered_but_never_sent_is_lost():
    ev = good_evidence()
    _late(ev)
    k = delivery_key("e1", "warehouse")
    ev["received"]["warehouse"] = [e for e in ev["received"]["warehouse"] if e["reference"] != k]
    assert LOST in failed(evaluate(**ev))


def test_per_destination_latency_is_reported():
    verdict = evaluate(**good_evidence())
    assert set(verdict.numbers["per_destination"]) == set(DESTS)
    assert verdict.numbers["per_destination"]["warehouse"]["p50_s"] == 1.0
