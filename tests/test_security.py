from __future__ import annotations

import pytest

from hub.security import SignatureError, sign, verify
from mocks.storefront import sign as mock_sign

BODY = b'{"id":"evt_1","type":"order.created"}'
NOW = 1_800_000_000.0


def check(secrets, body=BODY, sig=None, ts=NOW, now=NOW, **kw):
    ts_str = str(int(ts))
    sig = sig if sig is not None else sign(secrets[0], int(ts), body)
    return verify(secrets, body, sig, ts_str, now=now, **kw)


def code(fn) -> str:
    with pytest.raises(SignatureError) as exc:
        fn()
    return exc.value.code


def test_good_signature():
    assert check(["s"]) == int(NOW)


def test_mock_signer_is_accepted_by_hub_verifier():
    ts = int(NOW)
    assert verify(["s"], BODY, mock_sign("s", ts, BODY), str(ts), now=NOW) == ts


def test_bad_signature():
    assert code(lambda: check(["s"], sig=sign("wrong", int(NOW), BODY))) == "bad_signature"


def test_body_tampering_is_detected():
    sig = sign("s", int(NOW), BODY)
    assert code(lambda: check(["s"], body=BODY + b" ", sig=sig)) == "bad_signature"


def test_timestamp_is_part_of_the_signature():
    sig = sign("s", int(NOW), BODY)
    assert code(lambda: verify(["s"], BODY, sig, str(int(NOW) + 1), now=NOW)) == "bad_signature"


def test_expired_timestamp():
    old = NOW - 301
    assert code(lambda: check(["s"], ts=old)) == "timestamp_outside_window"


def test_window_edge_is_inclusive():
    assert check(["s"], ts=NOW - 300) == int(NOW - 300)


def test_future_timestamp_is_rejected_too():
    """Sender clock ahead by more than the window is rejected, not silently accepted."""
    assert code(lambda: check(["s"], ts=NOW + 301)) == "timestamp_outside_window"


def test_small_clock_skew_is_tolerated():
    assert check(["s"], ts=NOW + 120) == int(NOW + 120)


def test_unsigned_request_learns_nothing_about_the_clock():
    """A bad signature with an out-of-window timestamp reports the signature, not the clock."""
    assert code(lambda: check(["s"], sig="sha256=00", ts=NOW - 10_000)) == "bad_signature"


@pytest.mark.parametrize("sig,ts", [(None, "1"), ("sha256=x", None), ("", ""), ("sha256=x", "not-a-number")])
def test_missing_or_malformed_headers(sig, ts):
    assert code(lambda: verify(["s"], BODY, sig, ts, now=NOW)) in ("missing_headers", "bad_timestamp")


def test_previous_secret_is_accepted_during_rotation():
    old_sig = sign("previous", int(NOW), BODY)
    assert verify(["current", "previous"], BODY, old_sig, str(int(NOW)), now=NOW) == int(NOW)


def test_retired_secret_is_rejected():
    sig = sign("retired", int(NOW), BODY)
    assert code(lambda: verify(["current", "previous"], BODY, sig, str(int(NOW)), now=NOW)) == "bad_signature"


def test_no_secret_configured_refuses():
    assert code(lambda: verify([], BODY, "sha256=x", "1", now=NOW)) == "no_secret_configured"


def test_compare_is_constant_time():
    import inspect

    import hub.security as sec

    assert "compare_digest" in inspect.getsource(sec.verify)
