"""Webhook signature verification.

Signed string: f"{timestamp}." + raw body. Header X-Larkfield-Signature: sha256=<hex>.
Header X-Larkfield-Timestamp: unix seconds. The window is symmetric: a timestamp too far in the
future (sender clock ahead) is rejected the same as one too far in the past (replay).
"""

from __future__ import annotations

import hashlib
import hmac
import time
from collections.abc import Sequence

SIGNATURE_HEADER = "X-Larkfield-Signature"
TIMESTAMP_HEADER = "X-Larkfield-Timestamp"
SCHEME = "sha256="


class SignatureError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def sign(secret: str, timestamp: int, body: bytes) -> str:
    mac = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return SCHEME + mac.hexdigest()


def verify(
    secrets: Sequence[str],
    body: bytes,
    signature: str | None,
    timestamp: str | None,
    *,
    window_s: int = 300,
    now: float | None = None,
) -> int:
    """Return the verified timestamp, or raise SignatureError with a short code.

    Signature is checked before the window so an unsigned caller learns nothing about the clock.
    Any configured secret is accepted (rotation window).
    """
    if not signature or not timestamp:
        raise SignatureError("missing_headers")
    try:
        ts = int(timestamp)
    except ValueError:
        raise SignatureError("bad_timestamp") from None
    if not secrets:
        raise SignatureError("no_secret_configured")
    ok = False
    for secret in secrets:  # no early exit: constant work per secret
        # Bytes, not str: compare_digest raises TypeError on a non-ASCII str, which would be a 500, not a 401.
        ok |= hmac.compare_digest(sign(secret, ts, body).encode(), signature.encode("utf-8", "replace"))
    if not ok:
        raise SignatureError("bad_signature")
    current = time.time() if now is None else now
    if abs(current - ts) > window_s:
        raise SignatureError("timestamp_outside_window")
    return ts
