"""Structured JSON logs. Nothing sensitive is logged by construction; secret and PII redaction is the net."""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from typing import Any

_SENSITIVE_KEY = re.compile(r"secret|token|authorization|api[-_]?key|password|signature|credential", re.I)
REDACTED = "[redacted]"
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")
_PHONE_RE = re.compile(r"\+\d[\d\s-]{8,}\d|(?<![\w.-])\d{10,}(?!\w)")
_known_secrets: list[str] = []


def mask_pii(text: str) -> str:
    """Replace emails with [email] and phone numbers with [phone] (same idea as project 1)."""
    return _PHONE_RE.sub("[phone]", _EMAIL_RE.sub("[email]", text))


def set_known_secrets(values: list[str]) -> None:
    """Exact secret values to scrub from any string that reaches a log line or a stored error body."""
    _known_secrets[:] = sorted({v for v in values if v}, key=len, reverse=True)


def redact_text(text: str) -> str:
    """Scrub configured secrets and customer contact details from any string that is logged or stored."""
    for secret in _known_secrets:
        if secret in text:
            text = text.replace(secret, REDACTED)
    return mask_pii(text)


def redact(value: Any, key: str = "") -> Any:
    if key and _SENSITIVE_KEY.search(key):
        return REDACTED
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, dict):
        return {k: redact(v, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(v) for v in value]
    return value


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "event": redact_text(record.getMessage()),
        }
        fields = getattr(record, "fields", None)
        if fields:
            entry.update(redact(fields))
        if record.exc_info:
            entry["exc"] = redact_text(self.formatException(record.exc_info))
        return json.dumps(entry, default=str, ensure_ascii=False)


def configure_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    for noisy in ("httpx", "httpcore", "psycopg.pool", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def log(logger: logging.Logger, event: str, level: int = logging.INFO, **fields: Any) -> None:
    logger.log(level, event, extra={"fields": fields})
