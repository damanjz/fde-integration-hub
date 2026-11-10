from __future__ import annotations

import json
import logging

from hub.logging_ import JsonFormatter, log, mask_pii, redact, set_known_secrets


def render(**fields) -> dict:
    logger = logging.getLogger("test.fmt")
    record = logger.makeRecord("test.fmt", logging.INFO, __file__, 1, "something_happened", (), None)
    record.fields = fields
    return json.loads(JsonFormatter().format(record))


def test_output_is_one_json_object_with_standard_keys():
    out = render(delivery_id=5)
    assert out["event"] == "something_happened" and out["level"] == "info" and out["delivery_id"] == 5
    assert "ts" in out and out["logger"] == "test.fmt"


def test_sensitive_keys_are_redacted_whatever_the_value():
    out = render(client_secret="x", Authorization="Bearer abc", api_key="k", signature="sha256=1", token="t")
    assert all(
        out[k] == "[redacted]" for k in ("client_secret", "Authorization", "api_key", "signature", "token")
    )


def test_known_secret_values_are_scrubbed_from_free_text():
    set_known_secrets(["hunter2-secret"])
    out = render(error="upstream said: bad key hunter2-secret", nested={"a": ["hunter2-secret"]})
    assert "hunter2-secret" not in json.dumps(out)
    set_known_secrets([])


def test_emails_and_phones_are_masked():
    out = render(detail="contact jane.doe+x@example.co.in or +91 98765 43210 / 9876543210")
    assert (
        "jane.doe" not in out["detail"] and "98765" not in out["detail"] and "9876543210" not in out["detail"]
    )
    assert "[email]" in out["detail"] and "[phone]" in out["detail"]


def test_ids_and_timestamps_are_not_mistaken_for_phones():
    text = "evt_00042 2026-11-10T10:00:00 key 6f1c5a52-0b1e-4e0e-9a54-6a1a7c0a4e11 order LF-10042"
    assert mask_pii(text) == text


def test_log_helper_goes_through_the_formatter(caplog):
    with caplog.at_level(logging.INFO):
        log(logging.getLogger("test.helper"), "hello", delivery_id=1)
    assert caplog.records[-1].fields == {"delivery_id": 1}


def test_redact_handles_lists_and_leaves_numbers():
    assert redact({"n": 3, "items": ["a@b.co"]}) == {"n": 3, "items": ["[email]"]}
