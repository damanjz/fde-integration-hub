from __future__ import annotations

import httpx
import pytest

from hub.auth import ApiKeyAuth, OAuthClientCredentials
from hub.client import DestinationClient, DestinationSpec
from hub.gate import GateBlocked, GateConfig, MemoryGate
from hub.retry import Outcome

pytestmark = pytest.mark.anyio


def build(
    handler, *, oauth=True, duplicate=(), idem=False, gate=None, lookup=True, dup_error=None, dup_field=None
):
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    auth = (
        OAuthClientCredentials(http, "http://d/oauth/token", "id", "secret")
        if oauth
        else ApiKeyAuth("X-Api-Key", "k3y")
    )
    spec = DestinationSpec(
        "d",
        "http://d",
        "/send",
        "/find" if lookup else None,
        2.0,
        auth,
        lookup_param="ref",
        idempotency_header=idem,
        duplicate_statuses=tuple(duplicate),
        duplicate_error=dup_error,
        duplicate_number_field=dup_field,
    )
    gate = gate or MemoryGate({"d": GateConfig(rate=1000, burst=1000, breaker_threshold=100)})
    return DestinationClient(spec, http, gate), gate


class Server:
    """Issues tokens, validates them, and lets tests script the next API responses."""

    def __init__(self, script=None):
        self.script = list(script or [])
        self.valid: set[str] = set()
        self.n = 0
        self.seen: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth/token":
            self.n += 1
            tok = f"t{self.n}"
            self.valid.add(tok)
            return httpx.Response(200, json={"access_token": tok, "expires_in": 60})
        self.seen.append(request)
        if (
            request.headers.get("authorization", "")[7:] not in self.valid
            and "authorization" in request.headers
        ):
            return httpx.Response(401, json={"error": "invalid_token"})
        item = self.script.pop(0) if self.script else 201
        if isinstance(item, Exception):
            raise item
        if isinstance(item, tuple):
            return httpx.Response(item[0], headers=item[1], text=item[2] if len(item) > 2 else "body")
        return httpx.Response(item, text="body")


async def test_success_and_call_record():
    server = Server()
    client, _ = build(server)
    res = await client.send({"a": 1}, "key-1")
    assert res.outcome is Outcome.SUCCESS and res.status_code == 201
    assert len(res.calls) == 1 and res.calls[0].kind == "send" and res.calls[0].status_code == 201


async def test_401_refreshes_the_token_once_and_resends_in_the_same_attempt():
    server = Server()
    client, _ = build(server)
    await client.send({}, "k")  # caches t1
    server.valid.clear()  # token expires mid-run
    res = await client.send({}, "k")
    assert res.outcome is Outcome.SUCCESS
    assert [c.status_code for c in res.calls] == [401, 201]  # both calls are in the failure log
    assert server.n == 2


async def test_persistent_401_is_a_retryable_auth_failure_not_a_loop():
    server = Server(script=[401, 401])  # scripted 401s on both the first call and the post-refresh call
    client, _ = build(server)
    server.valid = set()
    res = await client.send({}, "k")
    assert res.outcome is Outcome.RETRY and res.error_class == "auth_rejected"
    assert [c.status_code for c in res.calls] == [401, 401]  # exactly one refresh, then give up


async def test_api_key_401_is_not_refreshed():
    server = Server(script=[401])
    client, _ = build(server, oauth=False)
    res = await client.send({}, "k")
    assert res.outcome is Outcome.RETRY and res.error_class == "auth_rejected" and len(res.calls) == 1


async def test_api_key_header_is_sent():
    server = Server()
    client, _ = build(server, oauth=False)
    await client.send({}, "k")
    assert server.seen[0].headers["x-api-key"] == "k3y"


async def test_429_blocks_the_gate_for_retry_after():
    server = Server(script=[(429, {"Retry-After": "7"})])
    client, gate = build(server)
    res = await client.send({}, "k")
    assert res.outcome is Outcome.THROTTLED and res.retry_after_s == 7.0
    with pytest.raises(GateBlocked) as exc:
        await client.send({}, "k")
    assert 5 < exc.value.wait_s <= 7


async def test_429_without_retry_after_defaults_to_a_second():
    client, _ = build(Server(script=[429]))
    assert (await client.send({}, "k")).retry_after_s == 1.0


async def test_5xx_is_retry_and_ambiguous():
    client, gate = build(Server(script=[503]))
    res = await client.send({}, "k")
    assert res.outcome is Outcome.RETRY and res.ambiguous and res.status_code == 503
    assert gate.state("d").consecutive_failures == 1


async def test_timeout_is_ambiguous_and_connect_error_is_not():
    req = httpx.Request("POST", "http://d/send")
    client, _ = build(Server(script=[httpx.ReadTimeout("t", request=req)]))
    res = await client.send({}, "k")
    assert (res.outcome, res.error_class, res.ambiguous) == (Outcome.RETRY, "timeout", True)
    client, _ = build(Server(script=[httpx.ConnectError("c", request=req)]))
    res = await client.send({}, "k")
    assert (res.outcome, res.error_class, res.ambiguous) == (Outcome.RETRY, "connect_error", False)


async def test_422_is_permanent_with_body():
    client, gate = build(Server(script=[422]))
    res = await client.send({}, "k")
    assert res.outcome is Outcome.PERMANENT and res.body == "body"
    assert gate.state("d").consecutive_failures == 0  # a bad payload says nothing about destination health


DUP = {"duplicate": (409,), "dup_error": "duplicate_document_number", "dup_field": "number"}


async def test_409_counts_as_delivered_only_for_the_duplicate_error_with_our_key():
    ours = '{"error": "duplicate_document_number", "number": "k"}'
    client, _ = build(Server(script=[(409, {}, ours)]), **DUP)
    res = await client.send({}, "k")
    assert res.outcome is Outcome.SUCCESS and res.duplicate
    client, _ = build(Server(script=[(409, {}, ours)]))  # 409 not configured as a duplicate signal
    assert (await client.send({}, "k")).outcome is Outcome.PERMANENT


async def test_other_409s_are_permanent_failures():
    """Period closed, wrong state, a different document with the same number: none of these is 'delivered'."""
    for body in (
        '{"error": "accounting_period_closed"}',
        '{"error": "duplicate_document_number", "number": "someone-elses-key"}',
        '{"error": "duplicate_document_number"}',
        "not json",
        "[]",
    ):
        client, _ = build(Server(script=[(409, {}, body)]), **DUP)
        res = await client.send({}, "k")
        assert res.outcome is Outcome.PERMANENT and not res.duplicate, body


async def test_idempotency_key_header_only_when_configured():
    server = Server()
    client, _ = build(server, idem=True)
    await client.send({}, "the-key")
    assert server.seen[0].headers["idempotency-key"] == "the-key"
    server = Server()
    client, _ = build(server)
    await client.send({}, "the-key")
    assert "idempotency-key" not in server.seen[0].headers


async def test_lookup_found_and_not_found():
    hit = '{"items": [{"ref": "abc", "status": "accepted"}]}'
    server = Server(script=[(200, {}, hit), 404, (200, {}, '{"items": []}')])
    client, _ = build(server)
    found = await client.lookup("abc")
    assert found.outcome is Outcome.SUCCESS and found.found is True
    assert server.seen[0].url.params["ref"] == "abc"
    missing = await client.lookup("abc")
    assert missing.outcome is Outcome.SUCCESS and missing.found is False
    empty = await client.lookup("abc")  # a search endpoint: 200 with no matches is NOT "found"
    assert empty.outcome is Outcome.SUCCESS and empty.found is False


async def test_lookup_requires_a_record_carrying_our_key():
    for body in (
        '{"items": [{"ref": "someone-else"}]}',
        '{"items": [{"other_field": "abc"}]}',
        '[{"ref": "nope"}]',
        '{"found": false}',
        "{}",
    ):
        client, _ = build(Server(script=[(200, {}, body)]))
        res = await client.lookup("abc")
        assert res.outcome is Outcome.SUCCESS and res.found is False, body


async def test_unreadable_lookup_answer_is_a_failure_never_found():
    for body in ("<html>ok</html>", "", '"abc"', "42", '{"unrelated": 1}'):
        client, _ = build(Server(script=[(200, {}, body)]))
        res = await client.lookup("abc")
        assert res.outcome is Outcome.RETRY and res.found is None, body
        assert res.error_class == "lookup_unreadable"


async def test_lookup_accepts_single_record_and_list_shapes():
    for body in ('{"ref": "abc"}', '[{"ref": "abc"}]', '{"data": [{"ref": "abc"}]}'):
        client, _ = build(Server(script=[(200, {}, body)]))
        assert (await client.lookup("abc")).found is True, body


async def test_lookup_server_error_is_retry():
    client, _ = build(Server(script=[500]))
    assert (await client.lookup("abc")).outcome is Outcome.RETRY


async def test_token_endpoint_down_is_retryable_and_nothing_was_sent():
    def handler(request):
        return httpx.Response(503)

    client, gate = build(handler)
    res = await client.send({}, "k")
    assert res.outcome is Outcome.RETRY and res.error_class == "token_server_error" and not res.ambiguous
    assert gate.state("d").consecutive_failures == 1


async def test_response_body_is_scrubbed_of_secrets_and_contact_details():
    from hub.logging_ import set_known_secrets

    set_known_secrets(["k3y-secret"])

    def handler(request):
        return httpx.Response(422, text="bad email bob@example.test key k3y-secret phone +91 9876543210")

    client, _ = build(handler, oauth=False)
    res = await client.send({}, "k")
    assert (
        "bob@example.test" not in res.body and "k3y-secret" not in res.body and "9876543210" not in res.body
    )
    set_known_secrets([])
