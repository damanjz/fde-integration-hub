from __future__ import annotations

import asyncio

import httpx
import pytest

from hub.auth import OAuthClientCredentials, TokenError

pytestmark = pytest.mark.anyio


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def make(handler, clock=None):
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return OAuthClientCredentials(http, "http://idp/token", "id", "secret", clock=clock or Clock())


def token_handler(ttl=60, calls=None):
    state = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["n"] += 1
        if calls is not None:
            calls.append(request)
        return httpx.Response(200, json={"access_token": f"t{state['n']}", "expires_in": ttl})

    return handler


async def test_token_is_cached():
    auth = make(token_handler())
    h1, t1 = await auth.header()
    h2, t2 = await auth.header()
    assert t1 == t2 == "t1" and h1 == {"Authorization": "Bearer t1"}
    assert auth.fetches == 1


async def test_client_credentials_form_is_sent():
    calls = []
    auth = make(token_handler(calls=calls))
    await auth.header()
    body = calls[0].content.decode()
    assert "grant_type=client_credentials" in body and "client_id=id" in body


async def test_token_refreshes_before_it_expires():
    clock = Clock()
    auth = make(token_handler(ttl=60), clock)
    _, t1 = await auth.header()
    clock.t += 49  # 60 s lifetime minus a 10 s margin: still fresh at 49 s
    _, t2 = await auth.header()
    assert t2 == t1
    clock.t += 2  # 51 s: inside the margin, so refresh before it can expire mid-request
    _, t3 = await auth.header()
    assert t3 != t1 and auth.fetches == 2


async def test_short_lived_token_margin_scales_down():
    clock = Clock()
    auth = make(token_handler(ttl=5), clock)  # margin = 20% = 1 s
    await auth.header()
    clock.t += 3.9
    assert (await auth.header())[1] == "t1"
    clock.t += 0.2
    assert (await auth.header())[1] == "t2"


async def test_concurrent_callers_trigger_one_refresh():
    auth = make(token_handler())
    results = await asyncio.gather(*[auth.header() for _ in range(20)])
    assert {t for _, t in results} == {"t1"} and auth.fetches == 1


async def test_invalidate_forces_refresh_on_a_401():
    auth = make(token_handler())
    _, t1 = await auth.header()
    await auth.invalidate(t1)
    _, t2 = await auth.header()
    assert t2 != t1


async def test_stale_invalidate_does_not_drop_a_fresh_token():
    """A slow request holding an old token gets a 401 after another task already refreshed."""
    auth = make(token_handler())
    _, old = await auth.header()
    await auth.invalidate(old)
    _, fresh = await auth.header()
    await auth.invalidate(old)  # late 401 for the old token
    _, still = await auth.header()
    assert still == fresh and auth.fetches == 2


async def test_token_endpoint_failures_raise_token_error():
    def handler(request):
        return httpx.Response(503)

    with pytest.raises(TokenError) as exc:
        await make(handler).header()
    assert exc.value.error_class == "token_server_error" and exc.value.outage


async def test_rejected_credentials_are_not_an_outage():
    def handler(request):
        return httpx.Response(401, json={"error": "invalid_client"})

    with pytest.raises(TokenError) as exc:
        await make(handler).header()
    assert exc.value.error_class == "token_rejected" and not exc.value.outage


async def test_token_endpoint_timeout_and_garbage():
    def slow(request):
        raise httpx.ReadTimeout("slow", request=request)

    with pytest.raises(TokenError) as exc:
        await make(slow).header()
    assert exc.value.error_class == "token_timeout"

    def garbage(request):
        return httpx.Response(200, text="not json")

    with pytest.raises(TokenError) as exc:
        await make(garbage).header()
    assert exc.value.error_class == "token_malformed"
