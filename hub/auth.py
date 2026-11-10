"""Credentials for destinations: OAuth2 client-credentials with a token cache, and a static API key."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass

import httpx


class TokenError(Exception):
    """The token endpoint did not give us a token. No API request was sent, so nothing is ambiguous."""

    def __init__(self, error_class: str, status_code: int | None = None, detail: str = ""):
        super().__init__(f"{error_class} {status_code or ''} {detail}".strip())
        self.error_class = error_class
        self.status_code = status_code
        self.detail = detail
        # Outage-type failures count toward the circuit breaker; a rejected credential does not.
        self.outage = status_code is None or status_code >= 500


@dataclass
class _Token:
    value: str
    expires_at: float  # monotonic


class OAuthClientCredentials:
    """Caches one token. Refreshes before expiry, on demand, and exactly once under concurrency.

    The expiry margin is min(10 s, 20% of the lifetime) so a token that is valid when we pick it is
    still valid when the request lands. A 401 despite that (clock drift, early revocation) is handled by
    invalidate(): it only drops the cache if the failed token is still the cached one, so a slow request
    holding an old token cannot throw away a fresh token another task just fetched.
    """

    refreshable = True

    def __init__(
        self,
        http: httpx.AsyncClient,
        token_url: str,
        client_id: str,
        client_secret: str,
        *,
        clock: Callable[[], float] = time.monotonic,
        timeout_s: float = 10.0,
    ):
        self._http = http
        self._url = token_url
        self._id = client_id
        self._secret = client_secret
        self._clock = clock
        self._timeout = timeout_s
        self._token: _Token | None = None
        self._lock = asyncio.Lock()
        self.fetches = 0

    async def header(self) -> tuple[dict[str, str], str]:
        """Return (Authorization header, token) so the caller can report which token failed."""
        token = self._token
        if token is None or self._clock() >= token.expires_at:
            token = await self._refresh(stale=token)
        return {"Authorization": f"Bearer {token.value}"}, token.value

    async def invalidate(self, failed_token: str) -> None:
        async with self._lock:
            if self._token is not None and self._token.value == failed_token:
                self._token = None

    async def _refresh(self, stale: _Token | None) -> _Token:
        async with self._lock:
            current = self._token
            if current is not None and current is not stale and self._clock() < current.expires_at:
                return current  # someone else refreshed while we waited for the lock
            try:
                resp = await self._http.post(
                    self._url,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": self._id,
                        "client_secret": self._secret,
                    },
                    timeout=self._timeout,
                )
            except httpx.TimeoutException as exc:
                raise TokenError("token_timeout", None, exc.__class__.__name__) from exc
            except httpx.TransportError as exc:
                raise TokenError("token_network_error", None, exc.__class__.__name__) from exc
            if resp.status_code != 200:
                raise TokenError(
                    "token_rejected" if resp.status_code < 500 else "token_server_error", resp.status_code
                )
            try:
                body = resp.json()
                value = body["access_token"]
                ttl = float(body.get("expires_in", 60))
            except (ValueError, KeyError, TypeError) as exc:
                raise TokenError("token_malformed", resp.status_code) from exc
            margin = min(10.0, ttl * 0.2)
            self._token = _Token(value, self._clock() + max(0.0, ttl - margin))
            self.fetches += 1
            return self._token


class ApiKeyAuth:
    refreshable = False

    def __init__(self, header_name: str, key: str):
        self._headers = {header_name: key}

    async def header(self) -> tuple[dict[str, str], str]:
        return dict(self._headers), ""

    async def invalidate(self, failed_token: str) -> None:
        return None
