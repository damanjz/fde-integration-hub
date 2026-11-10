"""Warehouse (3PL) mock: OAuth2 client credentials (tokens expire), 5 req/s limit, no idempotency support."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from mocks import core

router = APIRouter(prefix="/warehouse")
PROVIDER = "warehouse"
ACTIONS = {"prepare", "release", "cancel"}


@router.post("/oauth/token")
async def token(request: Request) -> Response:
    return await core.issue_token(PROVIDER, request)


@router.post("/shipments")
async def create_shipment(request: Request) -> Response:
    blocked = await core.gateway(PROVIDER, request, auth="oauth")
    if blocked is not None:
        return blocked
    try:
        payload = await request.json()
        assert payload["action"] in ACTIONS and payload["external_ref"] and payload["order_number"]
        assert payload["ship_to"]["line1"] and payload["items"]
    except (ValueError, KeyError, TypeError, AssertionError):
        core.STATE.stats[PROVIDER]["validation_422"] += 1
        return JSONResponse({"error": "invalid shipment"}, status_code=422)
    # The Idempotency-Key header is deliberately ignored: this provider does not support it.
    core.record(PROVIDER, payload["external_ref"], payload["order_number"], payload["action"])
    replaced = await core.after_commit(PROVIDER)
    if replaced is not None:
        return replaced
    return JSONResponse({"shipment": payload["external_ref"], "status": "accepted"}, status_code=201)


@router.get("/shipments")
async def find_shipment(request: Request, external_ref: str) -> Response:
    blocked = await core.gateway(PROVIDER, request, auth="oauth")
    if blocked is not None:
        return blocked
    return core.lookup_response(PROVIDER, external_ref, "external_ref")
