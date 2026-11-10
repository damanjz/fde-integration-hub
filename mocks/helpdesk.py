"""Helpdesk mock: API key auth, random 500s and slow responses (faults), Idempotency-Key honoured for 24 h."""

from __future__ import annotations

import time

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from mocks import core

router = APIRouter(prefix="/helpdesk")
PROVIDER = "helpdesk"


@router.post("/v1/records")
async def upsert_record(request: Request) -> Response:
    blocked = await core.gateway(PROVIDER, request, auth="apikey")
    if blocked is not None:
        return blocked
    try:
        payload = await request.json()
        assert payload["reference"] and payload["customer"]["email"] and payload["order_number"]
    except (ValueError, KeyError, TypeError, AssertionError):
        core.STATE.stats[PROVIDER]["validation_422"] += 1
        return JSONResponse({"error": "invalid record"}, status_code=422)

    key = request.headers.get("idempotency-key")
    now = time.time()
    if key:
        cached = core.STATE.idempotency.get(key)
        if cached and cached[0] > now:  # same key within 24 h: answer as before, store nothing new
            core.STATE.stats[PROVIDER]["idempotent_replays"] += 1
            return JSONResponse(cached[2], status_code=cached[1], headers={"Idempotent-Replayed": "true"})
    body = {"record": payload["reference"], "status": "stored"}
    core.record(PROVIDER, payload["reference"], payload["order_number"], payload.get("event_type", ""))
    if key:
        core.STATE.idempotency[key] = (now + core.IDEMPOTENCY_TTL_S, 201, body)
    replaced = await core.after_commit(PROVIDER)
    if replaced is not None:
        return replaced
    return JSONResponse(body, status_code=201)
