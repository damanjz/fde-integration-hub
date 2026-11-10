"""FastAPI app: webhook intake, dead-letter operations, metrics."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, Field

from hub import outbox
from hub.config import Settings, load_settings
from hub.db import open_pool
from hub.destinations import destination_names, destination_settings
from hub.logging_ import log, set_known_secrets
from hub.mapping import MAPPERS, delivery_key
from hub.security import SIGNATURE_HEADER, TIMESTAMP_HEADER, SignatureError, verify
from hub.worker import WorkerPool

logger = logging.getLogger("hub.api")
SUPPORTED_TYPES = {"order.created", "order.paid", "order.cancelled"}


class ReplayRequest(BaseModel):
    reason: str | None = None
    corrected_event: dict | None = None  # replaces the stored event for this one delivery
    force: bool = False  # override the helpdesk idempotency-window refusal


class SkipRequest(BaseModel):
    reason: str = Field(min_length=3, max_length=500)


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        when = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return when if when.tzinfo else when.replace(tzinfo=UTC)


def create_app(
    settings: Settings | None = None, *, pool: AsyncConnectionPool | None = None, start_workers: bool = True
) -> FastAPI:
    settings = settings or load_settings()
    if not settings.secrets_list():
        raise RuntimeError("HUB_WEBHOOK_SECRETS is empty: refusing to start without a webhook secret")
    set_known_secrets(settings.all_secret_values())
    destinations = [d for d in destination_names(settings) if d in MAPPERS]
    idempotency_ttls = {
        name: cfg.idempotency_ttl_s
        for name, cfg in destination_settings(settings).items()
        if cfg.idempotency_ttl_s is not None
    }

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        owned = pool is None
        app.state.pool = pool or await open_pool(settings.database_url, settings.pool_size)
        workers = None
        if start_workers and settings.workers > 0:
            workers = WorkerPool(settings, app.state.pool, name="api")
            await workers.start(settings.workers)
        try:
            yield
        finally:
            if workers:
                await workers.stop()
            if owned:
                await app.state.pool.close()

    app = FastAPI(title="Larkfield integration hub", lifespan=lifespan)
    app.state.settings = settings
    if pool is not None:
        app.state.pool = pool

    def get_pool(request: Request) -> AsyncConnectionPool:
        return request.app.state.pool

    def require_admin(x_admin_key: str | None = Header(default=None)) -> None:
        configured = settings.admin_key.get_secret_value()
        if not configured:
            raise HTTPException(503, "admin endpoints are disabled: set HUB_ADMIN_KEY")
        if not x_admin_key or not hmac.compare_digest(x_admin_key.encode(), configured.encode()):
            raise HTTPException(401, "bad admin key")

    @app.get("/healthz")
    async def healthz(pool: AsyncConnectionPool = Depends(get_pool)):
        async with pool.connection() as conn:
            await conn.execute("SELECT 1")
        return {"ok": True}

    @app.post("/webhooks/storefront")
    async def storefront_webhook(request: Request, pool: AsyncConnectionPool = Depends(get_pool)):
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > settings.webhook_max_body:
            return JSONResponse({"error": "body_too_large"}, status_code=413)
        body = await request.body()
        if len(body) > settings.webhook_max_body:
            return JSONResponse({"error": "body_too_large"}, status_code=413)
        try:
            verify(
                settings.secrets_list(),
                body,
                request.headers.get(SIGNATURE_HEADER),
                request.headers.get(TIMESTAMP_HEADER),
                window_s=settings.webhook_window_s,
            )
        except SignatureError as exc:
            await outbox.bump_counter(pool, f"rejected_{exc.code}")
            log(logger, "webhook_rejected", logging.WARNING, reason=exc.code)
            return JSONResponse({"error": exc.code}, status_code=401)

        try:
            payload = json.loads(body)
            event_id, event_type = payload["id"], payload["type"]
            if not (isinstance(event_id, str) and 0 < len(event_id) <= 128 and isinstance(event_type, str)):
                raise ValueError("id and type must be strings")
        except (ValueError, KeyError, TypeError) as exc:
            await outbox.bump_counter(pool, "rejected_malformed")
            log(logger, "webhook_malformed", logging.WARNING, error=exc.__class__.__name__)
            return JSONResponse({"error": "malformed_event"}, status_code=400)

        data = payload.get("data")
        order = data.get("order") if isinstance(data, dict) else None
        order_id = order.get("id") if isinstance(order, dict) and isinstance(order.get("id"), str) else None
        supported = event_type in SUPPORTED_TYPES
        occurred_at = _parse_time(payload.get("created_at")) or datetime.now(UTC)
        result = await outbox.ingest_event(
            pool,
            event_id=event_id,
            event_type=event_type,
            order_id=order_id,
            occurred_at=occurred_at,
            raw_body=body.decode("utf-8", errors="replace"),
            payload=payload,
            body_sha256=hashlib.sha256(body).hexdigest(),
            destinations=[(d, delivery_key(event_id, d)) for d in destinations] if supported else [],
        )
        if not result.inserted:
            log(logger, "webhook_duplicate", event_id=event_id, body_mismatch=result.body_mismatch)
            return {"status": "duplicate", "event_id": event_id}
        log(
            logger,
            "webhook_accepted",
            event_id=event_id,
            type=event_type,
            deliveries=len(destinations) if supported else 0,
        )
        return {"status": "accepted" if supported else "ignored", "event_id": event_id}

    @app.get("/deliveries", dependencies=[Depends(require_admin)])
    async def deliveries(
        status: str | None = Query(
            default=None, pattern="^(pending|in_flight|delivered|dead|blocked|superseded|skipped)$"
        ),
        destination: str | None = None,
        since: datetime | None = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        pool: AsyncConnectionPool = Depends(get_pool),
    ):
        rows = await outbox.list_deliveries(
            pool, status=status, destination=destination, since=since, limit=limit, offset=offset
        )
        return {"count": len(rows), "items": rows}

    @app.get("/deliveries/{delivery_id}", dependencies=[Depends(require_admin)])
    async def delivery(delivery_id: int, pool: AsyncConnectionPool = Depends(get_pool)):
        row = await outbox.get_delivery(pool, delivery_id)
        if row is None:
            raise HTTPException(404, "no such delivery")
        return row

    def actor_of(x_actor: str | None) -> str:
        return (x_actor or "admin")[:80]

    @app.post("/deliveries/{delivery_id}/replay", dependencies=[Depends(require_admin)])
    async def replay_delivery(
        delivery_id: int,
        body: ReplayRequest | None = None,
        x_actor: str | None = Header(default=None),
        pool: AsyncConnectionPool = Depends(get_pool),
    ):
        """Dead rows only. A corrected_event replaces the stored event for this one delivery."""
        body = body or ReplayRequest()
        existing = await outbox.get_delivery(pool, delivery_id)
        if existing is None:
            raise HTTPException(404, "no such delivery")
        if existing["status"] != "dead":
            hint = " (a later event was already delivered)" if existing["status"] == "superseded" else ""
            raise HTTPException(
                409, f"delivery is {existing['status']}{hint}, only dead deliveries can be replayed"
            )
        try:
            result = await outbox.replay(
                pool,
                ids=[delivery_id],
                actor=actor_of(x_actor),
                reason=body.reason,
                corrected_event=body.corrected_event,
                force=body.force,
                idempotency_ttl_s=idempotency_ttls,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        if result.refused:
            raise HTTPException(409, f"replay refused: {result.refused[0][1]}; send force=true to override")
        if not result.replayed:  # lost a race with another operator or a worker
            raise HTTPException(409, "delivery is no longer dead")
        log(logger, "delivery_replayed", delivery_id=delivery_id, corrected=body.corrected_event is not None)
        return {"replayed": result.replayed}

    @app.post("/deliveries/{delivery_id}/skip", dependencies=[Depends(require_admin)])
    async def skip_delivery(
        delivery_id: int,
        body: SkipRequest,
        x_actor: str | None = Header(default=None),
        pool: AsyncConnectionPool = Depends(get_pool),
    ):
        """Abandon a dead delivery on purpose so the rows blocked behind it go out. Reason required."""
        if not await outbox.skip(pool, delivery_id, actor=actor_of(x_actor), reason=body.reason):
            existing = await outbox.get_delivery(pool, delivery_id)
            if existing is None:
                raise HTTPException(404, "no such delivery")
            raise HTTPException(409, f"delivery is {existing['status']}, only dead deliveries can be skipped")
        log(logger, "delivery_skipped", delivery_id=delivery_id)
        return {"skipped": delivery_id}

    @app.get("/metrics", dependencies=[Depends(require_admin)])
    async def metrics(pool: AsyncConnectionPool = Depends(get_pool)):
        return await outbox.metrics(pool, destinations)

    return app
