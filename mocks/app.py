"""The mock provider app: one process, one router per provider, port 9000."""

from __future__ import annotations

from fastapi import FastAPI

from mocks import accounting, admin, helpdesk, storefront, warehouse


def create_app() -> FastAPI:
    app = FastAPI(title="Larkfield mock providers (simulated)")
    for module in (storefront, warehouse, helpdesk, accounting, admin):
        app.include_router(module.router)

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True}

    return app


app = create_app()
