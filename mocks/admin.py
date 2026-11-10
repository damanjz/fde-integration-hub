"""Mock admin API: fault injection, received-request record, stats, token revocation. Admin key required."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from mocks import core

router = APIRouter(prefix="/admin")


def guard(request: Request) -> None:
    if not core.admin_authorized(request):
        raise HTTPException(401, "admin key required (MOCK_ADMIN_KEY)")


@router.get("/faults", dependencies=[Depends(guard)])
async def get_faults() -> dict:
    return {name: f.model_dump() for name, f in core.STATE.faults.items()}


@router.put("/faults", dependencies=[Depends(guard)])
async def set_faults(update: dict[str, dict]) -> dict:
    """Partial update: {"warehouse": {"outage": true}, "helpdesk": {"error_rate": 0.2}}."""
    for name, fields in update.items():
        if name not in core.STATE.faults:
            raise HTTPException(404, f"unknown provider {name}")
        unknown = set(fields) - set(core.Faults.model_fields)
        if unknown:
            raise HTTPException(422, f"unknown fault fields {sorted(unknown)}")
        core.STATE.faults[name] = core.STATE.faults[name].model_copy(update=fields)
    return await get_faults()


@router.get("/received", dependencies=[Depends(guard)])
async def received() -> dict:
    """Every accepted (committed) request per provider, in arrival order. The chaos test reads this."""
    return {
        name: {"total": len(core.STATE.received_log[name]), "entries": core.STATE.received_log[name]}
        for name in core.PROVIDERS
    }


@router.get("/stats", dependencies=[Depends(guard)])
async def stats() -> dict:
    return {name: dict(c) for name, c in core.STATE.stats.items()}


@router.post("/tokens/revoke", dependencies=[Depends(guard)])
async def revoke_tokens(provider: str) -> dict:
    if provider not in core.STATE.tokens:
        raise HTTPException(404, "provider has no OAuth tokens")
    revoked = len(core.STATE.tokens[provider])
    core.STATE.tokens[provider].clear()
    return {"revoked": revoked}


@router.post("/reset", dependencies=[Depends(guard)])
async def reset() -> dict:
    core.STATE.reset()
    return {"reset": True}
