"""Accounting mock: OAuth2 client credentials, strict payload validation (422 is permanent)."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field, ValidationError, model_validator

from mocks import core

router = APIRouter(prefix="/accounting")
PROVIDER = "accounting"


class Customer(BaseModel):
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    name: str = Field(min_length=1)


class Line(BaseModel):
    sku: str = Field(min_length=1)
    description: str = ""
    quantity: int = Field(ge=1)
    unit_price: Decimal = Field(ge=0)


class Document(BaseModel):
    reference: str = Field(min_length=1)
    document_type: Literal["invoice", "payment", "credit_note"]
    order_number: str = Field(min_length=1)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    customer: Customer
    lines: list[Line] = Field(min_length=1)
    total: Decimal = Field(ge=0)
    occurred_at: str = ""

    @model_validator(mode="after")
    def total_matches_lines(self) -> Document:
        expected = sum((ln.unit_price * ln.quantity for ln in self.lines), Decimal(0))
        if expected != self.total:
            raise ValueError(f"total {self.total} does not match lines {expected}")
        return self


@router.post("/oauth/token")
async def token(request: Request) -> Response:
    return await core.issue_token(PROVIDER, request)


@router.post("/v1/documents")
async def create_document(request: Request) -> Response:
    blocked = await core.gateway(PROVIDER, request, auth="oauth")
    if blocked is not None:
        return blocked
    try:
        doc = Document.model_validate_json(await request.body())
    except ValidationError as exc:
        core.STATE.stats[PROVIDER]["validation_422"] += 1
        errors = [{"loc": ".".join(str(p) for p in e["loc"]), "msg": e["msg"]} for e in exc.errors()]
        return JSONResponse({"error": "validation_failed", "errors": errors}, status_code=422)
    if core.STATE.received[PROVIDER][doc.reference] > 0:  # invoice numbers are unique
        core.STATE.stats[PROVIDER]["duplicate_409"] += 1
        return JSONResponse({"error": "duplicate_document_number", "number": doc.reference}, status_code=409)
    core.record(PROVIDER, doc.reference, doc.order_number, doc.document_type)
    replaced = await core.after_commit(PROVIDER)
    if replaced is not None:
        return replaced
    return JSONResponse({"document": doc.reference, "status": "booked"}, status_code=201)
