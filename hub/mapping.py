"""Mapping layer: storefront event -> each destination's payload. Pure functions, no I/O.

Mappers never judge business validity (negative prices, odd currencies): that is the destination's
call and shows up as a permanent 4xx. They only raise MappingError when the event cannot be
translated at all (required structure missing), which dead-letters that delivery without a network call.
"""

from __future__ import annotations

import uuid
from decimal import Decimal, InvalidOperation
from typing import Any

KEY_NAMESPACE = uuid.UUID("6f1c5a52-0b1e-4e0e-9a54-6a1a7c0a4e11")


class MappingError(ValueError):
    pass


def delivery_key(event_id: str, destination: str) -> str:
    """Deterministic per (event, destination); stable across retries, replays and restarts."""
    return str(uuid.uuid5(KEY_NAMESPACE, f"{event_id}|{destination}"))


def _need(obj: Any, path: str) -> Any:
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur or cur[part] in (None, ""):
            raise MappingError(f"missing {path}")
        cur = cur[part]
    return cur


def _money(value: Any, path: str) -> str:
    try:
        return format(Decimal(str(value)), "f")
    except (InvalidOperation, ValueError):
        raise MappingError(f"not a number: {path}") from None


def _lines(order: dict) -> list[dict]:
    lines = _need(order, "lines")
    if not isinstance(lines, list) or not lines:
        raise MappingError("lines must be a non-empty list")
    out = []
    for i, line in enumerate(lines):
        if not isinstance(line, dict):
            raise MappingError(f"lines[{i}] is not an object")
        out.append(
            {
                "sku": _need(line, "sku"),
                "name": line.get("name", ""),
                "quantity": _need(line, "quantity"),
                "unit_price": _money(_need(line, "unit_price"), f"lines[{i}].unit_price"),
            }
        )
    return out


def _parts(event: dict, destination: str) -> tuple[str, str, dict, str]:
    event_id = _need(event, "id")
    etype = _need(event, "type")
    order = _need(event, "data.order")
    if not isinstance(order, dict):
        raise MappingError("data.order is not an object")
    return delivery_key(event_id, destination), etype, order, str(event.get("created_at", ""))


def to_warehouse(event: dict) -> dict:
    ref, etype, order, occurred = _parts(event, "warehouse")
    action = {"order.created": "prepare", "order.paid": "release", "order.cancelled": "cancel"}.get(etype)
    if action is None:
        raise MappingError(f"unsupported event type {etype}")
    addr = _need(order, "shipping_address")
    return {
        "external_ref": ref,
        "action": action,
        "order_number": _need(order, "number"),
        "ship_to": {
            "name": _need(order, "customer.name"),
            "line1": _need(addr, "line1"),
            "city": _need(addr, "city"),
            "postal_code": _need(addr, "postal_code"),
            "country": _need(addr, "country"),
        },
        "items": [{"sku": ln["sku"], "quantity": ln["quantity"]} for ln in _lines(order)],
        "occurred_at": occurred,
    }


def to_helpdesk(event: dict) -> dict:
    ref, etype, order, occurred = _parts(event, "helpdesk")
    total = _money(_need(order, "total"), "total")
    number = _need(order, "number")
    notes = {
        "order.created": f"Order {number} placed, total {total}",
        "order.paid": f"Order {number} paid",
        "order.cancelled": f"Order {number} cancelled",
    }
    if etype not in notes:
        raise MappingError(f"unsupported event type {etype}")
    customer = _need(order, "customer")
    return {
        "reference": ref,
        "customer": {
            "email": _need(customer, "email"),
            "name": _need(customer, "name"),
            "phone": customer.get("phone"),
        },
        "order_number": number,
        "event_type": etype,
        "note": notes[etype],
        "order_total": total,
        "occurred_at": occurred,
    }


def to_accounting(event: dict) -> dict:
    ref, etype, order, occurred = _parts(event, "accounting")
    doc_type = {"order.created": "invoice", "order.paid": "payment", "order.cancelled": "credit_note"}.get(
        etype
    )
    if doc_type is None:
        raise MappingError(f"unsupported event type {etype}")
    return {
        "reference": ref,
        "document_type": doc_type,
        "order_number": _need(order, "number"),
        "currency": _need(order, "currency"),
        "customer": {"email": _need(order, "customer.email"), "name": _need(order, "customer.name")},
        "lines": [
            {
                "sku": ln["sku"],
                "description": ln["name"],
                "quantity": ln["quantity"],
                "unit_price": ln["unit_price"],
            }
            for ln in _lines(order)
        ],
        "total": _money(_need(order, "total"), "total"),
        "occurred_at": occurred,
    }


MAPPERS = {"warehouse": to_warehouse, "helpdesk": to_helpdesk, "accounting": to_accounting}
