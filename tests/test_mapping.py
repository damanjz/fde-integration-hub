from __future__ import annotations

import pytest

from hub.mapping import MappingError, delivery_key, to_accounting, to_helpdesk, to_warehouse
from tests.helpers import make_event


def test_delivery_key_is_deterministic_and_per_destination():
    assert delivery_key("evt_1", "warehouse") == delivery_key("evt_1", "warehouse")
    assert delivery_key("evt_1", "warehouse") != delivery_key("evt_1", "helpdesk")
    assert delivery_key("evt_1", "warehouse") != delivery_key("evt_2", "warehouse")


@pytest.mark.parametrize(
    "etype,action", [("order.created", "prepare"), ("order.paid", "release"), ("order.cancelled", "cancel")]
)
def test_warehouse_mapping(etype, action):
    out = to_warehouse(make_event(1, etype))
    assert out["action"] == action
    assert out["external_ref"] == delivery_key("evt_1", "warehouse")
    assert out["ship_to"] == {
        "name": "A Buyer",
        "line1": "1 Park St",
        "city": "Hyderabad",
        "postal_code": "500081",
        "country": "IN",
    }
    assert out["items"] == [{"sku": "LF-BEAN-250", "quantity": 2}]


@pytest.mark.parametrize(
    "etype,doc", [("order.created", "invoice"), ("order.paid", "payment"), ("order.cancelled", "credit_note")]
)
def test_accounting_mapping(etype, doc):
    out = to_accounting(make_event(1, etype))
    assert out["document_type"] == doc
    assert out["reference"] == delivery_key("evt_1", "accounting")
    assert out["total"] == "25.00"
    assert out["lines"][0] == {
        "sku": "LF-BEAN-250",
        "description": "House blend",
        "quantity": 2,
        "unit_price": "12.50",
    }
    assert out["customer"] == {"email": "a.buyer@example.test", "name": "A Buyer"}


def test_helpdesk_mapping():
    out = to_helpdesk(make_event(1, "order.paid"))
    assert out["reference"] == delivery_key("evt_1", "helpdesk")
    assert out["note"] == "Order LF-1 paid"
    assert out["customer"]["email"] == "a.buyer@example.test"
    assert out["order_total"] == "25.00"


def test_mappers_do_not_judge_business_validity():
    """Negative price and a bad currency pass through: the destination decides (permanent 422)."""
    ev = make_event(
        1, currency="US$", lines=[{"sku": "S", "name": "n", "quantity": 1, "unit_price": "-5.00"}]
    )
    out = to_accounting(ev)
    assert out["currency"] == "US$"
    assert out["lines"][0]["unit_price"] == "-5.00"


def test_money_is_normalised_to_plain_decimal_strings():
    ev = make_event(1, total=25, lines=[{"sku": "S", "name": "n", "quantity": 1, "unit_price": 1e1}])
    out = to_accounting(ev)
    assert out["total"] == "25" and out["lines"][0]["unit_price"] == "10.0"


@pytest.mark.parametrize("mapper", [to_warehouse, to_helpdesk, to_accounting])
def test_missing_customer_is_a_mapping_error(mapper):
    ev = make_event(1)
    del ev["data"]["order"]["customer"]
    with pytest.raises(MappingError, match="customer"):
        mapper(ev)


def test_empty_lines_is_a_mapping_error():
    with pytest.raises(MappingError, match="lines"):
        to_warehouse(make_event(1, lines=[]))


def test_non_numeric_money_is_a_mapping_error():
    with pytest.raises(MappingError, match="not a number"):
        to_accounting(make_event(1, total="abc"))


def test_missing_shipping_address_only_breaks_the_warehouse():
    ev = make_event(1)
    del ev["data"]["order"]["shipping_address"]
    with pytest.raises(MappingError):
        to_warehouse(ev)
    assert to_accounting(ev)["order_number"] == "LF-1"


def test_unsupported_event_type():
    with pytest.raises(MappingError, match="unsupported"):
        to_warehouse(make_event(1, "order.refunded"))


def test_mapping_is_pure():
    ev = make_event(1)
    before = repr(ev)
    to_warehouse(ev), to_helpdesk(ev), to_accounting(ev)
    assert repr(ev) == before
