"""Pure-logic unit tests for WarehouseStateProcessor validation helpers.

These tests cover the input-validation static methods of the consumer's state
processor without ever touching Cassandra or Kafka.
"""
from __future__ import annotations

import pytest

from consumer_service.state_processor import (
    InventoryRow,
    ProcessingError,
    WarehouseStateProcessor,
    consistency_level,
)


class TestRequireText:
    def test_returns_stripped_value(self) -> None:
        assert WarehouseStateProcessor._require_text({"x": "  hello  "}, "x") == "hello"

    @pytest.mark.parametrize("bad", [None, "", "   ", 123, [], {}])
    def test_invalid_raises(self, bad: object) -> None:
        with pytest.raises(ProcessingError) as exc_info:
            WarehouseStateProcessor._require_text({"x": bad}, "x")
        assert exc_info.value.error_code == "VALIDATION_ERROR"

    def test_missing_key_raises(self) -> None:
        with pytest.raises(ProcessingError):
            WarehouseStateProcessor._require_text({}, "missing")


class TestRequireQuantity:
    def test_positive_int_ok(self) -> None:
        assert WarehouseStateProcessor._require_quantity({"quantity": 5}) == 5

    def test_zero_rejected_by_default(self) -> None:
        with pytest.raises(ProcessingError, match="must be positive"):
            WarehouseStateProcessor._require_quantity({"quantity": 0})

    def test_zero_allowed_when_flag_set(self) -> None:
        assert WarehouseStateProcessor._require_quantity({"quantity": 0}, allow_zero=True) == 0

    def test_negative_rejected(self) -> None:
        with pytest.raises(ProcessingError, match="positive"):
            WarehouseStateProcessor._require_quantity({"quantity": -1})

    @pytest.mark.parametrize("bad", [None, "5", 5.0, [], {}])
    def test_non_int_rejected(self, bad: object) -> None:
        with pytest.raises(ProcessingError, match="integer"):
            WarehouseStateProcessor._require_quantity({"quantity": bad})


class TestRequireCountedQuantity:
    def test_uses_counted_quantity_first(self) -> None:
        assert WarehouseStateProcessor._require_counted_quantity({"counted_quantity": 10}) == 10

    def test_falls_back_to_quantity(self) -> None:
        assert WarehouseStateProcessor._require_counted_quantity({"quantity": 7}) == 7

    def test_zero_allowed(self) -> None:
        assert WarehouseStateProcessor._require_counted_quantity({"counted_quantity": 0}) == 0

    def test_negative_rejected(self) -> None:
        with pytest.raises(ProcessingError):
            WarehouseStateProcessor._require_counted_quantity({"counted_quantity": -5})


class TestEventProductIds:
    def test_simple_event(self) -> None:
        ids = WarehouseStateProcessor._event_product_ids(
            {"product_id": "SKU-1", "event_type": "PRODUCT_RECEIVED"}
        )
        assert ids == {"SKU-1"}

    def test_order_with_items(self) -> None:
        ids = WarehouseStateProcessor._event_product_ids(
            {
                "order_id": "O-1",
                "items": [
                    {"product_id": "SKU-A", "quantity": 1},
                    {"product_id": "SKU-B", "quantity": 2},
                ],
            }
        )
        assert ids == {"SKU-A", "SKU-B"}

    def test_order_without_items_uses_order_marker(self) -> None:
        ids = WarehouseStateProcessor._event_product_ids({"order_id": "O-1"})
        assert ids == {"ORDER:O-1"}

    def test_empty_event_returns_empty(self) -> None:
        assert WarehouseStateProcessor._event_product_ids({}) == set()


class TestLatestSupplierForProduct:
    def test_returns_supplier_from_rows(self) -> None:
        rows = {
            ("SKU-1", "ZONE-A"): InventoryRow(available=10, reserved=0, supplier_id="SUP-NEW"),
        }
        result = WarehouseStateProcessor._latest_supplier_for_product(rows, "SKU-1", fallback="SUP-OLD")
        assert result == "SUP-NEW"

    def test_falls_back_when_no_match(self) -> None:
        rows = {
            ("SKU-OTHER", "ZONE-A"): InventoryRow(available=1, reserved=0, supplier_id="SUP-X"),
        }
        result = WarehouseStateProcessor._latest_supplier_for_product(rows, "SKU-1", fallback="SUP-FALLBACK")
        assert result == "SUP-FALLBACK"

    def test_fallback_can_be_none(self) -> None:
        result = WarehouseStateProcessor._latest_supplier_for_product({}, "SKU-1", fallback=None)
        assert result is None


class TestConsistencyLevel:
    @pytest.mark.parametrize("name", ["ONE", "QUORUM", "LOCAL_ONE", "LOCAL_QUORUM", "ALL"])
    def test_known_levels_are_resolved(self, name: str) -> None:
        level = consistency_level(name)
        assert isinstance(level, int)

    def test_lowercase_is_normalized(self) -> None:
        assert consistency_level("quorum") == consistency_level("QUORUM")

    def test_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported"):
            consistency_level("DOES_NOT_EXIST")
