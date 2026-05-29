"""Unit tests for the producer's event normalization logic (no Kafka I/O)."""
from __future__ import annotations

import pytest

from wms_service.main import EventPublisher


class TestEventPublisherNormalize:
    def test_event_type_is_required(self) -> None:
        with pytest.raises(ValueError, match="event_type"):
            EventPublisher._normalize_event({})

    def test_assigns_event_id_if_missing(self) -> None:
        result = EventPublisher._normalize_event({"event_type": "PRODUCT_RECEIVED"})
        assert result["event_id"]

    def test_keeps_provided_event_id(self) -> None:
        result = EventPublisher._normalize_event(
            {"event_id": "fixed-id", "event_type": "PRODUCT_RECEIVED"}
        )
        assert result["event_id"] == "fixed-id"

    def test_assigns_event_timestamp_if_missing(self) -> None:
        result = EventPublisher._normalize_event({"event_type": "PRODUCT_RECEIVED"})
        assert isinstance(result["event_timestamp"], str)
        assert result["event_timestamp"].endswith("Z")

    def test_default_schema_version_is_2(self) -> None:
        result = EventPublisher._normalize_event({"event_type": "PRODUCT_RECEIVED"})
        assert result["schema_version"] == 2

    @pytest.mark.parametrize("version", [3, -1, 99])
    def test_invalid_schema_version_raises(self, version: int) -> None:
        with pytest.raises(ValueError, match="schema_version"):
            EventPublisher._normalize_event(
                {"event_type": "PRODUCT_RECEIVED", "schema_version": version}
            )

    def test_falsy_schema_version_defaults_to_2(self) -> None:
        result = EventPublisher._normalize_event(
            {"event_type": "PRODUCT_RECEIVED", "schema_version": 0}
        )
        assert result["schema_version"] == 2

    def test_v1_does_not_get_supplier_id(self) -> None:
        result = EventPublisher._normalize_event(
            {"event_type": "PRODUCT_RECEIVED", "schema_version": 1}
        )
        assert "supplier_id" not in result

    def test_v2_gets_nullable_supplier_id_set_to_none(self) -> None:
        result = EventPublisher._normalize_event(
            {"event_type": "PRODUCT_RECEIVED", "schema_version": 2}
        )
        assert result["supplier_id"] is None

    def test_nullable_fields_filled_with_none(self) -> None:
        result = EventPublisher._normalize_event(
            {"event_type": "PRODUCT_RECEIVED"}
        )
        for field in ("product_id", "quantity", "zone_id", "from_zone_id", "to_zone_id"):
            assert field in result
            assert result[field] is None

    def test_keeps_user_provided_values(self) -> None:
        result = EventPublisher._normalize_event(
            {
                "event_type": "PRODUCT_RECEIVED",
                "product_id": "SKU-1",
                "zone_id": "ZONE-A",
                "quantity": 42,
                "supplier_id": "SUP-1",
            }
        )
        assert result["product_id"] == "SKU-1"
        assert result["zone_id"] == "ZONE-A"
        assert result["quantity"] == 42
        assert result["supplier_id"] == "SUP-1"
