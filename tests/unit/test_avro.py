"""Unit tests for app.avro schema helpers (no Schema Registry I/O)."""
from __future__ import annotations

import json

import pytest

from app.avro import (
    filter_event_for_schema,
    load_schema_text,
    schema_field_names,
    schema_path,
)


class TestSchemaPath:
    @pytest.mark.parametrize("version", [1, 2])
    def test_known_versions_exist(self, version: int) -> None:
        assert schema_path(version).is_file()

    @pytest.mark.parametrize("version", [0, 3, -1, 99])
    def test_unsupported_version_raises(self, version: int) -> None:
        with pytest.raises(ValueError):
            schema_path(version)


class TestLoadSchemaText:
    def test_returns_valid_json(self) -> None:
        text = load_schema_text(2)
        parsed = json.loads(text)
        assert parsed["type"] == "record"
        assert parsed["name"] == "WarehouseEvent"
        assert any(field["name"] == "supplier_id" for field in parsed["fields"])

    def test_v1_does_not_have_supplier_id(self) -> None:
        text = load_schema_text(1)
        parsed = json.loads(text)
        assert all(field["name"] != "supplier_id" for field in parsed["fields"])


class TestSchemaFieldNames:
    def test_v2_includes_supplier_id(self) -> None:
        names = schema_field_names(2)
        assert "supplier_id" in names
        assert "event_id" in names
        assert "event_type" in names

    def test_v1_excludes_supplier_id(self) -> None:
        names = schema_field_names(1)
        assert "supplier_id" not in names
        assert "event_id" in names


class TestFilterEventForSchema:
    def test_drops_unknown_fields(self) -> None:
        event = {
            "event_id": "e1",
            "event_type": "PRODUCT_RECEIVED",
            "product_id": "SKU-1",
            "extra_field": "should be dropped",
            "another_one": 42,
        }
        result = filter_event_for_schema(event, version=2)
        assert "extra_field" not in result
        assert "another_one" not in result
        assert result["event_id"] == "e1"
        assert result["product_id"] == "SKU-1"

    def test_drops_supplier_id_for_v1(self) -> None:
        event = {
            "event_id": "e1",
            "event_type": "PRODUCT_RECEIVED",
            "product_id": "SKU-1",
            "supplier_id": "SUP-1",
        }
        result = filter_event_for_schema(event, version=1)
        assert "supplier_id" not in result

    def test_keeps_supplier_id_for_v2(self) -> None:
        event = {
            "event_id": "e1",
            "event_type": "PRODUCT_RECEIVED",
            "supplier_id": "SUP-1",
        }
        result = filter_event_for_schema(event, version=2)
        assert result["supplier_id"] == "SUP-1"

    def test_empty_event_yields_empty_dict(self) -> None:
        assert filter_event_for_schema({}, version=2) == {}
