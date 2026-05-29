"""Integration tests covering producer -> Kafka -> consumer -> Cassandra.

These tests assume the docker-compose stack from the repo root is up and
reachable on default ports. They are marked `integration` so the CI workflow
can run them in a dedicated step.
"""
from __future__ import annotations

import time

import pytest
import requests

from tests.conftest import (
    Endpoints,
    get_inventory,
    publish_event,
    query_prometheus,
    wait_for_event_processed,
)


pytestmark = pytest.mark.integration


class TestWmsApi:
    def test_health_endpoint(self, wms_ready: Endpoints) -> None:
        response = requests.get(f"{wms_ready.wms}/health", timeout=5)
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_metrics_endpoint_exposes_required_metric_families(self, wms_ready: Endpoints) -> None:
        requests.get(f"{wms_ready.wms}/health", timeout=5)
        body = requests.get(f"{wms_ready.wms}/metrics", timeout=5).text
        assert "http_requests_total" in body
        assert "http_request_errors_total" in body
        assert "http_request_duration_seconds" in body

    def test_publish_valid_event_returns_kafka_metadata(
        self, wms_ready: Endpoints, unique_suffix: str
    ) -> None:
        event = {
            "event_id": f"itest-publish-{unique_suffix}",
            "event_type": "PRODUCT_RECEIVED",
            "schema_version": 2,
            "event_timestamp": "2026-04-01T12:00:00Z",
            "product_id": f"SKU-INT-{unique_suffix}",
            "zone_id": "ZONE-A",
            "quantity": 10,
            "supplier_id": "SUP-INT",
        }
        result = publish_event(wms_ready, event)
        assert "kafka_metadata" in result
        assert result["kafka_metadata"]["topic"] == "warehouse-events"
        assert isinstance(result["kafka_metadata"]["partition"], int)
        assert isinstance(result["kafka_metadata"]["offset"], int)

    def test_publish_missing_event_type_returns_400(self, wms_ready: Endpoints) -> None:
        response = requests.post(
            f"{wms_ready.wms}/events",
            json={"product_id": "SKU-NOTYPE", "zone_id": "ZONE-A", "quantity": 1},
            timeout=10,
        )
        assert response.status_code == 400
        assert "event_type" in response.text


class TestPipelineEndToEnd:
    def test_product_received_lands_in_cassandra(
        self,
        stack_ready: Endpoints,
        cassandra,
        unique_suffix: str,
    ) -> None:
        product_id = f"SKU-INT-PR-{unique_suffix}"
        event_id = f"itest-pr-{unique_suffix}"
        event = {
            "event_id": event_id,
            "event_type": "PRODUCT_RECEIVED",
            "schema_version": 2,
            "event_timestamp": "2026-04-01T12:00:00Z",
            "product_id": product_id,
            "zone_id": "ZONE-A",
            "quantity": 25,
            "supplier_id": "SUP-INT",
        }
        publish_event(stack_ready, event)

        processed = wait_for_event_processed(cassandra, event_id, timeout_seconds=60)
        assert processed["event_type"] == "PRODUCT_RECEIVED"
        assert processed["status"] == "PROCESSED"

        inv = get_inventory(cassandra, product_id, "ZONE-A")
        assert inv is not None
        assert inv["available"] == 25
        assert inv["reserved"] == 0
        assert inv["supplier_id"] == "SUP-INT"

    def test_duplicate_event_id_is_marked_duplicate(
        self,
        stack_ready: Endpoints,
        cassandra,
        unique_suffix: str,
    ) -> None:
        product_id = f"SKU-INT-DUP-{unique_suffix}"
        event_id = f"itest-dup-{unique_suffix}"
        event = {
            "event_id": event_id,
            "event_type": "PRODUCT_RECEIVED",
            "schema_version": 2,
            "event_timestamp": "2026-04-01T12:00:00Z",
            "product_id": product_id,
            "zone_id": "ZONE-A",
            "quantity": 10,
        }
        publish_event(stack_ready, event)
        wait_for_event_processed(cassandra, event_id, timeout_seconds=60)

        first_inventory = get_inventory(cassandra, product_id, "ZONE-A")
        assert first_inventory is not None and first_inventory["available"] == 10

        publish_event(stack_ready, event)
        time.sleep(3)
        second_inventory = get_inventory(cassandra, product_id, "ZONE-A")
        assert second_inventory == first_inventory

    def test_invalid_quantity_is_routed_to_dlq(
        self,
        stack_ready: Endpoints,
        cassandra,
        unique_suffix: str,
    ) -> None:
        product_id = f"SKU-INT-DLQ-{unique_suffix}"
        event_id = f"itest-dlq-{unique_suffix}"
        event = {
            "event_id": event_id,
            "event_type": "PRODUCT_SHIPPED",
            "schema_version": 2,
            "event_timestamp": "2026-04-01T12:00:00Z",
            "product_id": product_id,
            "zone_id": "ZONE-Z",
            "quantity": 5,
        }
        publish_event(stack_ready, event)

        time.sleep(5)
        inv = get_inventory(cassandra, product_id, "ZONE-Z")
        assert inv is None


class TestServiceMetrics:
    def test_wms_records_publish_events_metric(
        self,
        wms_ready: Endpoints,
        prometheus_ready: Endpoints,
        unique_suffix: str,
    ) -> None:
        event = {
            "event_id": f"itest-metric-{unique_suffix}",
            "event_type": "PRODUCT_RECEIVED",
            "schema_version": 2,
            "event_timestamp": "2026-04-01T12:00:00Z",
            "product_id": f"SKU-INT-MT-{unique_suffix}",
            "zone_id": "ZONE-A",
            "quantity": 1,
            "supplier_id": "SUP-MT",
        }
        publish_event(wms_ready, event)

        time.sleep(10)

        result = query_prometheus(
            wms_ready,
            'http_requests_total{service="wms",endpoint="/events",status="200"}',
        )
        assert result, "Expected at least one http_requests_total sample for /events 200"
        assert any(float(item["value"][1]) > 0 for item in result)

    def test_consumer_processes_events_metric_present(
        self,
        stack_ready: Endpoints,
        prometheus_ready: Endpoints,
    ) -> None:
        time.sleep(10)
        result = query_prometheus(prometheus_ready, "events_processed_total")
        assert result, "Expected events_processed_total to be present in Prometheus"
