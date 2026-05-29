"""End-to-end test: full warehouse order lifecycle.

Covers a realistic user journey:
1. PRODUCT_RECEIVED  (stock arrives)
2. PRODUCT_MOVED     (stock moved to another zone)
3. ORDER_CREATED     (customer places an order; stock gets reserved)
4. ORDER_COMPLETED   (order shipped; reserved stock released)

After each step we verify the resulting Cassandra state via direct CQL queries
and the observability layer (Prometheus + Alertmanager) is healthy.
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


pytestmark = pytest.mark.e2e


def _wait_order_status(cassandra, order_id: str, expected: str, timeout_seconds: float = 60) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_status: str | None = None
    while time.monotonic() < deadline:
        row = cassandra.execute(
            "SELECT status FROM orders_by_id WHERE order_id = %s", (order_id,)
        ).one()
        if row:
            last_status = row.status
            if row.status == expected:
                return
        time.sleep(0.5)
    raise AssertionError(f"Order {order_id} did not reach status {expected} (last={last_status})")


class TestOrderLifecycle:
    """Single long test that walks through the full warehouse user journey."""

    def test_full_order_lifecycle(
        self,
        stack_ready: Endpoints,
        cassandra,
        unique_suffix: str,
    ) -> None:
        product_id = f"SKU-E2E-{unique_suffix}"
        order_id = f"ORDER-E2E-{unique_suffix}"

        recv_event_id = f"e2e-recv-{unique_suffix}"
        publish_event(
            stack_ready,
            {
                "event_id": recv_event_id,
                "event_type": "PRODUCT_RECEIVED",
                "schema_version": 2,
                "event_timestamp": "2026-04-01T10:00:00Z",
                "product_id": product_id,
                "zone_id": "ZONE-A",
                "quantity": 100,
                "supplier_id": "SUP-E2E",
            },
        )
        processed = wait_for_event_processed(cassandra, recv_event_id, timeout_seconds=60)
        assert processed["status"] == "PROCESSED"
        inv_a = get_inventory(cassandra, product_id, "ZONE-A")
        assert inv_a == {"available": 100, "reserved": 0, "supplier_id": "SUP-E2E"}

        move_event_id = f"e2e-move-{unique_suffix}"
        publish_event(
            stack_ready,
            {
                "event_id": move_event_id,
                "event_type": "PRODUCT_MOVED",
                "schema_version": 2,
                "event_timestamp": "2026-04-01T11:00:00Z",
                "product_id": product_id,
                "from_zone_id": "ZONE-A",
                "to_zone_id": "ZONE-B",
                "quantity": 30,
            },
        )
        wait_for_event_processed(cassandra, move_event_id, timeout_seconds=60)
        inv_a = get_inventory(cassandra, product_id, "ZONE-A")
        inv_b = get_inventory(cassandra, product_id, "ZONE-B")
        assert inv_a["available"] == 70
        assert inv_b["available"] == 30

        create_event_id = f"e2e-create-{unique_suffix}"
        publish_event(
            stack_ready,
            {
                "event_id": create_event_id,
                "event_type": "ORDER_CREATED",
                "schema_version": 2,
                "event_timestamp": "2026-04-01T12:00:00Z",
                "order_id": order_id,
                "items": [
                    {"product_id": product_id, "quantity": 20, "zone_id": "ZONE-B"},
                ],
            },
        )
        wait_for_event_processed(cassandra, create_event_id, timeout_seconds=60)
        _wait_order_status(cassandra, order_id, "CREATED")
        inv_b = get_inventory(cassandra, product_id, "ZONE-B")
        assert inv_b == {"available": 10, "reserved": 20, "supplier_id": "SUP-E2E"}

        complete_event_id = f"e2e-complete-{unique_suffix}"
        publish_event(
            stack_ready,
            {
                "event_id": complete_event_id,
                "event_type": "ORDER_COMPLETED",
                "schema_version": 2,
                "event_timestamp": "2026-04-01T13:00:00Z",
                "order_id": order_id,
            },
        )
        wait_for_event_processed(cassandra, complete_event_id, timeout_seconds=60)
        _wait_order_status(cassandra, order_id, "COMPLETED")

        inv_b = get_inventory(cassandra, product_id, "ZONE-B")
        assert inv_b["reserved"] == 0
        assert inv_b["available"] == 10


class TestObservability:
    def test_prometheus_targets_are_up(self, prometheus_ready: Endpoints) -> None:
        result = requests.get(f"{prometheus_ready.prometheus}/api/v1/targets", timeout=10).json()
        targets = result["data"]["activeTargets"]
        assert targets, "Expected at least one active Prometheus target"
        for target in targets:
            assert target["health"] in ("up", "unknown"), f"Target {target['scrapeUrl']} is down: {target}"
        up_jobs = {t["labels"]["job"] for t in targets if t["health"] == "up"}
        assert "warehouse-wms" in up_jobs
        assert "warehouse-consumer" in up_jobs

    def test_prometheus_alerts_loaded(self, prometheus_ready: Endpoints) -> None:
        response = requests.get(f"{prometheus_ready.prometheus}/api/v1/rules", timeout=10)
        response.raise_for_status()
        body = response.json()
        groups = body["data"]["groups"]
        rule_names = {
            rule["name"]
            for group in groups
            for rule in group["rules"]
            if rule.get("type") == "alerting"
        }
        assert {
            "TargetDown",
            "HighHttpErrorRate",
            "HighRequestLatencyP95",
            "KafkaConsumerLagHigh",
            "CassandraWriteErrors",
            "NoEventsProcessed",
        }.issubset(rule_names), f"Missing alert rules. Loaded: {rule_names}"

    def test_kafka_exporter_exposes_topic_metrics(self, stack_ready: Endpoints) -> None:
        body = requests.get(f"{stack_ready.kafka_exporter}/metrics", timeout=10).text
        assert "kafka_brokers" in body
        assert "kafka_topic_partitions" in body
        assert 'kafka_topic_partitions{topic="warehouse-events"}' in body

    def test_alertmanager_is_reachable(self, stack_ready: Endpoints) -> None:
        response = requests.get(f"{stack_ready.alertmanager}/api/v2/status", timeout=10)
        assert response.status_code == 200
        assert response.json()["cluster"]["status"] in ("ready", "settling")

    def test_service_metrics_present_in_prometheus(self, prometheus_ready: Endpoints) -> None:
        for metric in (
            "http_requests_total",
            "http_request_errors_total",
            "http_request_duration_seconds_bucket",
        ):
            result = query_prometheus(prometheus_ready, metric)
            assert result, f"No samples found for {metric}"
