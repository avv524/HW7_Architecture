"""Shared pytest fixtures and helpers for integration / E2E tests.

These fixtures assume the docker-compose stack from the repository root is
running. They are NOT used by `tests/unit/*`, which run without any external
dependencies.
"""
from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterator

import pytest
import requests


WMS_URL = os.getenv("WMS_URL", "http://localhost:8080")
CONSUMER_METRICS_URL = os.getenv("CONSUMER_METRICS_URL", "http://localhost:8000")
LAG_EXPORTER_URL = os.getenv("LAG_EXPORTER_URL", "http://localhost:8001")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3000")
ALERTMANAGER_URL = os.getenv("ALERTMANAGER_URL", "http://localhost:9093")
KAFKA_EXPORTER_URL = os.getenv("KAFKA_EXPORTER_URL", "http://localhost:9308")

CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "warehouse")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))


@dataclass
class Endpoints:
    wms: str = WMS_URL
    consumer_metrics: str = CONSUMER_METRICS_URL
    lag_exporter: str = LAG_EXPORTER_URL
    prometheus: str = PROMETHEUS_URL
    grafana: str = GRAFANA_URL
    alertmanager: str = ALERTMANAGER_URL
    kafka_exporter: str = KAFKA_EXPORTER_URL


@pytest.fixture(scope="session")
def endpoints() -> Endpoints:
    return Endpoints()


def _wait_for(check: Callable[[], bool], timeout_seconds: float = 120, interval: float = 2.0, what: str = "service") -> None:
    deadline = time.monotonic() + timeout_seconds
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except Exception as exc:
            last_exc = exc
        time.sleep(interval)
    raise TimeoutError(f"{what} did not become ready within {timeout_seconds}s (last error: {last_exc})")


def _http_health_ok(url: str) -> bool:
    response = requests.get(url, timeout=5)
    return response.status_code == 200


@pytest.fixture(scope="session")
def wms_ready(endpoints: Endpoints) -> Endpoints:
    _wait_for(lambda: _http_health_ok(f"{endpoints.wms}/health"), what="WMS /health")
    return endpoints


@pytest.fixture(scope="session")
def consumer_ready(endpoints: Endpoints) -> Endpoints:
    _wait_for(lambda: _http_health_ok(f"{endpoints.consumer_metrics}/health"), what="Consumer /health")
    return endpoints


@pytest.fixture(scope="session")
def stack_ready(wms_ready: Endpoints, consumer_ready: Endpoints) -> Endpoints:
    return wms_ready


@pytest.fixture(scope="session")
def prometheus_ready(endpoints: Endpoints) -> Endpoints:
    _wait_for(
        lambda: _http_health_ok(f"{endpoints.prometheus}/-/ready"),
        what="Prometheus /-/ready",
    )
    return endpoints


@pytest.fixture()
def unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def publish_event(endpoints: Endpoints, event: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(f"{endpoints.wms}/events", json=event, timeout=10)
    response.raise_for_status()
    return response.json()


def query_prometheus(endpoints: Endpoints, query: str) -> list[dict[str, Any]]:
    response = requests.get(
        f"{endpoints.prometheus}/api/v1/query",
        params={"query": query},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()
    assert data["status"] == "success", data
    return data["data"]["result"]


def cassandra_session():
    """Connect to Cassandra from the test runner.

    Returned at module import time so tests can use it as a session-scoped fixture.

    On Python 3.12+ the cassandra-driver needs an explicit event loop (asyncore was
    removed).  Install gevent and set CASS_DRIVER_NO_CYEXT=1, or run from the
    Docker container (Python 3.11).  CI uses ubuntu-22.04 / Python 3.11.
    """
    import os as _os

    _os.environ.setdefault("CASS_DRIVER_NO_CYEXT", "1")
    try:
        from cassandra.cluster import Cluster
    except Exception as _exc:
        raise RuntimeError(
            "cassandra-driver failed to initialise on this Python version.  "
            "Try:  pip install gevent  &&  set CASS_DRIVER_NO_CYEXT=1.  "
            "Original error: " + str(_exc)
        ) from _exc

    cluster = Cluster(contact_points=CASSANDRA_HOSTS, port=CASSANDRA_PORT, protocol_version=5)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    return cluster, session


@pytest.fixture(scope="session")
def cassandra() -> Iterator[Any]:
    _wait_for(_can_connect_cassandra, what="Cassandra")
    cluster, session = cassandra_session()
    try:
        yield session
    finally:
        cluster.shutdown()


def _can_connect_cassandra() -> bool:
    cluster, session = cassandra_session()
    try:
        session.execute("SELECT release_version FROM system.local").one()
        return True
    finally:
        cluster.shutdown()


def wait_for_event_processed(cassandra_session, event_id: str, timeout_seconds: float = 30) -> dict[str, Any]:
    """Poll Cassandra until the event is recorded in `processed_events`."""
    deadline = time.monotonic() + timeout_seconds
    last_seen: dict[str, Any] = {}
    while time.monotonic() < deadline:
        row = cassandra_session.execute(
            "SELECT event_id, event_type, status, processed_at FROM processed_events WHERE event_id = %s",
            (event_id,),
        ).one()
        if row:
            return {
                "event_id": row.event_id,
                "event_type": row.event_type,
                "status": row.status,
                "processed_at": row.processed_at,
            }
        time.sleep(0.5)
    raise TimeoutError(f"Event {event_id} was not processed within {timeout_seconds}s (last_seen={last_seen})")


def get_inventory(cassandra_session, product_id: str, zone_id: str) -> dict[str, Any] | None:
    row = cassandra_session.execute(
        "SELECT available_quantity, reserved_quantity, supplier_id FROM inventory_by_product_zone "
        "WHERE product_id = %s AND zone_id = %s",
        (product_id, zone_id),
    ).one()
    if not row:
        return None
    return {
        "available": int(row.available_quantity or 0),
        "reserved": int(row.reserved_quantity or 0),
        "supplier_id": row.supplier_id,
    }
