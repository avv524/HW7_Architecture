"""Unit tests for app.http_metrics middleware.

Verifies that the three HW7-required metrics (`http_requests_total`,
`http_request_errors_total`, `http_request_duration_seconds`) are emitted with
correct labels for success, 4xx, 5xx and unhandled-exception responses.
"""
from __future__ import annotations

from typing import Iterator

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

from app.http_metrics import (
    HTTP_REQUEST_DURATION_SECONDS,
    HTTP_REQUEST_ERRORS_TOTAL,
    HTTP_REQUESTS_TOTAL,
    install_http_metrics,
)


def _counter_value(metric, **labels) -> float:
    """Read a Counter / Gauge value from the global Prometheus registry."""
    name = metric._name + "_total" if metric._type == "counter" else metric._name
    value = REGISTRY.get_sample_value(name, labels)
    return value or 0.0


def _bucket_count(metric, le: str, **labels) -> float:
    value = REGISTRY.get_sample_value(
        metric._name + "_bucket",
        {**labels, "le": le},
    )
    return value or 0.0


@pytest.fixture()
def client() -> Iterator[TestClient]:
    app = FastAPI()
    install_http_metrics(app)

    @app.get("/ok")
    def ok() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/fail/{code}")
    def fail(code: int) -> dict[str, str]:
        raise HTTPException(status_code=code, detail="boom")

    @app.get("/boom")
    def boom() -> dict[str, str]:
        raise RuntimeError("kaboom")

    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client


class TestSuccessfulRequests:
    def test_increments_requests_total_with_correct_labels(self, client: TestClient) -> None:
        before = _counter_value(HTTP_REQUESTS_TOTAL, method="GET", endpoint="/ok", status="200")
        response = client.get("/ok")
        assert response.status_code == 200
        after = _counter_value(HTTP_REQUESTS_TOTAL, method="GET", endpoint="/ok", status="200")
        assert after == before + 1

    def test_records_duration_histogram(self, client: TestClient) -> None:
        before = _bucket_count(HTTP_REQUEST_DURATION_SECONDS, "+Inf", method="GET", endpoint="/ok")
        client.get("/ok")
        after = _bucket_count(HTTP_REQUEST_DURATION_SECONDS, "+Inf", method="GET", endpoint="/ok")
        assert after == before + 1

    def test_does_not_increment_errors_for_2xx(self, client: TestClient) -> None:
        before = _counter_value(
            HTTP_REQUEST_ERRORS_TOTAL,
            method="GET",
            endpoint="/ok",
            error_type="http_500",
        )
        client.get("/ok")
        after = _counter_value(
            HTTP_REQUEST_ERRORS_TOTAL,
            method="GET",
            endpoint="/ok",
            error_type="http_500",
        )
        assert after == before


class TestClientErrorRequests:
    def test_4xx_does_not_increment_errors_total(self, client: TestClient) -> None:
        before_err = _counter_value(
            HTTP_REQUEST_ERRORS_TOTAL,
            method="GET",
            endpoint="/fail/{code}",
            error_type="http_404",
        )
        before_req = _counter_value(
            HTTP_REQUESTS_TOTAL,
            method="GET",
            endpoint="/fail/{code}",
            status="404",
        )
        response = client.get("/fail/404")
        assert response.status_code == 404
        after_err = _counter_value(
            HTTP_REQUEST_ERRORS_TOTAL,
            method="GET",
            endpoint="/fail/{code}",
            error_type="http_404",
        )
        after_req = _counter_value(
            HTTP_REQUESTS_TOTAL,
            method="GET",
            endpoint="/fail/{code}",
            status="404",
        )
        assert after_err == before_err
        assert after_req == before_req + 1


class TestServerErrorRequests:
    def test_5xx_increments_errors_total(self, client: TestClient) -> None:
        before = _counter_value(
            HTTP_REQUEST_ERRORS_TOTAL,
            method="GET",
            endpoint="/fail/{code}",
            error_type="http_503",
        )
        response = client.get("/fail/503")
        assert response.status_code == 503
        after = _counter_value(
            HTTP_REQUEST_ERRORS_TOTAL,
            method="GET",
            endpoint="/fail/{code}",
            error_type="http_503",
        )
        assert after == before + 1


class TestUnhandledExceptions:
    def test_exception_records_error_type_label(self, client: TestClient) -> None:
        before = _counter_value(
            HTTP_REQUEST_ERRORS_TOTAL,
            method="GET",
            endpoint="/boom",
            error_type="RuntimeError",
        )
        response = client.get("/boom")
        assert response.status_code == 500
        after = _counter_value(
            HTTP_REQUEST_ERRORS_TOTAL,
            method="GET",
            endpoint="/boom",
            error_type="RuntimeError",
        )
        assert after == before + 1

    def test_request_total_still_recorded(self, client: TestClient) -> None:
        before = _counter_value(
            HTTP_REQUESTS_TOTAL,
            method="GET",
            endpoint="/boom",
            status="500",
        )
        client.get("/boom")
        after = _counter_value(
            HTTP_REQUESTS_TOTAL,
            method="GET",
            endpoint="/boom",
            status="500",
        )
        assert after == before + 1


class TestMetricsEndpoint:
    def test_exposes_prometheus_text(self, client: TestClient) -> None:
        client.get("/ok")
        response = client.get("/metrics")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain")
        body = response.text
        assert "http_requests_total" in body
        assert "http_request_errors_total" in body
        assert "http_request_duration_seconds" in body
        assert 'endpoint="/ok"' in body
