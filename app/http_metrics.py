"""Shared Prometheus HTTP metrics + FastAPI middleware.

Exports the three metrics required by HW7:
- http_requests_total{method, endpoint, status}                       (Counter)
- http_request_errors_total{method, endpoint, error_type}             (Counter)
- http_request_duration_seconds{method, endpoint}                     (Histogram)

The middleware records latency for every request and labels metrics by the
matched route template (so high-cardinality path params don't explode labels).
"""

from __future__ import annotations

import time
from typing import Callable

from fastapi import FastAPI, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response as StarletteResponse
from starlette.types import ASGIApp


HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total number of HTTP requests handled by the service",
    ["method", "endpoint", "status"],
)

HTTP_REQUEST_ERRORS_TOTAL = Counter(
    "http_request_errors_total",
    "Total number of HTTP requests that resulted in an error (>=500 or unhandled exception)",
    ["method", "endpoint", "error_type"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)


def _route_template(request: Request, fallback: str) -> str:
    """Return the matched route template if FastAPI has resolved it, else fallback path."""
    route = request.scope.get("route")
    if route is not None and getattr(route, "path", None):
        return route.path
    return fallback


class PrometheusHTTPMiddleware(BaseHTTPMiddleware):
    """Record Prometheus metrics for every HTTP request.

    Errors are tracked twice:
    - http_requests_total{status="5xx"} for any 5xx response
    - http_request_errors_total{error_type=...} for any unhandled exception or 5xx
    """

    async def dispatch(self, request: Request, call_next: Callable) -> StarletteResponse:
        method = request.method
        start = time.perf_counter()
        status_code = 500
        error_type: str | None = None
        endpoint = request.url.path
        try:
            response = await call_next(request)
            status_code = response.status_code
            endpoint = _route_template(request, endpoint)
            return response
        except Exception as exc:
            endpoint = _route_template(request, endpoint)
            error_type = type(exc).__name__
            raise
        finally:
            duration = time.perf_counter() - start
            HTTP_REQUEST_DURATION_SECONDS.labels(method=method, endpoint=endpoint).observe(duration)
            HTTP_REQUESTS_TOTAL.labels(
                method=method,
                endpoint=endpoint,
                status=str(status_code),
            ).inc()
            if error_type is not None:
                HTTP_REQUEST_ERRORS_TOTAL.labels(
                    method=method,
                    endpoint=endpoint,
                    error_type=error_type,
                ).inc()
            elif status_code >= 500:
                HTTP_REQUEST_ERRORS_TOTAL.labels(
                    method=method,
                    endpoint=endpoint,
                    error_type=f"http_{status_code}",
                ).inc()


def install_http_metrics(app: FastAPI) -> None:
    """Attach the HTTP metrics middleware and a /metrics endpoint to a FastAPI app."""
    app.add_middleware(PrometheusHTTPMiddleware)

    HTTP_REQUEST_ERRORS_TOTAL.labels(method="_init", endpoint="_init", error_type="_init").inc(1)

    if not any(getattr(route, "path", None) == "/metrics" for route in app.router.routes):

        @app.get("/metrics", include_in_schema=False)
        def _metrics_endpoint() -> Response:
            return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


__all__ = [
    "HTTP_REQUESTS_TOTAL",
    "HTTP_REQUEST_ERRORS_TOTAL",
    "HTTP_REQUEST_DURATION_SECONDS",
    "PrometheusHTTPMiddleware",
    "install_http_metrics",
    "ASGIApp",
]
