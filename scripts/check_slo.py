"""Evaluate the SLOs declared in `monitoring/slo.yml` against a live Prometheus.

Designed to be run from CI after the load test (so Prometheus has metrics to
work with). Exits 0 when ALL SLOs are satisfied, and 1 otherwise, printing a
human-readable report to stdout.

Usage:
    python scripts/check_slo.py [--prom http://localhost:9090] [--slo monitoring/slo.yml]
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import yaml


@dataclass
class SloResult:
    name: str
    objective: float
    direction: str
    measured: float
    satisfied: bool
    description: str


def load_slos(slo_file: Path) -> list[dict[str, Any]]:
    raw = yaml.safe_load(slo_file.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or "slos" not in raw:
        raise ValueError(f"{slo_file} must have a top-level `slos:` key")
    return raw["slos"]


def query_prometheus(prom_url: str, expression: str, timeout: float = 10.0) -> float:
    response = requests.get(
        f"{prom_url.rstrip('/')}/api/v1/query",
        params={"query": expression.strip()},
        timeout=timeout,
    )
    response.raise_for_status()
    body = response.json()
    if body["status"] != "success":
        raise RuntimeError(f"Prometheus query failed: {body}")
    result = body["data"]["result"]
    if not result:
        return 0.0
    return float(result[0]["value"][1])


def evaluate(slo: dict[str, Any], prom_url: str) -> SloResult:
    name = slo["name"]
    objective = float(slo["objective"])
    direction = slo.get("direction", "greater_or_equal").lower()
    measured = query_prometheus(prom_url, slo["sli_query"])

    if direction in ("greater_or_equal", "gte", ">="):
        satisfied = measured >= objective
    elif direction in ("less_or_equal", "lte", "<="):
        satisfied = measured <= objective
    else:
        raise ValueError(f"Unknown direction {direction!r} for SLO {name}")

    return SloResult(
        name=name,
        objective=objective,
        direction=direction,
        measured=measured,
        satisfied=satisfied,
        description=slo.get("description", "").strip(),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prom", default="http://localhost:9090", help="Prometheus base URL")
    parser.add_argument("--slo", default="monitoring/slo.yml", help="Path to SLO definitions")
    parser.add_argument(
        "--wait",
        type=int,
        default=30,
        help="Seconds to wait before evaluation (so metrics have time to be scraped)",
    )
    args = parser.parse_args()

    slo_path = Path(args.slo)
    if not slo_path.exists():
        print(f"SLO file {slo_path} not found", file=sys.stderr)
        return 2

    print(f"Waiting {args.wait}s for Prometheus to settle...")
    time.sleep(args.wait)

    slos = load_slos(slo_path)
    results: list[SloResult] = []
    for slo in slos:
        try:
            results.append(evaluate(slo, args.prom))
        except Exception as exc:
            print(f"[ERROR] SLO {slo.get('name')!r} evaluation failed: {exc}")
            return 1

    print()
    print("=" * 78)
    print(f"{'SLO':<25} {'OBJECTIVE':>15} {'MEASURED':>15} {'STATUS':>10}")
    print("-" * 78)
    failed: list[SloResult] = []
    for r in results:
        status = "OK" if r.satisfied else "BREACHED"
        marker = " " if r.satisfied else "!"
        print(f"{marker} {r.name:<23} {r.objective:>15.6g} {r.measured:>15.6g} {status:>10}")
        if not r.satisfied:
            failed.append(r)
    print("=" * 78)

    if failed:
        print()
        print(f"{len(failed)} SLO(s) BREACHED:")
        for r in failed:
            print(f"  - {r.name}: measured {r.measured} {r.direction} {r.objective}")
            if r.description:
                print(f"      {r.description}")
        return 1

    print("\nAll SLOs satisfied.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
