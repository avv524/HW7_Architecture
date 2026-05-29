from __future__ import annotations

import json
import logging
import time
from functools import lru_cache
from pathlib import Path

import requests


LOGGER = logging.getLogger(__name__)
SCHEMAS_DIR = Path(__file__).resolve().parents[1] / "schemas"


def schema_path(version: int) -> Path:
    if version not in (1, 2):
        raise ValueError(f"Unsupported schema version: {version}")
    return SCHEMAS_DIR / f"warehouse_event_v{version}.avsc"


@lru_cache(maxsize=2)
def load_schema_text(version: int) -> str:
    return schema_path(version).read_text(encoding="utf-8")


@lru_cache(maxsize=2)
def schema_field_names(version: int) -> frozenset[str]:
    schema = json.loads(load_schema_text(version))
    return frozenset(field["name"] for field in schema["fields"])


def filter_event_for_schema(event: dict, version: int) -> dict:
    allowed = schema_field_names(version)
    return {key: value for key, value in event.items() if key in allowed}


def register_warehouse_schemas(schema_registry_url: str, subject: str, retries: int = 60) -> None:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            config_url = f"{schema_registry_url.rstrip('/')}/config/{subject}"
            response = requests.put(config_url, json={"compatibility": "BACKWARD"}, timeout=5)
            response.raise_for_status()

            versions_url = f"{schema_registry_url.rstrip('/')}/subjects/{subject}/versions"
            for version in (1, 2):
                schema_text = load_schema_text(version)
                response = requests.post(
                    versions_url,
                    json={"schemaType": "AVRO", "schema": schema_text},
                    timeout=5,
                )
                response.raise_for_status()
                LOGGER.info("Registered Avro schema v%s for subject %s: %s", version, subject, response.text)
            return
        except Exception as exc:
            last_error = exc
            LOGGER.warning("Schema Registry is not ready yet (%s/%s): %s", attempt, retries, exc)
            time.sleep(2)
    raise RuntimeError(f"Could not register Avro schemas in Schema Registry: {last_error}")
