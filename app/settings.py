from __future__ import annotations

import os
from dataclasses import dataclass


def _csv(name: str, default: str) -> tuple[str, ...]:
    return tuple(value.strip() for value in os.getenv(name, default).split(",") if value.strip())


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    warehouse_events_topic: str = os.getenv("WAREHOUSE_EVENTS_TOPIC", "warehouse-events")
    warehouse_dlq_topic: str = os.getenv("WAREHOUSE_DLQ_TOPIC", "warehouse-events-dlq")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "warehouse-state-consumer")

    cassandra_hosts: tuple[str, ...] = _csv("CASSANDRA_HOSTS", "localhost")
    cassandra_port: int = int(os.getenv("CASSANDRA_PORT", "9042"))
    cassandra_keyspace: str = os.getenv("CASSANDRA_KEYSPACE", "warehouse")
    cassandra_local_dc: str = os.getenv("CASSANDRA_LOCAL_DC", "datacenter1")
    cassandra_read_consistency: str = os.getenv("CASSANDRA_READ_CONSISTENCY", "QUORUM")
    cassandra_write_consistency: str = os.getenv("CASSANDRA_WRITE_CONSISTENCY", "QUORUM")

    metrics_port: int = int(os.getenv("METRICS_PORT", "8000"))


settings = Settings()
