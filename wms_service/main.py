from __future__ import annotations

import logging
import uuid
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastapi import Body, FastAPI, HTTPException

from app.avro import filter_event_for_schema, load_schema_text, register_warehouse_schemas
from app.http_metrics import install_http_metrics
from app.settings import settings
from app.time_utils import utc_now_iso


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger(__name__)

EVENT_EXAMPLES = {
    "product_received_v2": {
        "summary": "PRODUCT_RECEIVED V2",
        "description": "Приемка товара с supplier_id для schema evolution.",
        "value": {
            "event_id": "swagger-received-v2",
            "event_type": "PRODUCT_RECEIVED",
            "schema_version": 2,
            "event_timestamp": "2026-04-01T12:00:00Z",
            "product_id": "SKU-SWAGGER",
            "zone_id": "ZONE-A",
            "quantity": 100,
            "supplier_id": "SUP-001",
        },
    },
    "product_received_v1": {
        "summary": "PRODUCT_RECEIVED V1",
        "description": "Событие без supplier_id.",
        "value": {
            "event_id": "swagger-received-v1",
            "event_type": "PRODUCT_RECEIVED",
            "schema_version": 1,
            "event_timestamp": "2026-04-01T12:01:00Z",
            "product_id": "SKU-SWAGGER-V1",
            "zone_id": "ZONE-A",
            "quantity": 10,
        },
    },
    "product_reserved": {
        "summary": "PRODUCT_RESERVED",
        "description": "Резервирование товара в зоне.",
        "value": {
            "event_id": "swagger-reserved",
            "event_type": "PRODUCT_RESERVED",
            "event_timestamp": "2026-04-01T12:02:00Z",
            "product_id": "SKU-SWAGGER",
            "zone_id": "ZONE-A",
            "quantity": 30,
        },
    },
    "product_moved": {
        "summary": "PRODUCT_MOVED",
        "description": "Перемещение товара между зонами.",
        "value": {
            "event_id": "swagger-moved",
            "event_type": "PRODUCT_MOVED",
            "event_timestamp": "2026-04-01T12:03:00Z",
            "product_id": "SKU-SWAGGER",
            "from_zone_id": "ZONE-A",
            "to_zone_id": "ZONE-B",
            "quantity": 20,
        },
    },
    "order_created": {
        "summary": "ORDER_CREATED",
        "description": "Создание заказа с резервированием позиции.",
        "value": {
            "event_id": "swagger-order-created",
            "event_type": "ORDER_CREATED",
            "event_timestamp": "2026-04-01T12:04:00Z",
            "order_id": "ORDER-SWAGGER",
            "items": [{"product_id": "SKU-SWAGGER", "quantity": 15, "zone_id": "ZONE-A"}],
        },
    },
    "order_completed": {
        "summary": "ORDER_COMPLETED",
        "description": "Завершение заказа и уменьшение reserved.",
        "value": {
            "event_id": "swagger-order-completed",
            "event_type": "ORDER_COMPLETED",
            "event_timestamp": "2026-04-01T12:05:00Z",
            "order_id": "ORDER-SWAGGER",
        },
    },
    "invalid_dlq": {
        "summary": "Invalid event -> DLQ",
        "description": "Невалидная отгрузка с отрицательным quantity (для демонстрации DLQ типо).",
        "value": {
            "event_id": "swagger-dlq",
            "event_type": "PRODUCT_SHIPPED",
            "event_timestamp": "2026-04-01T12:06:00Z",
            "product_id": "SKU-SWAGGER",
            "zone_id": "ZONE-A",
            "quantity": -5,
        },
    },
}

BULK_EXAMPLES = {
    "out_of_order": {
        "summary": "Out-of-order demo",
        "description": "Третье событие старее второго и будет записано как STALE, по идее.",
        "value": [
            {
                "event_id": "swagger-order-1",
                "event_type": "PRODUCT_RECEIVED",
                "event_timestamp": "2026-04-01T13:00:00Z",
                "product_id": "SKU-OOO",
                "zone_id": "ZONE-A",
                "quantity": 100,
            },
            {
                "event_id": "swagger-order-2",
                "event_type": "PRODUCT_SHIPPED",
                "event_timestamp": "2026-04-01T13:05:00Z",
                "product_id": "SKU-OOO",
                "zone_id": "ZONE-A",
                "quantity": 20,
            },
            {
                "event_id": "swagger-order-3",
                "event_type": "PRODUCT_RECEIVED",
                "event_timestamp": "2026-04-01T13:02:00Z",
                "product_id": "SKU-OOO",
                "zone_id": "ZONE-A",
                "quantity": 50,
            },
        ],
    },
    "lag_demo": {
        "summary": "Consumer lag demo",
        "description": "Отправить после docker stop warehouse-consumer, потом смотреть lag на /metrics и Grafana.",
        "value": [
            {
                "event_id": "swagger-lag-1",
                "event_type": "PRODUCT_RECEIVED",
                "event_timestamp": "2026-04-01T14:00:00Z",
                "product_id": "SKU-LAG-SWAGGER",
                "zone_id": "ZONE-A",
                "quantity": 1,
            },
            {
                "event_id": "swagger-lag-2",
                "event_type": "PRODUCT_RECEIVED",
                "event_timestamp": "2026-04-01T14:01:00Z",
                "product_id": "SKU-LAG-SWAGGER",
                "zone_id": "ZONE-A",
                "quantity": 1,
            },
            {
                "event_id": "swagger-lag-3",
                "event_type": "PRODUCT_RECEIVED",
                "event_timestamp": "2026-04-01T14:02:00Z",
                "product_id": "SKU-LAG-SWAGGER",
                "zone_id": "ZONE-A",
                "quantity": 1,
            },
        ],
    },
}

NULLABLE_FIELDS = (
    "sequence_number",
    "product_id",
    "quantity",
    "counted_quantity",
    "zone_id",
    "from_zone_id",
    "to_zone_id",
    "order_id",
    "items",
    "supplier_id",
)


class EventPublisher:
    def __init__(self) -> None:
        subject = f"{settings.warehouse_events_topic}-value"
        register_warehouse_schemas(settings.schema_registry_url, subject)
        schema_registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        self.serializers = {
            1: AvroSerializer(
                schema_registry,
                load_schema_text(1),
                lambda value, ctx: value,
                conf={"auto.register.schemas": False},
            ),
            2: AvroSerializer(
                schema_registry,
                load_schema_text(2),
                lambda value, ctx: value,
                conf={"auto.register.schemas": False},
            ),
        }
        self.producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})

    def publish(self, event: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_event(event)
        version = normalized["schema_version"]
        serializer = self.serializers[version]
        payload = filter_event_for_schema(normalized, version)
        encoded = serializer(payload, SerializationContext(settings.warehouse_events_topic, MessageField.VALUE))
        key = normalized.get("product_id") or normalized.get("order_id") or normalized["event_id"]

        delivery: dict[str, Any] = {}

        def on_delivery(error, message) -> None:
            if error is not None:
                delivery["error"] = str(error)
                return
            delivery.update(
                {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                }
            )

        self.producer.produce(
            settings.warehouse_events_topic,
            key=str(key),
            value=encoded,
            on_delivery=on_delivery,
        )
        remaining = self.producer.flush(10)
        if remaining:
            raise TimeoutError(f"Kafka delivery timed out, {remaining} message(s) were not delivered")
        if "error" in delivery:
            raise RuntimeError(delivery["error"])
        LOGGER.info(
            "Published event_id=%s event_type=%s schema_version=%s partition=%s offset=%s",
            normalized["event_id"],
            normalized["event_type"],
            version,
            delivery.get("partition"),
            delivery.get("offset"),
        )
        return {"event": normalized, "kafka_metadata": delivery}

    @staticmethod
    def _normalize_event(event: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(event)
        if not normalized.get("event_type"):
            raise ValueError("event_type is required")
        normalized.setdefault("event_id", str(uuid.uuid4()))
        normalized.setdefault("event_timestamp", utc_now_iso())
        version = int(normalized.get("schema_version") or 2)
        if version not in (1, 2):
            raise ValueError("schema_version must be 1 or 2")
        normalized["schema_version"] = version
        for field in NULLABLE_FIELDS:
            if field == "supplier_id" and version == 1:
                continue
            normalized.setdefault(field, None)
        return normalized


app = FastAPI(title="Smart Warehouse WMS Producer")
install_http_metrics(app)


@app.on_event("startup")
def startup() -> None:
    app.state.publisher = EventPublisher()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events")
def publish_event(event: dict[str, Any] = Body(..., openapi_examples=EVENT_EXAMPLES)) -> dict[str, Any]:
    try:
        return app.state.publisher.publish(event)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/events/bulk")
def publish_events(events: list[dict[str, Any]] = Body(..., openapi_examples=BULK_EXAMPLES)) -> dict[str, Any]:
    results = []
    for event in events:
        try:
            results.append(app.state.publisher.publish(event))
        except Exception as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"published": len(results), "results": results}
