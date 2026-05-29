from __future__ import annotations

import json
import logging
import signal
import threading
import traceback
from typing import Any

import uvicorn
from confluent_kafka import KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from fastapi import FastAPI
from prometheus_client import Counter, Histogram
from starlette.responses import JSONResponse

from app.http_metrics import install_http_metrics
from app.settings import settings
from app.time_utils import utc_now_iso
from consumer_service.state_processor import KafkaMetadata, ProcessingError, StateStoreError, WarehouseStateProcessor


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger(__name__)

EVENTS_PROCESSED = Counter("events_processed_total", "Successfully processed warehouse events", ["event_type"])
PROCESSING_DURATION = Histogram("event_processing_duration_seconds", "Warehouse event processing duration")
CASSANDRA_WRITE_ERRORS = Counter("cassandra_write_errors_total", "Cassandra write errors")

app = FastAPI(title="Smart Warehouse Consumer")
install_http_metrics(app)


class DlqProducer:
    def __init__(self) -> None:
        self.producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})

    def send(self, original_event: Any, exc: Exception, metadata: KafkaMetadata) -> None:
        error_code = getattr(exc, "error_code", "PROCESSING_ERROR")
        payload = {
            "original_event": original_event,
            "error_reason": str(exc),
            "error_code": error_code,
            "failed_at": utc_now_iso(),
            "kafka_metadata": {
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset,
            },
            "stacktrace": traceback.format_exc(),
        }
        key = None
        if isinstance(original_event, dict):
            key = original_event.get("event_id") or original_event.get("product_id") or original_event.get("order_id")
        delivery: dict[str, str] = {}

        def on_delivery(error, _message) -> None:
            if error is not None:
                delivery["error"] = str(error)

        self.producer.produce(
            settings.warehouse_dlq_topic,
            key=str(key) if key else None,
            value=json.dumps(payload, ensure_ascii=True).encode("utf-8"),
            on_delivery=on_delivery,
        )
        remaining = self.producer.flush(10)
        if remaining:
            raise TimeoutError(f"DLQ delivery timed out, {remaining} message(s) were not delivered")
        if "error" in delivery:
            raise RuntimeError(delivery["error"])
        LOGGER.error(
            "Sent event to DLQ event_id=%s partition=%s offset=%s error=%s",
            key,
            metadata.partition,
            metadata.offset,
            exc,
        )


class ConsumerRunner:
    def __init__(self) -> None:
        schema_registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        self.consumer = DeserializingConsumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "key.deserializer": StringDeserializer("utf_8"),
                "value.deserializer": AvroDeserializer(schema_registry),
            }
        )
        self.admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        self.processor = WarehouseStateProcessor(settings, CASSANDRA_WRITE_ERRORS)
        self.dlq = DlqProducer()
        self.stopping = threading.Event()

    def run(self) -> None:
        self.consumer.subscribe([settings.warehouse_events_topic])
        LOGGER.info(
            "Consumer started topic=%s group=%s",
            settings.warehouse_events_topic,
            settings.kafka_consumer_group,
        )
        while not self.stopping.is_set():
            try:
                message = self.consumer.poll(1.0)
            except Exception as exc:
                raw_message = getattr(exc, "kafka_message", None)
                if raw_message is None:
                    LOGGER.exception("Kafka poll failed")
                    continue
                metadata = KafkaMetadata(topic=raw_message.topic(), partition=raw_message.partition(), offset=raw_message.offset())
                raw_value = raw_message.value()
                original_event = {
                    "raw_key": raw_message.key().decode("utf-8", errors="replace") if raw_message.key() else None,
                    "raw_value_hex": raw_value.hex() if isinstance(raw_value, bytes) else None,
                }
                self._handle_dlq(original_event, exc, metadata, raw_message)
                continue
            if message is None:
                continue
            if message.error():
                LOGGER.error("Kafka consumer error: %s", message.error())
                continue

            metadata = KafkaMetadata(topic=message.topic(), partition=message.partition(), offset=message.offset())
            event = message.value()
            try:
                with PROCESSING_DURATION.time():
                    result = self.processor.process(event, metadata)
                if result.status == "PROCESSED":
                    EVENTS_PROCESSED.labels(event_type=result.event_type).inc()
                if self._safe_commit(message):
                    LOGGER.info(
                        "Committed offset after event_id=%s event_type=%s status=%s partition=%s offset=%s",
                        result.event_id,
                        result.event_type,
                        result.status,
                        metadata.partition,
                        metadata.offset,
                    )
            except ProcessingError as exc:
                self._handle_dlq(event, exc, metadata, message)
            except StateStoreError:
                LOGGER.exception(
                    "State store failed; offset is not committed and will be retried partition=%s offset=%s",
                    metadata.partition,
                    metadata.offset,
                )
                try:
                    self.processor.session.shutdown()
                    self.processor.session = self.processor._connect()
                except Exception:
                    LOGGER.exception("Could not reconnect Cassandra session after state store error")
                self.consumer.seek(TopicPartition(metadata.topic, metadata.partition, metadata.offset))
                self.stopping.wait(2)
            except Exception as exc:
                self._handle_dlq(event, exc, metadata, message)

        self.consumer.close()

    def stop(self) -> None:
        self.stopping.set()

    def _safe_commit(self, message) -> bool:
        try:
            self.consumer.commit(message=message, asynchronous=False)
            return True
        except KafkaException:
            LOGGER.exception(
                "Offset commit failed; event may be redelivered partition=%s offset=%s",
                message.partition(),
                message.offset(),
            )
            return False

    def _handle_dlq(self, event: Any, exc: Exception, metadata: KafkaMetadata, message) -> None:
        try:
            self.dlq.send(event, exc, metadata)
        except Exception:
            LOGGER.exception(
                "DLQ publish failed; offset is not committed and will be retried partition=%s offset=%s",
                metadata.partition,
                metadata.offset,
            )
            self.consumer.seek(TopicPartition(metadata.topic, metadata.partition, metadata.offset))
            self.stopping.wait(2)
            return
        self._safe_commit(message)

    def kafka_health_check(self) -> bool:
        try:
            self.admin.list_topics(timeout=3)
            return True
        except Exception:
            LOGGER.exception("Kafka health check failed")
            return False


@app.get("/health")
def health() -> JSONResponse:
    runner: ConsumerRunner | None = getattr(app.state, "runner", None)
    kafka_ok = bool(runner and runner.kafka_health_check())
    cassandra_ok = bool(runner and runner.processor.health_check())
    status_code = 200 if kafka_ok and cassandra_ok else 503
    return JSONResponse(
        status_code=status_code,
        content={"status": "ok" if status_code == 200 else "unavailable", "kafka": kafka_ok, "cassandra": cassandra_ok},
    )


def start_http_server(runner: ConsumerRunner) -> uvicorn.Server:
    app.state.runner = runner
    config = uvicorn.Config(app, host="0.0.0.0", port=settings.metrics_port, log_level="info", access_log=False)
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    return server


def main() -> None:
    runner = ConsumerRunner()
    server = start_http_server(runner)

    def handle_signal(_signum, _frame) -> None:
        runner.stop()
        server.should_exit = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    runner.run()


if __name__ == "__main__":
    main()
