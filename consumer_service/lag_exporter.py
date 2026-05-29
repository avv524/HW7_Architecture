from __future__ import annotations

import logging
import signal
import threading

import uvicorn
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI
from prometheus_client import Gauge

from app.http_metrics import install_http_metrics
from app.settings import settings


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger(__name__)

CONSUMER_LAG = Gauge("consumer_lag", "Consumer lag by Kafka partition", ["partition"])
app = FastAPI(title="Smart Warehouse Lag Exporter")
install_http_metrics(app)


class LagExporter:
    def __init__(self) -> None:
        self.admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_consumer_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self.stopping = threading.Event()

    def run(self) -> None:
        while not self.stopping.is_set():
            self.update_lag()
            self.stopping.wait(5)
        self.consumer.close()

    def stop(self) -> None:
        self.stopping.set()

    def update_lag(self) -> None:
        try:
            metadata = self.admin.list_topics(settings.warehouse_events_topic, timeout=5)
            topic_metadata = metadata.topics.get(settings.warehouse_events_topic)
            if topic_metadata is None or topic_metadata.error is not None:
                LOGGER.warning("Topic metadata is unavailable for %s", settings.warehouse_events_topic)
                return
            partitions = sorted(topic_metadata.partitions.keys())
            topic_partitions = [TopicPartition(settings.warehouse_events_topic, partition) for partition in partitions]
            committed_offsets = self.consumer.committed(topic_partitions, timeout=5)
            for topic_partition in committed_offsets:
                _low, high = self.consumer.get_watermark_offsets(
                    TopicPartition(settings.warehouse_events_topic, topic_partition.partition),
                    timeout=5,
                    cached=False,
                )
                committed_offset = topic_partition.offset if topic_partition.offset >= 0 else 0
                CONSUMER_LAG.labels(partition=str(topic_partition.partition)).set(max(high - committed_offset, 0))
        except Exception:
            LOGGER.exception("Could not export consumer lag")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def main() -> None:
    exporter = LagExporter()
    app.state.exporter = exporter
    thread = threading.Thread(target=exporter.run, daemon=True)
    thread.start()

    def handle_signal(_signum, _frame) -> None:
        exporter.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    uvicorn.run(app, host="0.0.0.0", port=settings.metrics_port, access_log=False)
    exporter.stop()
    thread.join(timeout=5)


if __name__ == "__main__":
    main()
