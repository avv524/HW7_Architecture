from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterable

from cassandra import ConsistencyLevel


if TYPE_CHECKING:
    from cassandra.cluster import Cluster as _ClusterType

from app.settings import Settings
from app.time_utils import ensure_utc, parse_event_timestamp, utc_now


Cluster = None
ExecutionProfile = None
EXEC_PROFILE_DEFAULT = None
DCAwareRoundRobinPolicy = None
BatchStatement = None
BatchType = None
SimpleStatement = None


def _load_cassandra() -> None:
    """Lazily import cassandra.cluster + related modules on first real use."""
    global Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    global DCAwareRoundRobinPolicy, BatchStatement, BatchType, SimpleStatement
    if Cluster is not None:
        return
    from cassandra.cluster import EXEC_PROFILE_DEFAULT as _epd, Cluster as _C, ExecutionProfile as _EP
    from cassandra.policies import DCAwareRoundRobinPolicy as _DC
    from cassandra.query import BatchStatement as _BS, BatchType as _BT, SimpleStatement as _SS
    Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT = _C, _EP, _epd
    DCAwareRoundRobinPolicy = _DC
    BatchStatement, BatchType, SimpleStatement = _BS, _BT, _SS


LOGGER = logging.getLogger(__name__)


class ProcessingError(Exception):
    def __init__(self, message: str, error_code: str = "PROCESSING_ERROR") -> None:
        super().__init__(message)
        self.error_code = error_code


class StateStoreError(Exception):
    pass


class StateReadError(StateStoreError):
    pass


class StateWriteError(StateStoreError):
    pass


@dataclass(frozen=True)
class KafkaMetadata:
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class ProcessingResult:
    event_id: str
    event_type: str
    status: str
    reason: str | None = None


@dataclass
class InventoryChange:
    product_id: str
    zone_id: str
    available_delta: int = 0
    reserved_delta: int = 0
    set_available: int | None = None
    supplier_id: str | None = None
    overwrite_supplier: bool = False


@dataclass
class InventoryRow:
    available: int
    reserved: int
    supplier_id: str | None


def consistency_level(name: str) -> int:
    normalized = name.upper()
    if not hasattr(ConsistencyLevel, normalized):
        raise ValueError(f"Unsupported Cassandra consistency level: {name}")
    return getattr(ConsistencyLevel, normalized)


class WarehouseStateProcessor:
    def __init__(self, settings: Settings, cassandra_error_counter: Any | None = None) -> None:
        self.settings = settings
        self.read_consistency = consistency_level(settings.cassandra_read_consistency)
        self.write_consistency = consistency_level(settings.cassandra_write_consistency)
        self.cassandra_error_counter = cassandra_error_counter
        self.cluster: "_ClusterType | None" = None
        self.session = self._connect()

    def _connect(self):
        _load_cassandra()
        last_error: Exception | None = None
        for attempt in range(1, 61):
            try:
                profile = ExecutionProfile(
                    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=self.settings.cassandra_local_dc),
                    consistency_level=self.read_consistency,
                    request_timeout=20,
                )
                self.cluster = Cluster(
                    contact_points=list(self.settings.cassandra_hosts),
                    port=self.settings.cassandra_port,
                    protocol_version=5,
                    execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                )
                session = self.cluster.connect(self.settings.cassandra_keyspace)
                LOGGER.info("Connected to Cassandra hosts=%s keyspace=%s", self.settings.cassandra_hosts, self.settings.cassandra_keyspace)
                return session
            except Exception as exc:
                last_error = exc
                LOGGER.warning("Cassandra is not ready yet (%s/60): %s", attempt, exc)
                time.sleep(3)
        raise RuntimeError(f"Could not connect to Cassandra: {last_error}")

    def health_check(self) -> bool:
        try:
            statement = SimpleStatement("SELECT release_version FROM system.local", consistency_level=ConsistencyLevel.ONE)
            self.session.execute(statement, timeout=3).one()
            return True
        except Exception:
            LOGGER.exception("Cassandra health check failed")
            return False

    def process(self, event: dict[str, Any], metadata: KafkaMetadata) -> ProcessingResult:
        event_id = self._require_text(event, "event_id")
        event_type = self._require_text(event, "event_type")
        event_timestamp = parse_event_timestamp(event.get("event_timestamp"))

        if self._is_processed(event_id):
            LOGGER.info(
                "Skipping duplicate event_id=%s event_type=%s partition=%s offset=%s",
                event_id,
                event_type,
                metadata.partition,
                metadata.offset,
            )
            return ProcessingResult(event_id=event_id, event_type=event_type, status="DUPLICATE")

        if event_type == "PRODUCT_RECEIVED":
            return self._product_received(event, metadata, event_timestamp)
        if event_type == "PRODUCT_SHIPPED":
            return self._product_shipped(event, metadata, event_timestamp)
        if event_type == "PRODUCT_MOVED":
            return self._product_moved(event, metadata, event_timestamp)
        if event_type == "PRODUCT_RESERVED":
            return self._product_reserved(event, metadata, event_timestamp)
        if event_type == "PRODUCT_RELEASED":
            return self._product_released(event, metadata, event_timestamp)
        if event_type == "INVENTORY_COUNTED":
            return self._inventory_counted(event, metadata, event_timestamp)
        if event_type == "ORDER_CREATED":
            return self._order_created(event, metadata, event_timestamp)
        if event_type == "ORDER_COMPLETED":
            return self._order_completed(event, metadata, event_timestamp)
        raise ProcessingError(f"Unsupported event_type: {event_type}", "VALIDATION_ERROR")

    def _product_received(self, event: dict[str, Any], metadata: KafkaMetadata, event_timestamp) -> ProcessingResult:
        product_id = self._require_text(event, "product_id")
        zone_id = self._require_text(event, "zone_id")
        quantity = self._require_quantity(event)
        schema_version = int(event.get("schema_version") or 1)
        supplier_id = event.get("supplier_id") if schema_version >= 2 else None
        change = InventoryChange(
            product_id=product_id,
            zone_id=zone_id,
            available_delta=quantity,
            supplier_id=supplier_id,
            overwrite_supplier=True,
        )
        return self._apply_inventory_changes(event, metadata, event_timestamp, [change])

    def _product_shipped(self, event: dict[str, Any], metadata: KafkaMetadata, event_timestamp) -> ProcessingResult:
        change = InventoryChange(
            product_id=self._require_text(event, "product_id"),
            zone_id=self._require_text(event, "zone_id"),
            available_delta=-self._require_quantity(event),
        )
        return self._apply_inventory_changes(event, metadata, event_timestamp, [change])

    def _product_moved(self, event: dict[str, Any], metadata: KafkaMetadata, event_timestamp) -> ProcessingResult:
        product_id = self._require_text(event, "product_id")
        from_zone_id = self._require_text(event, "from_zone_id")
        to_zone_id = self._require_text(event, "to_zone_id")
        if from_zone_id == to_zone_id:
            raise ProcessingError("from_zone_id and to_zone_id must be different", "VALIDATION_ERROR")
        quantity = self._require_quantity(event)
        source_supplier = self._read_inventory(product_id, from_zone_id).supplier_id
        to_change = InventoryChange(product_id=product_id, zone_id=to_zone_id, available_delta=quantity)
        if source_supplier is not None:
            to_change.supplier_id = source_supplier
            to_change.overwrite_supplier = True
        changes = [
            InventoryChange(product_id=product_id, zone_id=from_zone_id, available_delta=-quantity),
            to_change,
        ]
        return self._apply_inventory_changes(event, metadata, event_timestamp, changes)

    def _product_reserved(self, event: dict[str, Any], metadata: KafkaMetadata, event_timestamp) -> ProcessingResult:
        quantity = self._require_quantity(event)
        change = InventoryChange(
            product_id=self._require_text(event, "product_id"),
            zone_id=self._require_text(event, "zone_id"),
            available_delta=-quantity,
            reserved_delta=quantity,
        )
        return self._apply_inventory_changes(event, metadata, event_timestamp, [change])

    def _product_released(self, event: dict[str, Any], metadata: KafkaMetadata, event_timestamp) -> ProcessingResult:
        quantity = self._require_quantity(event)
        change = InventoryChange(
            product_id=self._require_text(event, "product_id"),
            zone_id=self._require_text(event, "zone_id"),
            available_delta=quantity,
            reserved_delta=-quantity,
        )
        return self._apply_inventory_changes(event, metadata, event_timestamp, [change])

    def _inventory_counted(self, event: dict[str, Any], metadata: KafkaMetadata, event_timestamp) -> ProcessingResult:
        change = InventoryChange(
            product_id=self._require_text(event, "product_id"),
            zone_id=self._require_text(event, "zone_id"),
            set_available=self._require_counted_quantity(event),
        )
        return self._apply_inventory_changes(event, metadata, event_timestamp, [change])

    def _order_created(self, event: dict[str, Any], metadata: KafkaMetadata, event_timestamp) -> ProcessingResult:
        order_id = self._require_text(event, "order_id")
        order_key = f"order:{order_id}"
        stale_key = self._find_stale_entity([order_key], event_timestamp)
        if stale_key:
            return self._record_processed_only(event, metadata, event_timestamp, "STALE", f"Older than {stale_key}")
        if self._read_one("SELECT order_id FROM orders_by_id WHERE order_id = %s", (order_id,)):
            raise ProcessingError(f"Order already exists: {order_id}", "VALIDATION_ERROR")

        items = event.get("items")
        if not isinstance(items, list) or not items:
            raise ProcessingError("ORDER_CREATED requires non-empty items", "VALIDATION_ERROR")

        changes: list[InventoryChange] = []
        item_quantities: dict[tuple[str, str, str], int] = defaultdict(int)
        for item in items:
            if not isinstance(item, dict):
                raise ProcessingError("ORDER_CREATED item must be an object", "VALIDATION_ERROR")
            product_id = self._require_text(item, "product_id")
            quantity = self._require_quantity(item)
            zone_id = item.get("zone_id")
            if zone_id:
                zone_id = str(zone_id)
                changes.append(InventoryChange(product_id=product_id, zone_id=zone_id, available_delta=-quantity, reserved_delta=quantity))
                item_quantities[(order_id, product_id, zone_id)] += quantity
            else:
                allocated = self._allocate_product(product_id, quantity, changes)
                for allocated_zone, allocated_quantity in allocated:
                    changes.append(
                        InventoryChange(
                            product_id=product_id,
                            zone_id=allocated_zone,
                            available_delta=-allocated_quantity,
                            reserved_delta=allocated_quantity,
                        )
                    )
                    item_quantities[(order_id, product_id, allocated_zone)] += allocated_quantity

        extra_statements = [
            (
                "INSERT INTO orders_by_id (order_id, status, created_at, completed_at, last_event_timestamp, last_event_id, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (order_id, "CREATED", event_timestamp, None, event_timestamp, event["event_id"], utc_now()),
            )
        ]
        for row_key, quantity in item_quantities.items():
            row = (*row_key, quantity)
            extra_statements.append(
                (
                    "INSERT INTO order_items_by_order (order_id, product_id, zone_id, quantity) VALUES (%s, %s, %s, %s)",
                    row,
                )
            )
        return self._apply_inventory_changes(
            event,
            metadata,
            event_timestamp,
            changes,
            extra_statements=extra_statements,
            extra_entity_keys=[order_key],
        )

    def _order_completed(self, event: dict[str, Any], metadata: KafkaMetadata, event_timestamp) -> ProcessingResult:
        order_id = self._require_text(event, "order_id")
        order_key = f"order:{order_id}"
        stale_key = self._find_stale_entity([order_key], event_timestamp)
        if stale_key:
            return self._record_processed_only(event, metadata, event_timestamp, "STALE", f"Older than {stale_key}")
        order = self._read_one("SELECT status FROM orders_by_id WHERE order_id = %s", (order_id,))
        if not order:
            raise ProcessingError(f"Order does not exist: {order_id}", "VALIDATION_ERROR")
        if order.status == "COMPLETED":
            return self._record_processed_only(event, metadata, event_timestamp, "ORDER_ALREADY_COMPLETED", None)

        rows = self._read_all("SELECT product_id, zone_id, quantity FROM order_items_by_order WHERE order_id = %s", (order_id,))
        if not rows:
            raise ProcessingError(f"Order has no items: {order_id}", "VALIDATION_ERROR")
        changes = [
            InventoryChange(product_id=row.product_id, zone_id=row.zone_id, reserved_delta=-int(row.quantity))
            for row in rows
        ]
        extra_statements = [
            (
                "UPDATE orders_by_id SET status = %s, completed_at = %s, last_event_timestamp = %s, last_event_id = %s, updated_at = %s "
                "WHERE order_id = %s",
                ("COMPLETED", event_timestamp, event_timestamp, event["event_id"], utc_now(), order_id),
            )
        ]
        return self._apply_inventory_changes(
            event,
            metadata,
            event_timestamp,
            changes,
            extra_statements=extra_statements,
            extra_entity_keys=[order_key],
        )

    def _apply_inventory_changes(
        self,
        event: dict[str, Any],
        metadata: KafkaMetadata,
        event_timestamp,
        changes: list[InventoryChange],
        extra_statements: list[tuple[str, tuple[Any, ...]]] | None = None,
        extra_entity_keys: list[str] | None = None,
    ) -> ProcessingResult:
        entity_keys = list(extra_entity_keys or [])
        entity_keys.extend(f"product:{change.product_id}" for change in changes)
        entity_keys.extend(f"inventory:{change.product_id}:{change.zone_id}" for change in changes)
        stale_key = self._find_stale_entity(entity_keys, event_timestamp)
        if stale_key:
            return self._record_processed_only(event, metadata, event_timestamp, "STALE", f"Older than {stale_key}")

        rows: dict[tuple[str, str], InventoryRow] = {}
        product_deltas: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        for change in changes:
            key = (change.product_id, change.zone_id)
            current = rows.get(key) or self._read_inventory(change.product_id, change.zone_id)
            old_available = current.available
            old_reserved = current.reserved
            new_available = change.set_available if change.set_available is not None else old_available + change.available_delta
            new_reserved = old_reserved + change.reserved_delta
            if new_available < 0:
                raise ProcessingError(
                    f"Insufficient available quantity for product={change.product_id} zone={change.zone_id}: "
                    f"current={old_available}, delta={change.available_delta}",
                    "VALIDATION_ERROR",
                )
            if new_reserved < 0:
                raise ProcessingError(
                    f"Insufficient reserved quantity for product={change.product_id} zone={change.zone_id}: "
                    f"current={old_reserved}, delta={change.reserved_delta}",
                    "VALIDATION_ERROR",
                )
            supplier_id = change.supplier_id if change.overwrite_supplier else current.supplier_id
            rows[key] = InventoryRow(available=new_available, reserved=new_reserved, supplier_id=supplier_id)
            product_deltas[change.product_id][0] += new_available - old_available
            product_deltas[change.product_id][1] += new_reserved - old_reserved

        batch = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=self.write_consistency)
        now = utc_now()
        event_id = event["event_id"]
        event_type = event["event_type"]

        for (product_id, zone_id), row in rows.items():
            batch.add(
                "INSERT INTO inventory_by_product_zone "
                "(product_id, zone_id, available_quantity, reserved_quantity, supplier_id, last_event_timestamp, last_event_id, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (product_id, zone_id, row.available, row.reserved, row.supplier_id, event_timestamp, event_id, now),
            )
            batch.add(
                "INSERT INTO inventory_by_zone "
                "(zone_id, product_id, available_quantity, reserved_quantity, supplier_id, last_event_timestamp, last_event_id, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (zone_id, product_id, row.available, row.reserved, row.supplier_id, event_timestamp, event_id, now),
            )

        for product_id, (available_delta, reserved_delta) in product_deltas.items():
            aggregate = self._read_product_aggregate(product_id)
            total_available = aggregate.available + available_delta
            total_reserved = aggregate.reserved + reserved_delta
            if total_available < 0 or total_reserved < 0:
                raise ProcessingError(f"Negative aggregate inventory for product={product_id}", "VALIDATION_ERROR")
            supplier_id = self._latest_supplier_for_product(rows, product_id, aggregate.supplier_id)
            batch.add(
                "INSERT INTO inventory_by_product "
                "(product_id, total_available, total_reserved, supplier_id, last_event_timestamp, last_event_id, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (product_id, total_available, total_reserved, supplier_id, event_timestamp, event_id, now),
            )

        for entity_key in sorted(set(entity_keys)):
            batch.add(
                "INSERT INTO entity_versions (entity_key, last_event_timestamp, last_event_id, event_type, updated_at) "
                "VALUES (%s, %s, %s, %s, %s)",
                (entity_key, event_timestamp, event_id, event_type, now),
            )

        for query, params in extra_statements or []:
            batch.add(query, params)

        self._add_processed_event(batch, event, metadata, event_timestamp, "PROCESSED", now)
        self._add_event_history(batch, event, metadata, event_timestamp, "PROCESSED")
        self._execute_batch(batch)
        LOGGER.info(
            "Processed event_id=%s event_type=%s partition=%s offset=%s",
            event_id,
            event_type,
            metadata.partition,
            metadata.offset,
        )
        return ProcessingResult(event_id=event_id, event_type=event_type, status="PROCESSED")

    def _record_processed_only(
        self,
        event: dict[str, Any],
        metadata: KafkaMetadata,
        event_timestamp,
        status: str,
        reason: str | None,
    ) -> ProcessingResult:
        batch = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=self.write_consistency)
        now = utc_now()
        self._add_processed_event(batch, event, metadata, event_timestamp, status, now)
        self._add_event_history(batch, event, metadata, event_timestamp, status)
        self._execute_batch(batch)
        LOGGER.info(
            "Recorded event_id=%s event_type=%s status=%s reason=%s partition=%s offset=%s",
            event["event_id"],
            event["event_type"],
            status,
            reason,
            metadata.partition,
            metadata.offset,
        )
        return ProcessingResult(event_id=event["event_id"], event_type=event["event_type"], status=status, reason=reason)

    def _allocate_product(self, product_id: str, quantity: int, pending_changes: list[InventoryChange] | None = None) -> list[tuple[str, int]]:
        rows = sorted(
            self._read_all("SELECT zone_id, available_quantity FROM inventory_by_product_zone WHERE product_id = %s", (product_id,)),
            key=lambda row: row.zone_id,
        )
        remaining = quantity
        allocation: list[tuple[str, int]] = []
        for row in rows:
            pending_delta = sum(
                change.available_delta
                for change in pending_changes or []
                if change.product_id == product_id and change.zone_id == row.zone_id and change.set_available is None
            )
            available = int(row.available_quantity or 0) + pending_delta
            if available <= 0:
                continue
            taken = min(available, remaining)
            allocation.append((row.zone_id, taken))
            remaining -= taken
            if remaining == 0:
                break
        if remaining > 0:
            raise ProcessingError(
                f"Insufficient stock to reserve order item product={product_id}: requested={quantity}, missing={remaining}",
                "VALIDATION_ERROR",
            )
        return allocation

    def _find_stale_entity(self, entity_keys: Iterable[str], event_timestamp) -> str | None:
        for entity_key in sorted(set(entity_keys)):
            row = self._read_one("SELECT last_event_timestamp FROM entity_versions WHERE entity_key = %s", (entity_key,))
            if not row or row.last_event_timestamp is None:
                continue
            last_timestamp = ensure_utc(row.last_event_timestamp)
            if event_timestamp <= last_timestamp:
                return entity_key
        return None

    def _read_inventory(self, product_id: str, zone_id: str) -> InventoryRow:
        row = self._read_one(
            "SELECT available_quantity, reserved_quantity, supplier_id FROM inventory_by_product_zone WHERE product_id = %s AND zone_id = %s",
            (product_id, zone_id),
        )
        if not row:
            return InventoryRow(available=0, reserved=0, supplier_id=None)
        return InventoryRow(
            available=int(row.available_quantity or 0),
            reserved=int(row.reserved_quantity or 0),
            supplier_id=row.supplier_id,
        )

    def _read_product_aggregate(self, product_id: str) -> InventoryRow:
        row = self._read_one(
            "SELECT total_available, total_reserved, supplier_id FROM inventory_by_product WHERE product_id = %s",
            (product_id,),
        )
        if not row:
            return InventoryRow(available=0, reserved=0, supplier_id=None)
        return InventoryRow(
            available=int(row.total_available or 0),
            reserved=int(row.total_reserved or 0),
            supplier_id=row.supplier_id,
        )

    @staticmethod
    def _latest_supplier_for_product(rows: dict[tuple[str, str], InventoryRow], product_id: str, fallback: str | None) -> str | None:
        for (row_product_id, _zone_id), row in rows.items():
            if row_product_id == product_id:
                return row.supplier_id
        return fallback

    def _is_processed(self, event_id: str) -> bool:
        row = self._read_one("SELECT event_id FROM processed_events WHERE event_id = %s", (event_id,))
        return row is not None

    def _read_one(self, query: str, params: tuple[Any, ...]):
        statement = SimpleStatement(query, consistency_level=self.read_consistency)
        try:
            return self.session.execute(statement, params).one()
        except Exception as exc:
            LOGGER.exception("Cassandra read failed")
            raise StateReadError("Cassandra read failed") from exc

    def _read_all(self, query: str, params: tuple[Any, ...]):
        statement = SimpleStatement(query, consistency_level=self.read_consistency)
        try:
            return list(self.session.execute(statement, params))
        except Exception as exc:
            LOGGER.exception("Cassandra read failed")
            raise StateReadError("Cassandra read failed") from exc

    def _execute_batch(self, batch: BatchStatement) -> None:
        try:
            self.session.execute(batch)
        except Exception:
            if self.cassandra_error_counter is not None:
                self.cassandra_error_counter.inc()
            LOGGER.exception("Cassandra batch write failed")
            raise StateWriteError("Cassandra batch write failed") from None

    def _add_processed_event(
        self,
        batch: BatchStatement,
        event: dict[str, Any],
        metadata: KafkaMetadata,
        event_timestamp,
        status: str,
        processed_at,
    ) -> None:
        batch.add(
            "INSERT INTO processed_events "
            "(event_id, event_type, processed_at, event_timestamp, kafka_topic, kafka_partition, kafka_offset, status) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                event["event_id"],
                event["event_type"],
                processed_at,
                event_timestamp,
                metadata.topic,
                metadata.partition,
                metadata.offset,
                status,
            ),
        )

    def _add_event_history(
        self,
        batch: BatchStatement,
        event: dict[str, Any],
        metadata: KafkaMetadata,
        event_timestamp,
        status: str,
    ) -> None:
        product_ids = self._event_product_ids(event)
        payload = json.dumps(event, ensure_ascii=True, sort_keys=True)
        for product_id in product_ids:
            batch.add(
                "INSERT INTO event_history_by_product "
                "(product_id, event_timestamp, event_id, event_type, status, kafka_partition, kafka_offset, event_payload) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    product_id,
                    event_timestamp,
                    event["event_id"],
                    event["event_type"],
                    status,
                    metadata.partition,
                    metadata.offset,
                    payload,
                ),
            )

    @staticmethod
    def _event_product_ids(event: dict[str, Any]) -> set[str]:
        product_ids: set[str] = set()
        if event.get("product_id"):
            product_ids.add(str(event["product_id"]))
        items = event.get("items")
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and item.get("product_id"):
                    product_ids.add(str(item["product_id"]))
        if not product_ids and event.get("order_id"):
            product_ids.add(f"ORDER:{event['order_id']}")
        return product_ids

    @staticmethod
    def _require_text(source: dict[str, Any], field: str) -> str:
        value = source.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ProcessingError(f"{field} is required", "VALIDATION_ERROR")
        return value.strip()

    @staticmethod
    def _require_quantity(source: dict[str, Any], allow_zero: bool = False) -> int:
        value = source.get("quantity")
        if not isinstance(value, int):
            raise ProcessingError("quantity is required and must be an integer", "VALIDATION_ERROR")
        if allow_zero:
            if value < 0:
                raise ProcessingError(f"Invalid quantity: {value} (must be >= 0)", "VALIDATION_ERROR")
        elif value <= 0:
            raise ProcessingError(f"Invalid quantity: {value} (must be positive)", "VALIDATION_ERROR")
        return value

    @staticmethod
    def _require_counted_quantity(source: dict[str, Any]) -> int:
        value = source.get("counted_quantity")
        if value is None:
            value = source.get("quantity")
        if not isinstance(value, int):
            raise ProcessingError("counted_quantity is required and must be an integer", "VALIDATION_ERROR")
        if value < 0:
            raise ProcessingError(f"Invalid counted_quantity: {value} (must be >= 0)", "VALIDATION_ERROR")
        return value
