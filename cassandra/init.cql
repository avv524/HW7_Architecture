CREATE KEYSPACE IF NOT EXISTS warehouse
WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '1'}
AND durable_writes = true;

USE warehouse;

CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
    product_id text,
    zone_id text,
    available_quantity int,
    reserved_quantity int,
    supplier_id text,
    last_event_timestamp timestamp,
    last_event_id text,
    updated_at timestamp,
    PRIMARY KEY ((product_id), zone_id)
) WITH CLUSTERING ORDER BY (zone_id ASC);

CREATE TABLE IF NOT EXISTS inventory_by_product (
    product_id text PRIMARY KEY,
    total_available int,
    total_reserved int,
    supplier_id text,
    last_event_timestamp timestamp,
    last_event_id text,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS inventory_by_zone (
    zone_id text,
    product_id text,
    available_quantity int,
    reserved_quantity int,
    supplier_id text,
    last_event_timestamp timestamp,
    last_event_id text,
    updated_at timestamp,
    PRIMARY KEY ((zone_id), product_id)
) WITH CLUSTERING ORDER BY (product_id ASC);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id text PRIMARY KEY,
    event_type text,
    processed_at timestamp,
    event_timestamp timestamp,
    kafka_topic text,
    kafka_partition int,
    kafka_offset bigint,
    status text
);

CREATE TABLE IF NOT EXISTS entity_versions (
    entity_key text PRIMARY KEY,
    last_event_timestamp timestamp,
    last_event_id text,
    event_type text,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS event_history_by_product (
    product_id text,
    event_timestamp timestamp,
    event_id text,
    event_type text,
    status text,
    kafka_partition int,
    kafka_offset bigint,
    event_payload text,
    PRIMARY KEY ((product_id), event_timestamp, event_id)
) WITH CLUSTERING ORDER BY (event_timestamp DESC, event_id ASC);

CREATE TABLE IF NOT EXISTS orders_by_id (
    order_id text PRIMARY KEY,
    status text,
    created_at timestamp,
    completed_at timestamp,
    last_event_timestamp timestamp,
    last_event_id text,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS order_items_by_order (
    order_id text,
    product_id text,
    zone_id text,
    quantity int,
    PRIMARY KEY ((order_id), product_id, zone_id)
) WITH CLUSTERING ORDER BY (product_id ASC, zone_id ASC);
