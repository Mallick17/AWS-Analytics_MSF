-- Iceberg Table Definition: analytics.orders
-- This schema defines the orders table for storing processed order events

-- Table: analytics.orders
-- Stores order creation events after Flink processing

CREATE TABLE IF NOT EXISTS analytics.orders (
    -- Event metadata
    event_id        STRING      NOT NULL    COMMENT 'Unique identifier for the event',
    event_time      TIMESTAMP   NOT NULL    COMMENT 'Event timestamp (used for partitioning)',
    
    -- Order data
    order_id        STRING      NOT NULL    COMMENT 'Unique identifier for the order',
    user_id         STRING      NOT NULL    COMMENT 'Identifier of the user who placed the order',
    order_amount    DECIMAL(10,2) NOT NULL  COMMENT 'Total order amount',
    currency        STRING      NOT NULL    COMMENT 'ISO 4217 currency code',
    order_status    STRING      NOT NULL    COMMENT 'Status of the order (CREATED)',
    
    -- Processing metadata
    processed_at    TIMESTAMP   NOT NULL    COMMENT 'Flink processing timestamp'
)
PARTITIONED BY (days(event_time))
COMMENT 'Order creation events ingested from Kafka'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);

-- Notes:
-- 1. Partitioned by day for efficient time-range queries
-- 2. Uses Iceberg format v2 for better performance
-- 3. Parquet format with Snappy compression for storage efficiency
-- 4. event_time is the event-time reference for Flink watermarks
-- 5. processed_at tracks when Flink processed the event
