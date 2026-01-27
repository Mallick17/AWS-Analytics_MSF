# Order Pipeline

## Overview

The order pipeline is the reference implementation for event ingestion. It demonstrates the complete flow from event generation to data lake storage.

## Pipeline Flow

```
Producer                    Kafka                      Flink                     Iceberg
   │                          │                          │                          │
   │  Generate Order Event    │                          │                          │
   ├─────────────────────────▶│                          │                          │
   │                          │   Publish to Topic       │                          │
   │                          │   orders.created.v1      │                          │
   │                          │                          │                          │
   │                          │                          │  Consume Event           │
   │                          ├─────────────────────────▶│                          │
   │                          │                          │                          │
   │                          │                          │  Validate Schema         │
   │                          │                          │  Assign Watermarks       │
   │                          │                          │  Add processed_at        │
   │                          │                          │                          │
   │                          │                          │  Write to Iceberg        │
   │                          │                          ├─────────────────────────▶│
   │                          │                          │                          │
```

## Event Schema

### Order Created Event (v1)

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_time": "2024-01-15T10:30:00.000Z",
  "order_id": "ORD-A1B2C3D4",
  "user_id": "USR-X9Y8Z7W6",
  "order_amount": "150.50",
  "currency": "USD",
  "order_status": "CREATED"
}
```

### Schema Rules

| Field | Type | Constraints |
|-------|------|-------------|
| event_id | UUID | Required, unique |
| event_time | ISO-8601 | Required, event-time reference |
| order_id | String | Required, 1-64 chars |
| user_id | String | Required, 1-64 chars |
| order_amount | Decimal | Required, > 0, 2 decimal places |
| currency | String | Required, 3 chars (ISO 4217) |
| order_status | Enum | Required, always "CREATED" |

## Components

### 1. Producer (`producer/`)

**Schema Definition**: `producer/schemas/order_created_v1.py`
- Pydantic model with validation
- JSON serialization

**Generator**: `producer/generators/order_generator.py`
- Faker-based realistic data
- Deterministic mode via seed

**Kafka Client**: `producer/kafka/producer.py`
- Partitioned by order_id
- Delivery confirmation

### 2. Flink Job (`flink/jobs/order_ingest/`)

**Job Implementation**: `flink/jobs/order_ingest/job.py`
- Consumes from Kafka
- Validates events
- Assigns watermarks (5s tolerance)
- Adds processed_at timestamp
- Writes to Iceberg

**Transformation SQL**:
```sql
INSERT INTO local.analytics.orders
SELECT
    event_id,
    event_time,
    order_id,
    user_id,
    order_amount,
    currency,
    order_status,
    CURRENT_TIMESTAMP as processed_at
FROM orders_source
```

### 3. Iceberg Table (`storage/iceberg/`)

**Table Definition**: `storage/iceberg/schemas/orders.sql`

| Column | Type | Description |
|--------|------|-------------|
| event_id | STRING | Event identifier |
| event_time | TIMESTAMP | Event timestamp |
| order_id | STRING | Order identifier |
| user_id | STRING | User identifier |
| order_amount | DECIMAL(10,2) | Order amount |
| currency | STRING | Currency code |
| order_status | STRING | Order status |
| processed_at | TIMESTAMP | Processing timestamp |

**Partitioning**: `days(event_time)`

## Running the Pipeline

### 1. Start Infrastructure

```bash
./infra/scripts/start.sh
```

### 2. Start Flink Job

```bash
cd flink
python -m jobs.order_ingest.job
```

### 3. Produce Events

```bash
cd producer
python cli.py produce orders --rate 10 --count 100
```

### 4. Verify Data

```bash
# Check S3 for Iceberg files
aws --endpoint-url=http://localhost:4566 \
  s3 ls s3://analytics-data/warehouse/analytics/orders/ --recursive
```

## Monitoring

### Flink Metrics

- **Records In/Out**: Events processed
- **Checkpoints**: Recovery points
- **Latency**: Processing delay

Access via Flink UI: http://localhost:8081

### Kafka Lag

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group flink-order-ingest \
  --describe
```

## Error Handling

| Error | Handling |
|-------|----------|
| Invalid JSON | Logged, skipped |
| Missing field | Logged, skipped |
| Kafka unavailable | Flink restarts |
| S3 unavailable | Checkpoint retry |
