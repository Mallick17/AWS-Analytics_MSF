# Order Ingest Job

Reference Flink job for ingesting order creation events.

## Overview

This job:
1. Consumes events from Kafka topic `orders.created.v1`
2. Validates and deserializes JSON events
3. Assigns event-time watermarks
4. Adds `processed_at` timestamp
5. Prints rows via the built-in print connector

## Running the Job

### Local Development

```bash
# Start infrastructure first
cd infra && ./scripts/start.sh

# Run the job
cd flink
python -m jobs.order_ingest.job
```

### Submit to Flink Cluster

```bash
# Package and submit
flink run -py jobs/order_ingest/job.py
```

## Pipeline

```
Kafka (orders.created.v1)
    │
    ▼
┌─────────────────────────┐
│  Deserialize JSON       │
│  Validate Schema        │
│  Assign Watermarks      │
└─────────────────────────┘
    │
    ▼
┌─────────────────────────┐
│  Add processed_at       │
│  (minimal transform)    │
└─────────────────────────┘
    │
    ▼
Print Sink (stdout/logs)
```

## Configuration

Environment variables:
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka brokers
- `KAFKA_TOPIC_ORDERS`: Source topic

## Schema

### Input (Kafka)
```json
{
  "event_id": "uuid",
  "event_time": "ISO-8601",
  "order_id": "string",
  "user_id": "string",
  "order_amount": "decimal",
  "currency": "string",
  "order_status": "CREATED"
}
```

### Output
All input fields plus:
- `processed_at`: Flink processing timestamp
