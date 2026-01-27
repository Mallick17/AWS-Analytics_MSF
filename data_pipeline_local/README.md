# Flink Analytics Platform - Local Development Environment

A local development environment for building and testing PyFlink streaming jobs that consume Kafka events and write to Iceberg tables.

## Overview

This project provides a **production-aligned local setup** for developing Flink analytics pipelines:

- **Kafka** (KRaft mode) for event streaming
- **Apache Flink** (PyFlink) for stream processing
- **LocalStack S3** + **Apache Iceberg** for data lake storage
- **Python CLI** for event generation

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Java 11 (for PyFlink)

### Setup

```bash
# 1. Run setup script
chmod +x infra/scripts/*.sh
./infra/scripts/setup.sh

# 2. Start all services
./infra/scripts/start.sh

# 3. Verify health
./infra/scripts/health-check.sh
```

### Install Dependencies

```bash
# Producer
cd producer && pip install -r requirements.txt

# Flink
cd flink && pip install -r requirements.txt
```

### Run the Pipeline

```bash
# Terminal 1: Start Flink job
cd flink
python -m jobs.order_ingest.job

# Terminal 2: Produce events
cd producer
python cli.py produce orders --rate 10 --count 100
```

### Access UIs

- **Flink Dashboard**: http://localhost:8081

## Project Structure

```
rt_data_pipeline_local/
├── infra/                  # Docker Compose & scripts
│   ├── docker-compose.yml
│   └── scripts/
├── producer/               # Event generator CLI
│   ├── cli.py
│   ├── schemas/
│   └── generators/
├── flink/                  # PyFlink jobs
│   ├── common/             # Shared utilities
│   └── jobs/
│       └── order_ingest/   # Reference pipeline
├── storage/                # Iceberg schemas
├── localstack/             # S3 initialization
├── tests/                  # E2E tests
└── docs/                   # Documentation
```

## Reference Pipeline: Order Ingestion

```
Producer → Kafka (orders.created.v1) → Flink → Iceberg (analytics.orders)
```

### Event Schema

```json
{
  "event_id": "uuid",
  "event_time": "ISO-8601 timestamp",
  "order_id": "string",
  "user_id": "string",
  "order_amount": "decimal",
  "currency": "string",
  "order_status": "CREATED"
}
```

## Configuration

All components use environment variables for cloud portability:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka brokers |
| `AWS_ENDPOINT_URL` | http://localhost:4566 | S3 endpoint |
| `ICEBERG_WAREHOUSE` | s3://analytics-data/warehouse | Iceberg warehouse |

See `.env.example` for all options.

## Testing

```bash
# Unit tests
cd flink && pytest tests/unit/ -v

# Integration tests (requires services)
cd flink && pytest tests/integration/ -v

# E2E tests (requires services + job)
pytest tests/e2e/ -v
```

## Documentation

- [Architecture](docs/architecture.md)
- [Developer Workflow](docs/dev-workflow.md)
- [Order Pipeline](docs/order-pipeline.md)
- [Cloud Deployment Notes](docs/cloud-flink-notes.md)

## Design Principles

1. **Real runtimes**: Kafka and Flink run as actual services, not emulated
2. **Cloud-ready**: Same code deploys to AWS with config changes only
3. **No Glue**: Hadoop catalog for simplicity (Glue optional for cloud)
4. **Single responsibility**: Clear separation between producer, flink, storage

## Known Limitations

- LocalStack doesn't emulate AWS S3 Tables control plane
- Performance differs from AWS
- No IAM/security validation locally

These are acceptable trade-offs for local development.

## Success Criteria

✅ Start system locally in <30 minutes  
✅ End-to-end flow without cloud dependencies  
✅ Iterate on jobs without rebuilding containers  
✅ Query Iceberg data locally  
✅ Deploy to Cloud Flink with config-only changes

## License

Internal use only.
