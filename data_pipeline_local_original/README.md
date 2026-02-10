# Flink Analytics Platform - Local Development Environment

A local development environment for building and testing PyFlink streaming jobs that consume Kafka events.

## Overview

This project provides a **production-aligned local setup** for developing Flink analytics pipelines:

- **Kafka** (Zookeeper mode) for event streaming
- **Apache Flink** (PyFlink) for stream processing
- **Python CLI** for event generation

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.13+
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
uv sync --group producer --group test
```

Note: the Flink job runs inside the Dockerized Flink runtime. Your host Python (3.13) is only used for the producer and tests.

### Run the Pipeline

```bash
# Terminal 1: Submit Flink job to the Dockerized cluster
./infra/scripts/submit-order-ingest.sh

# Terminal 2: Produce events
uv run --group producer python producer/cli.py produce orders --rate 10 --count 100
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
├── tests/                  # E2E tests
└── docs/                   # Documentation
```

## Reference Pipeline: Order Ingestion

```
Producer → Kafka (orders.created.v1) → Flink (print sink)
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
| `FLINK_CHECKPOINT_DIR` | file:///opt/flink/checkpoints | Checkpoint directory |

See `.env.example` for all options.

## Testing

```bash
# Unit tests
uv run --group test --group flink pytest flink/tests/unit/ -v

# Integration tests (requires services)
uv run --group test --group flink pytest flink/tests/integration/ -v

# E2E tests (requires services + job)
uv run --group test pytest tests/e2e/ -v
```

## Documentation

- [Architecture](docs/architecture.md)
- [Developer Workflow](docs/dev-workflow.md)
- [Order Pipeline](docs/order-pipeline.md)
- [Cloud Deployment Notes](docs/cloud-flink-notes.md)

## Design Principles

1. **Real runtimes**: Kafka and Flink run as actual services, not emulated
2. **Cloud-ready**: Same code deploys to AWS with config changes only
3. **Single responsibility**: Clear separation between producer and flink

## Known Limitations

- This setup prints to Flink logs instead of persisting to a lakehouse table

These are acceptable trade-offs for local development.

## Success Criteria

✅ Start system locally in <30 minutes  
✅ End-to-end flow without cloud dependencies  
✅ Iterate on jobs without rebuilding containers  
✅ See events printed from Flink job logs

## License

Internal use only.
