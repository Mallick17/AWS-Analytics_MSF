# Developer Workflow

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.13+
- Java 11 (for PyFlink)

### Initial Setup

```bash
# 1. Clone and navigate to project
cd rt_data_pipeline_local

# 2. Run setup script
chmod +x infra/scripts/*.sh
./infra/scripts/setup.sh

# 3. Start all services
./infra/scripts/start.sh

# 4. Verify health
./infra/scripts/health-check.sh
```

### Install Python Dependencies

```bash
uv sync --group producer --group test
```

Note: the Flink job runs inside the Dockerized Flink runtime. Your host Python (3.13) is only used for the producer and tests.

## Daily Development

### Starting the Environment

```bash
./infra/scripts/start.sh
```

### Stopping the Environment

```bash
# Keep data
./infra/scripts/stop.sh

# Clean data
./infra/scripts/stop.sh --clean
```

### Producing Events

```bash
# Generate 100 events
uv run --group producer python producer/cli.py produce orders --rate 10 --count 100

# Continuous stream
uv run --group producer python producer/cli.py produce orders --rate 5 --count 0

# Dry run (no Kafka)
uv run --group producer python producer/cli.py produce orders --dry-run --count 5
```

### Running Flink Jobs

```bash
# Submit order ingest job to the Dockerized Flink cluster
./infra/scripts/submit-order-ingest.sh
```

### Viewing Logs

```bash
# All services
docker compose -f infra/docker-compose.yml logs -f

# Specific service
docker compose -f infra/docker-compose.yml logs -f kafka
docker compose -f infra/docker-compose.yml logs -f jobmanager
```

### Accessing UIs

- **Flink UI**: http://localhost:8081
- **Kafka** (via CLI): `docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list`

## Testing

### Unit Tests

```bash
uv run --group test --group flink pytest flink/tests/unit/ -v
```

### Integration Tests

```bash
# Requires services running
uv run --group test --group flink pytest flink/tests/integration/ -v
```

### End-to-End Tests

```bash
# Requires services + Flink job running
uv run --group test pytest tests/e2e/ -v --timeout=120
```

## Debugging

### Flink Job Issues

1. Check Flink UI: http://localhost:8081
2. View job logs in TaskManager
3. Check checkpoints directory

### Kafka Issues

```bash
# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# View messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders.created.v1 \
  --from-beginning \
  --max-messages 10
```

## Code Changes

### Modifying Producer

1. Edit files in `producer/`
2. No rebuild needed
3. Re-run CLI command

### Modifying Flink Jobs

1. Edit files in `flink/`
2. Jobs mount as volume, no rebuild needed
3. Submit new job (cancels old one)

### Modifying Infrastructure

1. Edit `infra/docker-compose.yml`
2. Run `./infra/scripts/stop.sh && ./infra/scripts/start.sh`

## Best Practices

1. **Use seeds for reproducibility**: `--seed 42`
2. **Clean environment for fresh start**: `./stop.sh --clean`
3. **Check health before testing**: `./health-check.sh`
4. **Monitor Flink UI during development**
5. **Use dry-run for schema validation**
