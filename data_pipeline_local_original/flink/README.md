# Flink Analytics Jobs

PyFlink streaming jobs for the analytics platform.

## Structure

```
flink/
├── common/              # Shared utilities
│   ├── config.py        # Environment-based configuration
│   ├── kafka_source.py  # Kafka source factory
│   ├── serialization.py # JSON ser/de utilities
│   └── job_base.py      # Abstract job base class
│
├── jobs/                # Job implementations
│   └── order_ingest/    # Order ingestion job
│       ├── job.py       # Job implementation
│       ├── config.yaml  # Job-specific config
│       └── README.md    # Job documentation
│
└── tests/               # Test suites
    ├── unit/            # Unit tests
    └── integration/     # Integration tests
```

## Installation

```bash
uv sync --group producer --group test
```

Note: PyFlink runs inside the Dockerized Flink runtime in this project. Your host Python is not used to execute Flink jobs.

Note: PyFlink requires Java 11. Ensure `JAVA_HOME` is set.

## Running Jobs

### Local Development

```bash
# Start infrastructure
cd ../infra && ./scripts/start.sh

# Submit order ingest job to the Dockerized Flink cluster
cd ..
./infra/scripts/submit-order-ingest.sh
```

### Submit to Flink Cluster

```bash
# Via Flink CLI
flink run -py jobs/order_ingest/job.py

# Via REST API
curl -X POST http://localhost:8081/jars/upload -F "jarfile=@job.jar"
```

## Configuration

All jobs use environment-based configuration for cloud portability:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka brokers |
| `KAFKA_TOPIC_ORDERS` | orders.created.v1 | Order events topic |
| `FLINK_PARALLELISM` | 1 | Job parallelism |
| `FLINK_CHECKPOINT_INTERVAL` | 10000 | Checkpoint interval (ms) |

## Cloud Deployment

Jobs are designed for cloud portability:
- No hardcoded local paths
- Environment-driven configuration
- Compatible with AWS Managed Flink / EMR Flink

To deploy to cloud:
1. Update environment variables for cloud endpoints
2. Replace LocalStack S3 with AWS S3
3. Optionally switch to AWS Glue catalog
