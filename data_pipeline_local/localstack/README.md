# LocalStack Configuration

LocalStack provides local AWS S3 emulation for Iceberg storage.

## Overview

- **Service**: S3 only (no Glue, no IAM)
- **Endpoint**: `http://localhost:4566`
- **Credentials**: `test` / `test` (dummy)
- **Region**: `us-east-1`

## S3 Structure

```
s3://analytics-data/
├── warehouse/          # Iceberg table data
│   └── analytics/
│       └── orders/     # Orders table
├── checkpoints/        # Flink checkpoints
└── savepoints/         # Flink savepoints
```

## Initialization

The `init/s3.sh` script runs automatically when LocalStack starts:
- Creates `analytics-data` bucket
- Sets up directory structure

## CLI Commands

```bash
# List buckets
aws --endpoint-url=http://localhost:4566 s3 ls

# List warehouse contents
aws --endpoint-url=http://localhost:4566 s3 ls s3://analytics-data/warehouse/ --recursive

# View Iceberg metadata
aws --endpoint-url=http://localhost:4566 s3 ls s3://analytics-data/warehouse/analytics/orders/metadata/
```

## Known Limitations

1. **No S3 Tables control plane** - LocalStack doesn't emulate AWS S3 Tables management APIs
2. **No IAM** - All requests succeed without authentication
3. **Performance** - Not representative of AWS S3 performance

These are acceptable trade-offs for local development.
