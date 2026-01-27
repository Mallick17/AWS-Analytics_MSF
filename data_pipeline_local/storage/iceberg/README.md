# Iceberg Storage

Apache Iceberg table definitions for the analytics platform.

## Overview

Iceberg tables are stored in LocalStack S3 for local development:
- **Warehouse**: `s3://analytics-data/warehouse`
- **Catalog**: Hadoop-style (no AWS Glue dependency)
- **Format**: Parquet with Snappy compression

## Tables

### analytics.orders

Stores order creation events after Flink processing.

| Column | Type | Description |
|--------|------|-------------|
| event_id | STRING | Unique identifier for the event |
| event_time | TIMESTAMP | Event timestamp (partition key) |
| order_id | STRING | Unique identifier for the order |
| user_id | STRING | User who placed the order |
| order_amount | DECIMAL(10,2) | Total order amount |
| currency | STRING | ISO 4217 currency code |
| order_status | STRING | Order status (CREATED) |
| processed_at | TIMESTAMP | Flink processing timestamp |

**Partitioning**: By day (`days(event_time)`)

## Querying Data

### Using PyIceberg

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "local",
    **{
        "type": "hadoop",
        "warehouse": "s3://analytics-data/warehouse",
        "s3.endpoint": "http://localhost:4566",
        "s3.access-key-id": "test",
        "s3.secret-access-key": "test",
    }
)

table = catalog.load_table("analytics.orders")
df = table.scan().to_pandas()
print(df)
```

### Using Spark

```python
spark.read.format("iceberg").load("local.analytics.orders").show()
```

## Cloud Migration

For cloud deployment:
1. Change warehouse to AWS S3 path
2. Optionally switch to AWS Glue catalog
3. Update S3 credentials/IAM

No table schema changes required.
