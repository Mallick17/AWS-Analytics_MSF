# Data Pipeline Flow: Local vs AWS

Visual guide showing how data flows through the pipeline in both environments.

---

## 🔄 Local Development Flow

```
Producer → Kafka (Docker) → Flink (Docker) → Iceberg (Filesystem) → ./warehouse/
```

### Execution Steps

1. **Producer generates events**
   ```bash
   python producer/cli.py produce orders --rate 10
   ```
   - Creates JSON events with Faker
   - Sends to Kafka topic `orders.created.v1`

2. **Kafka stores events**
   - Running in Docker at `kafka:29092`
   - No authentication (PLAINTEXT)

3. **Flink job runs**
   ```bash
   docker exec flink-jobmanager python /opt/flink/usrlib/jobs/order_ingest/job.py
   ```
   - Detects `DEPLOYMENT_ENV=local`
   - Uses **Hadoop catalog** (filesystem-based)
   - Connects to Kafka without auth

4. **Data written to filesystem**
   - Location: `./warehouse/analytics.db/orders/`
   - Format: Parquet files
   - Partitioned by day

**Directory Structure**:
```
warehouse/
└── analytics.db/
    └── orders/
        ├── metadata/        # Iceberg metadata
        └── data/            # Parquet files
            └── event_time_day=2026-02-10/
```

---

## ☁️ AWS Production Flow

```
External Producer → AWS MSK → Managed Flink → S3 Tables → S3 Bucket
```

### Execution Steps

1. **Deploy application**
   ```bash
   mvn clean package
   aws s3 cp target/data-pipeline-aws.zip s3://my-bucket/
   ```

2. **Managed Flink starts**
   - Runs `streaming_job.py` (entry point)
   - Injects `lib/pyflink-dependencies.jar`
   - Imports and runs `jobs/order_ingest/job.py`

3. **Job detects AWS environment**
   - `DEPLOYMENT_ENV=aws`
   - Uses **S3 Tables catalog**
   - Connects to MSK with **IAM authentication**

4. **Data written to S3**
   - Location: S3 Tables bucket (managed by AWS)
   - Format: Parquet files
   - Partitioned by day

---

## 🔀 Key Differences

| Component | Local | AWS |
|-----------|-------|-----|
| **Entry Point** | `job.py` directly | `streaming_job.py` → `job.py` |
| **JAR Loading** | Pre-installed in image | Runtime injection |
| **Kafka** | Docker (no auth) | MSK (IAM auth) |
| **Catalog** | Hadoop (filesystem) | S3 Tables |
| **Storage** | `./warehouse/` | S3 bucket |
| **Config** | `DEPLOYMENT_ENV=local` | `DEPLOYMENT_ENV=aws` |

---

## 🎯 Same Code, Different Behavior

The Flink job code is **identical** in both environments. Differences are handled by:

**Environment Detection**:
```python
def is_aws_environment() -> bool:
    return os.getenv("DEPLOYMENT_ENV") == "aws"
```

**Conditional Configuration**:
- Local: `warehouse = "file:///opt/iceberg/warehouse"`
- AWS: `warehouse = "arn:aws:s3tables:..."`

**Conditional Catalog**:
- Local: Hadoop catalog with `HadoopFileIO`
- AWS: S3 Tables catalog with `S3TablesCatalog`

**Conditional Authentication**:
- Local: No Kafka auth
- AWS: MSK IAM authentication

---

## 📊 Complete Flow Comparison

### Local
```
1. mvn clean package                    # Build JARs
2. docker-compose build                 # Build Flink image
3. docker-compose up -d                 # Start services
4. docker exec ... python job.py        # Run job
5. python producer/cli.py produce       # Generate events
6. ls warehouse/analytics.db/orders/    # Verify data
```

### AWS
```
1. mvn clean package                    # Build ZIP
2. aws s3 cp ... s3://bucket/           # Upload
3. Create Managed Flink app             # Via Console
4. Set environment variables            # DEPLOYMENT_ENV=aws
5. Start application                    # Via Console
6. Check S3 Tables                      # Verify data
```

---

## 💡 Summary

- **Local**: Fast iteration, filesystem storage, no AWS costs
- **AWS**: Production-ready, scalable, managed infrastructure
- **Same code** runs in both with automatic environment detection
