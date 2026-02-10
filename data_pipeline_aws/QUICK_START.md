# Quick Start Guide

## 🚀 Get Started in 5 Minutes

This guide will help you run the transformation pipeline locally and see data flowing through all three topics.

---

## Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Maven 3.6+
- Java 11+

---

## Step 1: Build the Project

```bash
cd data_pipeline_aws

# Build JAR dependencies
mvn clean package

# Verify outputs
ls -lh target/pyflink-dependencies.jar
ls -lh target/data-pipeline-aws.zip
```

---

## Step 2: Start Infrastructure

```bash
cd infra

# Start Kafka, MinIO, Iceberg REST, and Flink
docker-compose up -d

# Check services are running
docker-compose ps

# Expected services:
# - kafka (port 9092)
# - minio (ports 9000, 9001)
# - iceberg-rest (port 8181)
# - flink-jobmanager (port 8081)
# - flink-taskmanager
```

---

## Step 3: Install Producer Dependencies

```bash
cd ../producer

# Create virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

---

## Step 4: Produce Test Data

```bash
# Produce to all topics in parallel
python cli.py produce all --rate 10 --count 100

# Or produce to individual topics:
python cli.py produce orders --rate 10 --count 50
python cli.py produce bids --rate 5 --count 30
python cli.py produce users --rate 15 --count 70
```

You should see output like:
```
2026-02-10 16:45:00 - INFO - Producing to all topics (rate=10/s, count=100 per topic)
2026-02-10 16:45:01 - INFO - ✓ orders.created.v1: Sent 100 events
2026-02-10 16:45:01 - INFO - ✓ bid-events: Sent 100 events
2026-02-10 16:45:01 - INFO - ✓ user-events: Sent 100 events
2026-02-10 16:45:01 - INFO - ✓ All topics completed
```

---

## Step 5: Run the Flink Job

```bash
cd ..

# Run the transformation pipeline
python streaming_job.py
```

You should see output like:
```
================================================================================
FLINK TRANSFORMATION PIPELINE
================================================================================
Environment: LOCAL
Python version: 3.9.x
Working directory: /path/to/data_pipeline_aws

================================================================================
LOADING CONFIGURATION
================================================================================

Enabled topics (3):
  - orders.created.v1
  - bid-events
  - user-events

================================================================================
CREATING FLINK ENVIRONMENT
================================================================================
✓ Parallelism: 1

================================================================================
CREATING ICEBERG CATALOG
================================================================================
✓ Created catalog: iceberg_catalog.analytics

================================================================================
CREATING PIPELINES
================================================================================

────────────────────────────────────────────────────────────────────────────────
Processing: orders.created.v1
────────────────────────────────────────────────────────────────────────────────
  Transformation: order_events_raw
  Description: Transforms raw order events with timestamp conversion
  Module: transformations.order_events_raw
  Class: OrderEventsRawTransformer
  Loading transformer...
  Creating Kafka source...
  ✓ Created Kafka source: kafka_source_orders_created_v1
  Creating Iceberg sink...
  ✓ Created Iceberg sink: iceberg_catalog.analytics.orders
  Generating transformation SQL...
  Executing pipeline...
  ✓ Pipeline created: orders.created.v1 → iceberg_catalog.analytics.orders

────────────────────────────────────────────────────────────────────────────────
Processing: bid-events
────────────────────────────────────────────────────────────────────────────────
  Transformation: bid_events_enriched
  Description: Enriches bid events with computed fields (is_mobile)
  Module: transformations.bid_events_enriched
  Class: BidEventsEnrichedTransformer
  Loading transformer...
  Creating Kafka source...
  ✓ Created Kafka source: kafka_source_bid_events
  Creating Iceberg sink...
  ✓ Created Iceberg sink: iceberg_catalog.analytics.bid_events
  Generating transformation SQL...
  Executing pipeline...
  ✓ Pipeline created: bid-events → iceberg_catalog.analytics.bid_events

────────────────────────────────────────────────────────────────────────────────
Processing: user-events
────────────────────────────────────────────────────────────────────────────────
  Transformation: user_events_processed
  Description: Processes user events with device detection
  Module: transformations.user_events_processed
  Class: UserEventsProcessedTransformer
  Loading transformer...
  Creating Kafka source...
  ✓ Created Kafka source: kafka_source_user_events
  Creating Iceberg sink...
  ✓ Created Iceberg sink: iceberg_catalog.analytics.user_activities
  Generating transformation SQL...
  Executing pipeline...
  ✓ Pipeline created: user-events → iceberg_catalog.analytics.user_activities

================================================================================
PIPELINE SUMMARY
================================================================================
Total topics: 3
Pipelines created: 3
Failed: 0

✓ All pipelines created successfully!
Flink jobs are now running...
```

---

## Step 6: Verify Data

### Option 1: Check Flink Web UI
```bash
# Open in browser
http://localhost:8081

# Navigate to:
# - Jobs → Running Jobs
# - Task Managers
# - Job Manager
```

### Option 2: Check MinIO Console
```bash
# Open in browser
http://localhost:9001

# Login credentials:
# Username: admin
# Password: password

# Navigate to:
# Buckets → warehouse → analytics
# You should see folders for:
# - orders/
# - bid_events/
# - user_activities/
```

### Option 3: Check Docker Logs
```bash
# Flink JobManager logs
docker logs flink-jobmanager -f

# Flink TaskManager logs
docker logs flink-taskmanager -f

# Kafka logs
docker logs kafka -f
```

---

## Step 7: Query the Data (Optional)

You can query the Iceberg tables using tools like:
- Apache Spark with Iceberg support
- Trino/Presto
- DuckDB with Iceberg extension

Example with DuckDB:
```sql
-- Install Iceberg extension
INSTALL iceberg;
LOAD iceberg;

-- Query orders table
SELECT * FROM iceberg_scan('s3://warehouse/analytics/orders')
LIMIT 10;

-- Query bid events with is_mobile filter
SELECT event_name, platform, is_mobile, COUNT(*) as count
FROM iceberg_scan('s3://warehouse/analytics/bid_events')
GROUP BY event_name, platform, is_mobile;

-- Query user activities
SELECT event_type, device_type, is_mobile, COUNT(*) as count
FROM iceberg_scan('s3://warehouse/analytics/user_activities')
GROUP BY event_type, device_type, is_mobile;
```

---

## 🎯 What Just Happened?

1. **Producer** generated 100 events for each of 3 topics
2. **Kafka** received and stored the events
3. **Flink** consumed events from all 3 topics simultaneously
4. **Transformations** were applied:
   - Orders: Timestamp conversion
   - Bids: Platform detection → `is_mobile` field
   - Users: Device detection → `is_mobile` field
5. **Iceberg** tables were created and populated in MinIO

---

## 🧹 Cleanup

```bash
# Stop all services
cd infra
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v

# Remove built artifacts
cd ..
mvn clean
```

---

## 🔄 Run Again

```bash
# Quick restart
cd infra
docker-compose up -d
cd ../producer
python cli.py produce all --count 100
cd ..
python streaming_job.py
```

---

## 🐛 Troubleshooting

### Issue: "Connection refused" to Kafka
```bash
# Wait for Kafka to be ready
docker logs kafka | grep "started (kafka.server.KafkaServer)"

# Or use health check
docker-compose ps
```

### Issue: "JAR not found"
```bash
# Rebuild the project
mvn clean package

# Verify JAR exists
ls -lh target/pyflink-dependencies.jar
```

### Issue: "Module not found" errors
```bash
# Install Python dependencies
cd producer
pip install -r requirements.txt

cd ../flink
pip install -r requirements.txt
```

### Issue: Flink job not processing data
```bash
# Check Flink logs
docker logs flink-jobmanager -f

# Check if topics exist in Kafka
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Check if data is in Kafka
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders.created.v1 \
  --from-beginning \
  --max-messages 5
```

---

## 📚 Next Steps

- Read `IMPLEMENTATION_SUMMARY.md` for detailed architecture
- Read `AWS_DEPLOYMENT.md` for AWS deployment
- Add your own topics and transformations
- Explore the Flink Web UI
- Query the Iceberg tables

---

**Happy Streaming! 🚀**
