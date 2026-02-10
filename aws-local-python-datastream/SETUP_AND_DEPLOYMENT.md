# 🚀 Flink + Kafka → AWS S3 Tables Pipeline (Fully Containerized)

This guide sets up a **fully containerized** local development environment for a streaming data pipeline using Apache Flink, Apache Kafka, and AWS S3 Tables (Iceberg format).

## 🏗️ Architecture Overview

```
┌─────────────────────────────── CONTAINERIZED LOCAL ENV ──────────────────────────────┐
│                                                                                        │
│  ┌─────────────┐    ┌─────────────┐    ┌──────────────┐    ┌──────────────┐        │
│  │   Kafka     │    │  Producer   │    │    Flink     │    │   Flink      │        │
│  │ (Container) │◄───│(Container)  │───▶│ JobManager   │───▶│ TaskManager  │        │
│  │             │    │             │    │ (Container)  │    │ (Container)  │        │
│  └─────────────┘    └─────────────┘    └──────────────┘    └──────────────┘        │
│                                                  │                                   │
└──────────────────────────────────────────────────┼───────────────────────────────────┘
                                                   │ AWS Credentials
                                                   ▼
                                  ┌────────────────────────────────┐
                                  │     AWS S3 Tables (Iceberg)    │
                                  │ arn:aws:s3tables:<region>:...  │
                                  └────────────────────────────────┘
```

**Key Points:**
- 🐳 **Everything runs in containers** - no host Python dependencies
- ⚡ **Modular producer** with CLI for flexible event generation  
- 🔄 **Local mode**: Outputs to console for easy testing
- ☁️ **Production mode**: Writes to AWS S3 Tables
- 🛠️ **Simple scripts** for managing the entire environment

---

## 🛠️ 1. Local Development Setup

Run the pipeline locally using Docker containers for Kafka and Flink, writing to **AWS S3 Tables** in the cloud.

### ✅ Prerequisites

*   **Docker & Docker Compose** installed and running
*   **Python 3.8+** installed
*   **AWS Credentials** with permissions for S3 Tables
*   **AWS S3 Tables Bucket** created (e.g., `arn:aws:s3tables:ap-south-1:123456789:bucket/my-bucket`)

### 📝 Step 1: Clone and Configure

1.  **Navigate to the project root**:
    ```bash
    cd python-datastream
    ```

2.  **Create environment file from template**:
    ```bash
    cp .env.example .env
    ```

3.  **Edit `.env`** with your AWS credentials:
    ```ini
    # AWS Credentials (for S3 Tables access)
    AWS_ACCESS_KEY_ID=AKIA...
    AWS_SECRET_ACCESS_KEY=your-secret-key
    AWS_REGION=ap-south-1

    # S3 Tables Configuration (use ARN format)
    S3_WAREHOUSE=arn:aws:s3tables:ap-south-1:123456789012:bucket/your-s3tables-bucket

    # Flink/Iceberg Configuration
    FLINK_ENV=local
    ICEBERG_NAMESPACE=analytics
    
    # Kafka (auto-configured for Docker)
    KAFKA_BOOTSTRAP_SERVERS=kafka:29092
    ```

### 🐳 Step 2: Start Local Infrastructure

> **Note**: For local Docker development:
> - ✅ **Required JARs**: Only basic Kafka and Iceberg connectors are pre-installed
> - ✅ **Console Output**: Transformed data is printed to console (not S3 Tables)
> - ✅ **Pipeline Testing**: Tests Kafka → Flink transformations → Output flow
> - ☁️ **S3 Tables**: Only available in AWS Managed Flink (production)
> - 📦 **Maven Build**: Only needed for AWS production deployment (`pyflink-dependencies.jar`)

#### Option A: Quick Start (Recommended)

```bash
# Make scripts executable (first time only)
chmod +x infra/scripts/*.sh

# Start all services
./infra/scripts/start.sh
```

#### Option B: With Docker Image Rebuild

```bash
# Build images from scratch (first time or after Dockerfile changes)
./infra/scripts/start.sh --build

# Or rebuild without cache (if you encounter issues)
./infra/scripts/start.sh --no-cache
```

#### Option C: With Debugging Tools

```bash
# Start with Kafka UI for debugging
./infra/scripts/start.sh --debug
```

> **Note**: The first build may take 5-10 minutes as it installs Python, PyFlink, and downloads Iceberg JARs.

### ⚙️ Step 3: Verify Services

```bash
# Check health of all services
./infra/scripts/health-check.sh
```

Expected output:
```
Container Status:
NAME              STATUS                   PORTS
kafka             Up (healthy)             9092, 9093
flink-jobmanager  Up (healthy)             8081
flink-taskmanager Up                       

Service Health:
  Kafka:            ✓ Healthy
                    Topics: orders.created.v1, bid-events, user-events
  Flink JobManager: ✓ Healthy
  Flink TaskManager: ✓ Healthy (1 registered)
```

### 🌐 Access Points

| Service | URL | Description |
|---------|-----|-------------|
| Flink UI | http://localhost:8081 | Job monitoring, metrics |
| Kafka UI | http://localhost:8080 | Topic browser (with `--debug`) |
| Kafka Bootstrap | localhost:9092 | Producer/consumer connections |

### ▶️ Step 4: Run the Flink Job

```bash
# Execute the streaming job inside the Flink container
docker-compose -f infra/docker-compose.yml --env-file .env exec jobmanager \
  python /opt/flink/usrlib/streaming_job.py
```

The job will start and wait for events from Kafka topics.

### ⚡ Step 5: Produce Test Data

producer/
├── __init__.py              # Package initialization
├── requirements.txt         # Dependencies (kafka-python, faker, click, etc.)
├── config.yaml             # Configuration settings
├── cli.py                  # CLI entry point
├── generators/             # Event generators
│   ├── bid_events.py       # Bid/ride events generator
│   └── user_events.py      # User activity events generator
└── kafka/                  # Kafka utilities
    └── client.py           # Kafka client manager

**Option A: From Host Machine** (Recommended)
```bash
# Install dependencies (first time only)
pip3 install -r requirements.txt

# Run the modular producer
python -m producer.cli --local --topic both
python -m producer.cli --local --topic bid-events --count 50 --rate 2.0
python -m producer.cli --local --topic user-events --count 100
```

**Option B: Inside Docker Container**
```bash
# Run producer container with mounted script
docker-compose -f infra/docker-compose.yml --env-file .env run --rm producer

# Or start as background service
docker-compose -f infra/docker-compose.yml --env-file .env --profile producer up -d

# View producer logs
docker logs -f kafka-producer
```

**Legacy Producer (still available):**
```bash
python sample_gen.py --local --topic both  # Old monolithic script
```

**Producer Options:**
```bash
# Modular producer (recommended)
python -m producer.cli --local --topic bid-events    # Only bid events
python -m producer.cli --local --topic user-events   # Only user events  
python -m producer.cli --local --topic both          # Alternating events
python -m producer.cli --local --topic both --rate 10 --count 100  # 10 events/sec, stop after 100

# Legacy producer (simple script)
python sample_gen.py --local --topic bid-events   # Only bid events
python sample_gen.py --local --topic user-events  # Only user events
python sample_gen.py --local --topic both         # Alternating events
python sample_gen.py --local --topic both --rate 10  # 10 events/sec
python sample_gen.py --local --topic both --count 100  # Stop after 100
```

### 🔍 Step 6: Verify Data in S3 Tables

1. **AWS Console**: Navigate to S3 Tables and check your bucket
2. **AWS CLI**:
   ```bash
   aws s3tables list-tables --table-bucket-arn "arn:aws:s3tables:ap-south-1:123456789:bucket/your-bucket"
   ```

### 🛑 Step 7: Stop Services

```bash
# Stop all containers (preserves data)
./infra/scripts/stop.sh

# Stop and remove all data (clean slate)
./infra/scripts/stop.sh --clean
```

### 📜 Script Reference

| Script | Description |
|--------|-------------|
| `./infra/scripts/start.sh` | Build and start all services |
| `./infra/scripts/start.sh --build` | Rebuild Docker images first |
| `./infra/scripts/start.sh --no-cache` | Rebuild without Docker cache |
| `./infra/scripts/start.sh --debug` | Include Kafka UI |
| `./infra/scripts/stop.sh` | Stop all containers |
| `./infra/scripts/stop.sh --clean` | Stop and remove volumes |
| `./infra/scripts/health-check.sh` | Check service health |

### � Troubleshooting

**Issue: Docker build fails with `pemja` error**
```
Include folder should be at '/opt/java/openjdk/include' but doesn't exist
```
*Solution*: This is fixed in the current Dockerfile which installs the full JDK.

**Issue: Kafka connection refused**
```bash
# Wait a few more seconds for Kafka to be ready
sleep 30
./infra/scripts/health-check.sh
```

**Issue: View container logs**
```bash
# All logs
docker-compose -f infra/docker-compose.yml --env-file .env logs -f

# Specific service
docker-compose -f infra/docker-compose.yml --env-file .env logs -f jobmanager
docker-compose -f infra/docker-compose.yml --env-file .env logs -f kafka
```

---

## 🎛️ Producer Usage (Fully Containerized)

The modular producer runs entirely in containers with no host dependencies.

### Default Producer Service

```bash
# Start producer with default settings (both topics, 100 events, 2 events/sec)
docker-compose -f infra/docker-compose.yml --env-file .env --profile producer up -d

# View logs
docker logs kafka-producer -f

# Stop producer
docker-compose -f infra/docker-compose.yml --env-file .env stop producer
```

### Custom Producer Commands

Use `docker-compose exec` to run custom producer commands:

```bash
# Produce only order events
docker-compose -f infra/docker-compose.yml --env-file .env exec kafka-producer \
  python -m producer.cli --topic orders.created.v1 --count 50 --rate 1

# Produce only bid events  
docker-compose -f infra/docker-compose.yml --env-file .env exec kafka-producer \
  python -m producer.cli --topic bid-events --count 30 --rate 5

# Produce to both topics with custom settings
docker-compose -f infra/docker-compose.yml --env-file .env exec kafka-producer \
  python -m producer.cli --topic both --count 200 --rate 3
```

### One-off Producer Runs

```bash
# Run producer as a one-time container
docker run --rm --network infra_flink-network \
  -v $(pwd):/opt/flink/usrlib \
  -w /opt/flink/usrlib \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  flink:1.19-python \
  bash -c "pip install -r producer/requirements.txt && python -m producer.cli --topic both --count 10"
```

### Producer CLI Options

```
Options:
  --topic [orders.created.v1|bid-events|both]  Topic to produce to
  --count INTEGER                              Number of events to produce
  --rate FLOAT                                Rate of production (events/sec)
  --help                                      Show help message

Examples:
  python -m producer.cli --topic orders.created.v1 --count 50
  python -m producer.cli --topic bid-events --rate 5 --count 100
  python -m producer.cli --topic both --rate 2 --count 200
```

---

## 🔧 Advanced Usage

### Start with Debugging Tools

```bash
# Start with Kafka UI for topic inspection
./infra/scripts/start.sh --debug

# Access Kafka UI
open http://localhost:8080
```

### Rebuild Containers

```bash
# Force rebuild Docker images (after code changes)
./infra/scripts/start.sh --build

# Rebuild without Docker cache
./infra/scripts/start.sh --no-cache
```

### Health Checks

```bash
# Check all service health
./infra/scripts/health-check.sh
```

Expected output:
```
Container Status:
NAME              STATUS                   PORTS
kafka             Up (healthy)             9092, 9093
flink-jobmanager  Up (healthy)             8081
flink-taskmanager Up                       

Service Health:
  Kafka:            ✓ Healthy
                    Topics: orders.created.v1, bid-events, user-events
  Flink JobManager: ✓ Healthy
  Flink TaskManager: ✓ Healthy (1 registered)
```

### View Logs

```bash
# All service logs
docker-compose -f infra/docker-compose.yml --env-file .env logs -f

# Specific service logs
docker logs flink-jobmanager -f
docker logs flink-taskmanager -f
docker logs kafka -f
docker logs kafka-producer -f
```

---

## 🌐 Access Points

| Service | URL | Description |
|---------|-----|-------------|
| Flink Web UI | http://localhost:8081 | Job monitoring, metrics, task managers |
| Kafka UI | http://localhost:8080 | Topic browser (with `--debug` flag) |
| Kafka Bootstrap | localhost:9092 | External access for producers/consumers |

---

## 🔍 Troubleshooting

### Common Issues

**Container build fails**
```bash
# Clean build cache and retry
docker system prune -a
./infra/scripts/start.sh --no-cache
```

**Kafka connection issues**
```bash
# Check Kafka health
./infra/scripts/health-check.sh

# Wait for Kafka startup
sleep 30 && ./infra/scripts/health-check.sh
```

**Producer not connecting**
```bash
# Check producer logs
docker logs kafka-producer -f

# Check network connectivity
docker network ls
docker network inspect infra_flink-network
```

**Flink job fails to start**
```bash
# Check JobManager logs
docker logs flink-jobmanager -f

# Check dependencies in container
docker-compose exec jobmanager pip list
```

### Debug Commands

```bash
# Enter any container for debugging
docker-compose -f infra/docker-compose.yml --env-file .env exec jobmanager bash
docker-compose -f infra/docker-compose.yml --env-file .env exec kafka bash

# Check container resources
docker stats

# View all container environments
docker-compose -f infra/docker-compose.yml --env-file .env config
```

---

## 📦 AWS Production Deployment

For production deployment to AWS Managed Flink:

### Build Production Package

```bash
# Build Maven package (includes all dependencies)
mvn clean package

# Upload to S3
aws s3 cp target/pyflink-s3tables-app.zip s3://your-code-bucket/flink-app/
```

### AWS Managed Flink Configuration

1. **Application code location**: `s3://your-code-bucket/flink-app/pyflink-s3tables-app.zip`
2. **Runtime properties**:
   - Group ID: `kinesis.analytics.flink.run.options`
   - `python`: `streaming_job.py`  
   - `jarfile`: `lib/pyflink-dependencies.jar`

---

## 📋 Quick Reference

```bash
# Complete workflow
cd python-datastream
chmod +x infra/scripts/*.sh
./infra/scripts/start.sh                    # Start environment
./infra/scripts/health-check.sh             # Verify health

# Deploy job
docker-compose -f infra/docker-compose.yml --env-file .env exec jobmanager \
  python /opt/flink/usrlib/streaming_job.py

# Start producer
docker-compose -f infra/docker-compose.yml --env-file .env --profile producer up -d

# Monitor
docker logs flink-taskmanager -f            # See output
open http://localhost:8081                  # Flink UI

# Custom producer
docker-compose -f infra/docker-compose.yml --env-file .env exec kafka-producer \
  python -m producer.cli --topic both --count 100 --rate 2

# Cleanup
./infra/scripts/stop.sh --clean             # Stop and clean
```

---

## 🗂️ Project Structure

```
python-datastream/
├── .env                           # Environment variables
├── streaming_job.py               # Main Flink job  
├── requirements.txt               # Local dev dependencies
├── producer/                      # Modular event producer
│   ├── cli.py                     # CLI interface
│   ├── generators/                # Event generators
│   ├── kafka/                     # Kafka utilities  
│   └── requirements.txt           # Producer dependencies
├── infra/                         # Docker infrastructure
│   ├── docker-compose.yml         # Service definitions
│   ├── Dockerfile.flink-python    # Flink + Python image
│   └── scripts/                   # Management scripts
│       ├── start.sh               # Start environment
│       ├── stop.sh                # Stop environment
│       └── health-check.sh        # Health verification
└── sample_gen.py                  # Legacy producer (deprecated)
```

All components run in containers - no host Python dependencies required! 🎉
