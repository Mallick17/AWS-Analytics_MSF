# 🚀 Setup and Deployment Guide

This guide details how to set up the `python-datastream-ag` project for both **Local Development** (Docker) and **AWS Production** (Managed Flink with S3 Tables).

**IMPORTANT**: This pipeline writes data to **AWS S3 Tables (Iceberg)** in production mode, NOT standard S3 buckets. S3 Tables provide native Iceberg support with enhanced performance and management features.

---

## 🛠️ 1. Local Development Setup

Run the pipeline locally using Docker containers for Kafka, Flink, and an Iceberg REST Catalog. In local mode, data is output to console for testing.

### ✅ Prerequisites
*   **Docker & Docker Compose** installed and running.
*   **Python 3.8+** installed.
*   **AWS Credentials** (Access Key & Secret) with permissions for S3 Tables and standard S3.
*   **S3 Tables Bucket** created for production Iceberg data (e.g., ARN format).
*   **Standard S3 Bucket** for Flink checkpoints/savepoints (e.g., `s3://my-flink-state-bucket`).

### 📝 Step 1: Clone and Configure

1.  **Navigate to the project root**:
    ```bash
    cd python-datastream-ag
    ```

2.  **Configure Environment Variables**:
    Create a `.env` file from the example and add your credentials.
    ```bash
    cp .env.example .env
    ```

3.  **Edit `.env`**:
    Open `.env` in your editor and fill in the details:
    ```ini
    AWS_ACCESS_KEY_ID=AKIA...
    AWS_SECRET_ACCESS_KEY=secret...
    AWS_REGION=ap-south-1
    
    # S3 Tables ARN for production (Iceberg data)
    S3_WAREHOUSE=arn:aws:s3tables:ap-south-1:123456789012:bucket/your-s3tables-bucket
    
    # Standard S3 bucket for Flink state and local development
    S3_WAREHOUSE_BUCKET=s3://your-flink-state-bucket
    
    FLINK_ENV=local
    ```

    **CRITICAL**: The `S3_WAREHOUSE` must be an S3 Tables ARN, not a standard S3 bucket path!

### 🐳 Step 2: Start Local Infrastructure

1.  **Start Docker Containers**:
    This starts Kafka, Zookeeper, Flink JobManager/TaskManager, and the Iceberg REST Catalog.
    ```bash
    ./infra/scripts/start.sh
    ```

2.  **Verify Status**:
    Ensure all containers are in `Up` state.
    ```bash
    ./infra/scripts/health-check.sh
    ```

### ⚙️ Step 3: Initialize Resources

1.  **Create Kafka Topics**:
    This script waits for Kafka to be ready and creates `bid-events` and `user-events`.
    ```bash
    ./infra/scripts/setup_local.sh
    ```

### ▶️ Step 4: Run the Flink Job

Choose one of the following methods to run the job:

#### Method 1: Interactive Mode (job runs in foreground)
```bash
./infra/scripts/run-job.sh
```
*   Result: You see logs in real-time, press Ctrl+C to stop monitoring

#### Method 2: Background Mode (job runs in background)
```bash
./infra/scripts/run-job-background.sh
```
*   Result: Job starts and runs continuously in background, terminal is free

#### Method 3: Manual Execution (for debugging)
```bash
docker-compose -f infra/docker-compose.yml exec jobmanager python /opt/flink/usrlib/streaming_job.py
```
*   Result: Direct execution inside the container

### ⚡ Step 5: Produce Test Data

1.  **Open a NEW terminal window**.
2.  **Navigate to the project root**:
    ```bash
    cd python-datastream
    ```
3.  **Install Producer Dependencies**:
    ```bash
    pip install -r tests/local_producer/requirements.txt
    ```
4.  **Run the Producer**:
    This generates synthetic data and sends it to your local Kafka.
    ```bash
    python tests/local_producer/producer.py
    ```
    *   *Result: Logs showing events being sent to `bid-events` and `user-events`.*

### 🔍 Step 6: Verify Data Output

**Local Development Mode:**
*   Data is printed to the Flink console for verification
*   Check the job logs: `docker-compose logs -f jobmanager`

**Production Mode (FLINK_ENV=production):**
1.  **Check your AWS S3 Tables bucket** (using the ARN specified in S3_WAREHOUSE)
2.  Navigate to your S3 Tables bucket in the AWS Console
3.  You should see Iceberg table structures with:
    *   Table metadata and schema information
    *   Data files in Iceberg format (optimized for analytics)

**Note**: S3 Tables automatically manages Iceberg table metadata, file organization, and optimization - you don't see raw files like in standard S3.

### 🛑 Step 7: Cleanup

1.  **Stop Containers**:
    ```bash
    ./infra/scripts/stop.sh
    ```

---

## ☁️ 2. AWS Production Deployment

Deploy the application to AWS Managed Flink (Kinesis Data Analytics).

### ✅ Prerequisites
*   **Maven 3.x** installed (`mvn -version`).
*   **AWS CLI** configured (`aws configure`).
*   **S3 Bucket** for application artifacts (code).
*   **AWS Managed Flink Application** created in AWS Console.

### 📦 Step 1: Build the Project

1.  **Navigate to the project root**:
    ```bash
    cd python-datastream
    ```

2.  **Build and Package**:
    Run Maven to download Java dependencies and package the Python code into a deployment ZIP.
    ```bash
    mvn clean package
    ```
    *   *Output*: `target/pyflink-s3tables-app.zip`
    *   *This ZIP contains: Python code, Config files, and `lib/pyflink-dependencies.jar`.*

### ☁️ Step 2: Upload Artifact to S3

1.  **Upload the ZIP file**:
    Replace `your-code-bucket` with your actual S3 bucket name.
    ```bash
    aws s3 cp target/pyflink-s3tables-app.zip s3://your-code-bucket/flink-app/pyflink-s3tables-app.zip
    ```

### 🔧 Step 3: Configure AWS Managed Flink

1.  **Log in to AWS Console**.
2.  Navigate to **Managed Service for Apache Flink**.
3.  Select your application.
4.  Click **Configure**.

#### Update Code Location
5.  Under **Application code location**:
    *   **Bucket**: `your-code-bucket`
    *   **Path to S3 object**: `flink-app/pyflink-s3tables-app.zip`

#### Configure Runtime Properties
6.  Under **Runtime properties**, ensure the following Group ID exists or add it:
    *   **Group ID**: `kinesis.analytics.flink.run.options`
    *   **Key**: `python` | **Value**: `streaming_job.py`
    *   **Key**: `jarfile` | **Value**: `lib/pyflink-dependencies.jar`

7.  Click **Update** to save changes.

### 🚀 Step 4: Run the Application

1.  Once the application status is **Ready**, click **Run**.
2.  Select **Run without snapshot** (for fresh start) or **Restore from snapshot**.
3.  The application status will change to **Starting** and then **Running**.

### 📊 Step 5: Monitor

1.  Click **Open Apache Flink Dashboard** to view job graphs and metrics.
2.  Check **CloudWatch Logs** for application output (stdout/stderr).
