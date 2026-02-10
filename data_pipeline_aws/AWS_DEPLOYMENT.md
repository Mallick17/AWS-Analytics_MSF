# AWS Deployment Guide for data_pipeline_aws

This guide provides step-by-step instructions for deploying the data pipeline to both **local Docker** and **AWS Managed Flink**.

---

## 🏗️ Architecture Overview

### Local Development
- **Kafka**: Local Kafka broker for event streaming
- **Flink**: Custom Docker image with pre-installed JAR dependencies
- **Iceberg**: Hadoop catalog writing to local filesystem (`./warehouse/`)
- **Storage**: Parquet files in `./warehouse/analytics.db/orders/`

### AWS Production
- **Kafka**: AWS MSK (Managed Streaming for Kafka) with IAM authentication
- **Flink**: AWS Managed Flink (Kinesis Data Analytics)
- **Iceberg**: S3 Tables catalog writing to AWS S3 Tables bucket
- **Storage**: Parquet files in S3 Tables bucket

---

## 📋 Prerequisites

### For Local Development
- Docker & Docker Compose installed
- Maven 3.x installed (`mvn -version`)
- Python 3.8+ (for producer)

### For AWS Deployment
- AWS CLI configured (`aws configure`)
- AWS account with permissions for:
  - S3 Tables
  - Managed Flink (Kinesis Data Analytics)
  - MSK (Managed Streaming for Kafka)
  - IAM roles

---

## 🚀 Local Development Setup

### Step 1: Build JAR Dependencies

```bash
cd data_pipeline_aws
mvn clean package
```

**Output**: `target/pyflink-dependencies.jar`

### Step 2: Build Docker Image

```bash
docker-compose -f infra/docker-compose.yml build
```

This builds a custom Flink image with the JAR pre-installed.

### Step 3: Start Infrastructure

```bash
docker-compose -f infra/docker-compose.yml up -d
```

**Services started**:
- Kafka (port 9092)
- Flink JobManager (port 8081)
- Flink TaskManager

### Step 4: Verify Services

```bash
# Check container status
docker-compose -f infra/docker-compose.yml ps

# Check Flink UI
# Open browser: http://localhost:8081
```

### Step 5: Run the Flink Job

```bash
# Execute job inside JobManager container
docker-compose -f infra/docker-compose.yml exec jobmanager \
  python /opt/flink/usrlib/jobs/order_ingest/job.py
```

### Step 6: Produce Test Events

```bash
# Install producer dependencies
cd producer
pip install -r requirements.txt

# Generate and send events
python cli.py produce orders --rate 10 --count 100
```

### Step 7: Verify Data

```bash
# Check warehouse directory
ls -la warehouse/analytics.db/orders/

# You should see:
# - metadata/ (Iceberg metadata files)
# - data/ (Parquet data files)
```

### Step 8: Query Data (Optional)

Using DuckDB:
```bash
pip install duckdb

duckdb
```

```sql
-- Query Parquet files
SELECT * FROM read_parquet('warehouse/analytics.db/orders/data/*.parquet') LIMIT 10;
```

### Step 9: Stop Infrastructure

```bash
docker-compose -f infra/docker-compose.yml down
```

---

## ☁️ AWS Production Deployment

### Step 1: Set Up AWS Resources

#### 1.1 Create S3 Tables Bucket

```bash
aws s3tables create-table-bucket \
  --name my-analytics-bucket \
  --region ap-south-1
```

**Note the ARN**: `arn:aws:s3tables:ap-south-1:123456789012:bucket/my-analytics-bucket`

#### 1.2 Create MSK Cluster (if not exists)

Follow AWS MSK documentation to create a cluster with IAM authentication enabled.

**Note the bootstrap servers**: `b-1.msk-cluster.kafka.ap-south-1.amazonaws.com:9098`

#### 1.3 Create IAM Role for Managed Flink

Create a role with policies for:
- S3 Tables access
- MSK access
- CloudWatch Logs

### Step 2: Build Deployment Package

```bash
cd data_pipeline_aws
mvn clean package
```

**Output**: `target/data-pipeline-aws.zip`

### Step 3: Upload to S3

```bash
# Create S3 bucket for code artifacts (if not exists)
aws s3 mb s3://my-flink-artifacts

# Upload deployment package
aws s3 cp target/data-pipeline-aws.zip \
  s3://my-flink-artifacts/flink-apps/data-pipeline-aws.zip
```

### Step 4: Create Managed Flink Application

#### Via AWS Console:

1. Navigate to **Managed Service for Apache Flink**
2. Click **Create application**
3. Configure:
   - **Application name**: `data-pipeline-aws`
   - **Runtime**: Apache Flink 1.19
   - **Access permissions**: Select the IAM role created in Step 1.3

4. **Configure application code**:
   - **Code location**: S3
   - **S3 bucket**: `my-flink-artifacts`
   - **Path**: `flink-apps/data-pipeline-aws.zip`

5. **Runtime properties**:
   Add the following property groups:

   **Group ID**: `kinesis.analytics.flink.run.options`
   - Key: `python` | Value: `streaming_job.py`
   - Key: `jarfile` | Value: `lib/pyflink-dependencies.jar`

6. **Environment properties**:
   - `DEPLOYMENT_ENV` = `aws`
   - `AWS_S3_TABLES_ARN` = `arn:aws:s3tables:ap-south-1:123456789012:bucket/my-analytics-bucket`
   - `AWS_REGION` = `ap-south-1`
   - `AWS_MSK_BOOTSTRAP_SERVERS` = `b-1.msk-cluster.kafka.ap-south-1.amazonaws.com:9098`
   - `KAFKA_TOPIC_ORDERS` = `orders.created.v1`
   - `KAFKA_CONSUMER_GROUP` = `flink-order-ingest`
   - `ICEBERG_NAMESPACE` = `analytics`

7. Click **Create application**

#### Via AWS CLI:

```bash
aws kinesisanalyticsv2 create-application \
  --application-name data-pipeline-aws \
  --runtime-environment FLINK-1_19 \
  --service-execution-role arn:aws:iam::123456789012:role/FlinkExecutionRole \
  --application-configuration '{
    "ApplicationCodeConfiguration": {
      "CodeContent": {
        "S3ContentLocation": {
          "BucketARN": "arn:aws:s3:::my-flink-artifacts",
          "FileKey": "flink-apps/data-pipeline-aws.zip"
        }
      },
      "CodeContentType": "ZIPFILE"
    },
    "EnvironmentProperties": {
      "PropertyGroups": [
        {
          "PropertyGroupId": "kinesis.analytics.flink.run.options",
          "PropertyMap": {
            "python": "streaming_job.py",
            "jarfile": "lib/pyflink-dependencies.jar"
          }
        },
        {
          "PropertyGroupId": "EnvironmentProperties",
          "PropertyMap": {
            "DEPLOYMENT_ENV": "aws",
            "AWS_S3_TABLES_ARN": "arn:aws:s3tables:ap-south-1:123456789012:bucket/my-analytics-bucket",
            "AWS_REGION": "ap-south-1",
            "AWS_MSK_BOOTSTRAP_SERVERS": "b-1.msk-cluster.kafka.ap-south-1.amazonaws.com:9098",
            "KAFKA_TOPIC_ORDERS": "orders.created.v1",
            "KAFKA_CONSUMER_GROUP": "flink-order-ingest",
            "ICEBERG_NAMESPACE": "analytics"
          }
        }
      ]
    }
  }'
```

### Step 5: Start the Application

#### Via AWS Console:
1. Select your application
2. Click **Run**
3. Choose **Run without snapshot** (for first run)

#### Via AWS CLI:
```bash
aws kinesisanalyticsv2 start-application \
  --application-name data-pipeline-aws \
  --run-configuration '{}'
```

### Step 6: Monitor the Application

#### CloudWatch Logs:
```bash
# View logs
aws logs tail /aws/kinesis-analytics/data-pipeline-aws --follow
```

#### Flink Dashboard:
1. In AWS Console, select your application
2. Click **Open Apache Flink Dashboard**

### Step 7: Verify Data in S3 Tables

#### Via AWS Console:
1. Navigate to **S3 Tables**
2. Select your bucket
3. Browse to `analytics/orders/`
4. Verify Parquet files are being created

#### Via AWS CLI:
```bash
aws s3tables list-tables \
  --table-bucket-arn arn:aws:s3tables:ap-south-1:123456789012:bucket/my-analytics-bucket \
  --namespace analytics
```

---

## 🔄 Updating the Application

### Update Code

1. Make changes to Python code
2. Rebuild: `mvn clean package`
3. Upload new ZIP: `aws s3 cp target/data-pipeline-aws.zip s3://my-flink-artifacts/flink-apps/data-pipeline-aws.zip`
4. In AWS Console:
   - Stop the application
   - Click **Configure**
   - Update code location (same path, new version)
   - Click **Update**
   - Start the application

---

## 🧹 Cleanup

### Local:
```bash
docker-compose -f infra/docker-compose.yml down -v
rm -rf warehouse/
```

### AWS:
```bash
# Stop application
aws kinesisanalyticsv2 stop-application --application-name data-pipeline-aws

# Delete application
aws kinesisanalyticsv2 delete-application --application-name data-pipeline-aws

# Delete S3 artifacts
aws s3 rm s3://my-flink-artifacts/flink-apps/data-pipeline-aws.zip
```

---

## 📊 Monitoring and Troubleshooting

### Local Development

**Check Flink logs**:
```bash
docker-compose -f infra/docker-compose.yml logs -f jobmanager
docker-compose -f infra/docker-compose.yml logs -f taskmanager
```

**Check Kafka topics**:
```bash
docker-compose -f infra/docker-compose.yml exec kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders.created.v1 --from-beginning
```

### AWS Production

**CloudWatch Metrics**:
- Monitor: `KPUs`, `Downtime`, `Backpressure`

**CloudWatch Logs**:
- Check for errors in application logs
- Look for JAR loading issues
- Verify environment variables

**Common Issues**:
1. **JAR not found**: Verify `lib/pyflink-dependencies.jar` is in ZIP
2. **Import errors**: Check Python path in `streaming_job.py`
3. **Kafka connection**: Verify MSK bootstrap servers and IAM permissions
4. **S3 Tables access**: Verify IAM role has S3 Tables permissions

---

## 📝 Notes

- **Local** uses Hadoop catalog with filesystem storage
- **AWS** uses S3 Tables catalog with S3 storage
- Both environments use the same Flink job code
- Environment detection is automatic via `DEPLOYMENT_ENV` variable
