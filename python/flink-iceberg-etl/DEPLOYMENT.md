# PyFlink to Iceberg Deployment Guide for AWS Managed Flink

## Overview
This guide provides complete instructions to deploy a Python-based Flink application that reads from MSK Kafka and writes to S3 Tables (Iceberg) using AWS Managed Service for Apache Flink.

## Architecture
```
MSK Kafka Topic → PyFlink (ETL) → S3 Tables (Iceberg)
```

## Prerequisites

### 1. AWS Services
- AWS Managed Service for Apache Flink (formerly Kinesis Data Analytics)
- Amazon MSK (Managed Streaming for Apache Kafka)
- AWS Glue Data Catalog
- S3 Tables
- IAM Roles with appropriate permissions

### 2. Local Development
- Python 3.9 or 3.10 (AWS Managed Flink supports these versions)
- pip
- AWS CLI configured

## Step 1: Prepare Your Application Package

### Directory Structure
```
flink-iceberg-etl/
├── main.py                    # Your main application
├── requirements.txt           # Python dependencies
├── .pyiceberg.yaml           # PyIceberg configuration (optional)
└── lib/                      # Additional JAR files (if needed)
    ├── iceberg-aws-bundle-1.4.3.jar
    ├── flink-sql-connector-kafka-1.18.0.jar
    └── aws-msk-iam-auth-1.1.6.jar
```

### Create Application Package

```bash
# Create a directory for your application
mkdir flink-iceberg-etl
cd flink-iceberg-etl

# Copy your Python files
cp main.py .
cp requirements.txt .
cp .pyiceberg.yaml .

# Create lib directory for JAR dependencies
mkdir lib
```

### Download Required JARs

```bash
cd lib

# Flink Kafka Connector
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.18.0/flink-sql-connector-kafka-1.18.0.jar

# AWS MSK IAM Auth
wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.6/aws-msk-iam-auth-1.1.6-all.jar

# Iceberg AWS Bundle
wget https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.3/iceberg-aws-bundle-1.4.3.jar

# Iceberg Flink Runtime (for Flink 1.18)
wget https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.4.3/iceberg-flink-runtime-1.18-1.4.3.jar

cd ..
```

### Package Application

```bash
# Create ZIP file for AWS Managed Flink
zip -r flink-iceberg-etl.zip main.py requirements.txt .pyiceberg.yaml lib/
```

## Step 2: Upload to S3

```bash
# Create S3 bucket for application code
aws s3 mb s3://your-flink-apps-bucket --region ap-south-1

# Upload application package
aws s3 cp flink-iceberg-etl.zip s3://your-flink-apps-bucket/applications/

# Upload JAR files separately (optional, for reference)
aws s3 cp lib/ s3://your-flink-apps-bucket/jars/ --recursive
```

## Step 3: Create IAM Role for Flink Application

### IAM Policy (flink-iceberg-policy.json)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::your-flink-apps-bucket/*",
        "arn:aws:s3tables:ap-south-1:149815625933:bucket/flink-transform-sink/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-flink-apps-bucket",
        "arn:aws:s3tables:ap-south-1:149815625933:bucket/flink-transform-sink"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka:DescribeCluster",
        "kafka:GetBootstrapBrokers",
        "kafka:DescribeClusterV2"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeGroup",
        "kafka-cluster:AlterGroup",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:ReadData",
        "kafka-cluster:WriteData"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
```

### Create Role

```bash
# Create IAM role
aws iam create-role \
  --role-name FlinkIcebergETLRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "kinesisanalytics.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  }'

# Attach policy
aws iam put-role-policy \
  --role-name FlinkIcebergETLRole \
  --policy-name FlinkIcebergPolicy \
  --policy-document file://flink-iceberg-policy.json
```

## Step 4: Create Flink Application via AWS Console

### Using AWS Console

1. Navigate to **Amazon Managed Service for Apache Flink**
2. Click **Create application**
3. Configure:
   - **Application name**: `flink-iceberg-etl`
   - **Runtime**: Apache Flink 1.18
   - **Language**: Python 3.10
   - **Access permissions**: Select `FlinkIcebergETLRole`

4. **Application configuration**:
   - **Application code location**: 
     - S3 bucket: `s3://your-flink-apps-bucket`
     - S3 object key: `applications/flink-iceberg-etl.zip`
   
5. **Environment properties** (Add these as runtime properties):
   ```
   Group: ApplicationProperties
   
   KAFKA_BOOTSTRAP_SERVERS = b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,b-2...
   KAFKA_TOPICS = user_events
   KAFKA_CONSUMER_GROUP = flink-python-consumer
   KAFKA_OFFSET = earliest
   S3_WAREHOUSE = arn:aws:s3tables:ap-south-1:149815625933:bucket/flink-transform-sink
   TABLE_NAMESPACE = sink
   AWS_REGION = ap-south-1
   PARALLELISM = 2
   CHECKPOINT_INTERVAL = 60000
   ```

6. **Monitoring**:
   - Enable CloudWatch Logs
   - Set log level to INFO

7. **Scaling**:
   - Parallelism: 2
   - KPUs (Kinesis Processing Units): 2

### Using AWS CLI

```bash
# Create application
aws kinesisanalyticsv2 create-application \
  --region ap-south-1 \
  --application-name flink-iceberg-etl \
  --runtime-environment FLINK-1_18 \
  --service-execution-role arn:aws:iam::149815625933:role/FlinkIcebergETLRole \
  --application-configuration '{
    "ApplicationCodeConfiguration": {
      "CodeContent": {
        "S3ContentLocation": {
          "BucketARN": "arn:aws:s3:::your-flink-apps-bucket",
          "FileKey": "applications/flink-iceberg-etl.zip"
        }
      },
      "CodeContentType": "ZIPFILE"
    },
    "EnvironmentProperties": {
      "PropertyGroups": [
        {
          "PropertyGroupId": "ApplicationProperties",
          "PropertyMap": {
            "KAFKA_BOOTSTRAP_SERVERS": "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098",
            "KAFKA_TOPICS": "user_events",
            "KAFKA_CONSUMER_GROUP": "flink-python-consumer",
            "KAFKA_OFFSET": "earliest",
            "S3_WAREHOUSE": "arn:aws:s3tables:ap-south-1:149815625933:bucket/flink-transform-sink",
            "TABLE_NAMESPACE": "sink",
            "AWS_REGION": "ap-south-1",
            "PARALLELISM": "2",
            "CHECKPOINT_INTERVAL": "60000"
          }
        }
      ]
    },
    "FlinkApplicationConfiguration": {
      "CheckpointConfiguration": {
        "ConfigurationType": "CUSTOM",
        "CheckpointingEnabled": true,
        "CheckpointInterval": 60000,
        "MinPauseBetweenCheckpoints": 30000
      },
      "MonitoringConfiguration": {
        "ConfigurationType": "CUSTOM",
        "MetricsLevel": "APPLICATION",
        "LogLevel": "INFO"
      },
      "ParallelismConfiguration": {
        "ConfigurationType": "CUSTOM",
        "Parallelism": 2,
        "ParallelismPerKPU": 1,
        "AutoScalingEnabled": false
      }
    }
  }'
```

## Step 5: Start Application

### Via Console
1. Go to your application
2. Click **Configure**
3. Click **Run**

### Via CLI

```bash
# Start application
aws kinesisanalyticsv2 start-application \
  --region ap-south-1 \
  --application-name flink-iceberg-etl
```

## Step 6: Monitor Application

### CloudWatch Logs

```bash
# View logs
aws logs tail /aws/kinesis-analytics/flink-iceberg-etl --follow
```

### CloudWatch Metrics
Monitor:
- `KPUs` - Processing units usage
- `Uptime` - Application uptime
- `InputRecords` - Records consumed from Kafka
- `OutputRecords` - Records written to Iceberg
- `CheckpointDuration` - Checkpoint performance

## Step 7: Verify Data in Iceberg

### Using Athena

```sql
-- Create external table pointing to Iceberg
CREATE TABLE user_events_iceberg
LOCATION 'arn:aws:s3tables:ap-south-1:149815625933:bucket/flink-transform-sink/sink/user_events'
TBLPROPERTIES (
  'table_type' = 'ICEBERG'
);

-- Query data
SELECT * FROM user_events_iceberg LIMIT 10;
```

### Using PyIceberg (Local)

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "s3_tables",
    **{
        "type": "glue",
        "warehouse": "arn:aws:s3tables:ap-south-1:149815625933:bucket/flink-transform-sink",
        "client.region": "ap-south-1"
    }
)

table = catalog.load_table("sink.user_events")
df = table.scan().to_pandas()
print(df.head())
```

## Troubleshooting

### Common Issues

1. **ClassNotFoundException for Iceberg classes**
   - Ensure JAR files are in the `lib/` directory
   - Verify JAR versions match Flink version (1.18)

2. **MSK Authentication Errors**
   - Verify IAM role has `kafka-cluster:*` permissions
   - Check MSK cluster security group allows Flink VPC

3. **S3 Access Denied**
   - Verify IAM role has S3 permissions for warehouse location
   - Check S3 Tables bucket permissions

4. **Checkpoint Failures**
   - Increase checkpoint timeout
   - Check S3 write permissions
   - Verify network connectivity

### Debug Mode

Set environment variable:
```
LOG_LEVEL = DEBUG
```

## Performance Tuning

### Parallelism
- Start with parallelism = number of Kafka partitions
- Increase KPUs if CPU usage > 80%

### Checkpointing
- Increase interval for higher throughput (reduce overhead)
- Decrease interval for better exactly-once semantics

### Iceberg Write Performance
- Adjust `write.target-file-size-bytes` in table properties
- Use appropriate partition strategy

## Cost Optimization

1. **Right-size KPUs**: Start small and scale up
2. **Optimize checkpoint interval**: Balance durability vs performance
3. **Use Spot instances**: For non-critical workloads
4. **Monitor idle time**: Pause application when not needed

## Production Checklist

- [ ] IAM roles properly configured
- [ ] CloudWatch alarms set up
- [ ] Backup checkpoint location configured
- [ ] Monitoring dashboard created
- [ ] Disaster recovery plan documented
- [ ] Performance baseline established
- [ ] Cost monitoring enabled

## Next Steps

1. Set up CI/CD pipeline for application updates
2. Implement custom metrics and monitoring
3. Create data quality checks
4. Set up automated scaling policies
5. Implement multi-region deployment