# Cloud Flink Deployment Notes

## Overview

This document outlines considerations for deploying the local Flink jobs to cloud environments (AWS Managed Flink, EMR Flink).

## Local vs Cloud Comparison

| Aspect | Local | Cloud (AWS) |
|--------|-------|-------------|
| Flink Runtime | Docker containers | Managed Flink / EMR |
| Kafka | Docker (Zookeeper) | Amazon MSK |
| Storage | N/A (print sink) | S3 / Lakehouse (optional) |
| Credentials | N/A | IAM roles |
| Networking | Docker network | VPC |

## Configuration Changes

### Environment Variables

```bash
# Local
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Note: this local setup prints rows and does not write to S3/Iceberg.

# Cloud
KAFKA_BOOTSTRAP_SERVERS=b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9092

# Optional (if adding a lakehouse sink)
# ICEBERG_WAREHOUSE=s3://your-bucket/warehouse
```

### Iceberg Catalog (Glue) (Optional)

If you later add an Iceberg sink, you can use AWS Glue catalog instead of Hadoop:

```python
# Cloud (Glue)
ddl = """
CREATE CATALOG aws WITH (
    'type' = 'iceberg',
    'catalog-type' = 'glue',
    'warehouse' = 's3://your-bucket/warehouse',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
)
"""
```

## AWS Managed Flink Deployment

### Prerequisites

1. MSK cluster with appropriate topics
2. S3 bucket for Iceberg data
3. IAM role with permissions:
   - MSK read access
   - S3 read/write access
   - Glue catalog access (if using Glue)
4. VPC configuration

### Packaging

```bash
# Package Python job
cd flink
zip -r flink-job.zip jobs/ common/

# Upload to S3
aws s3 cp flink-job.zip s3://your-deployment-bucket/
```

### Application Configuration

```json
{
  "ApplicationConfiguration": {
    "FlinkApplicationConfiguration": {
      "ParallelismConfiguration": {
        "ConfigurationType": "CUSTOM",
        "Parallelism": 4,
        "ParallelismPerKPU": 1
      },
      "CheckpointConfiguration": {
        "ConfigurationType": "CUSTOM",
        "CheckpointingEnabled": true,
        "CheckpointInterval": 60000
      }
    },
    "EnvironmentProperties": {
      "PropertyGroups": [
        {
          "PropertyGroupId": "KafkaConfig",
          "PropertyMap": {
            "bootstrap.servers": "b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9092"
          }
        },
        {
          "PropertyGroupId": "IcebergConfig",
          "PropertyMap": {
            "warehouse": "s3://your-bucket/warehouse"
          }
        }
      ]
    }
  }
}
```

## EMR Flink Deployment

### EMR Cluster Configuration

```bash
aws emr create-cluster \
  --name "Flink Analytics" \
  --release-label emr-6.15.0 \
  --applications Name=Flink \
  --instance-type m5.xlarge \
  --instance-count 3
```

### Job Submission

```bash
flink run \
  -m yarn-cluster \
  -py jobs/order_ingest/job.py \
  -pyFiles flink-job.zip
```

## Security Considerations

### IAM Policy (Minimum)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket",
        "arn:aws:s3:::your-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka:DescribeCluster",
        "kafka:GetBootstrapBrokers"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable"
      ],
      "Resource": "*"
    }
  ]
}
```

### MSK Authentication

For production, use IAM authentication:

```python
# Kafka connector with IAM
'properties.security.protocol' = 'SASL_SSL',
'properties.sasl.mechanism' = 'AWS_MSK_IAM',
'properties.sasl.jaas.config' = '...',
```

## Testing Cloud Deployment

1. **Unit tests**: Run locally (no changes)
2. **Integration tests**: Point to cloud resources
3. **Staging environment**: Full cloud deployment
4. **Monitoring**: CloudWatch metrics and alarms

## Checklist

- [ ] MSK cluster created and configured
- [ ] S3 bucket created with appropriate lifecycle policies
- [ ] IAM roles configured
- [ ] VPC and security groups set up
- [ ] Glue catalog configured (if using)
- [ ] Environment variables updated
- [ ] Job packaged and uploaded
- [ ] Monitoring and alerting configured
- [ ] Backup and recovery procedures documented
