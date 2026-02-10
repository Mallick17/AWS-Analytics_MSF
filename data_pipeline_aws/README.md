# Data Pipeline AWS

Production-ready Apache Flink data pipeline supporting both **local Docker development** and **AWS Managed Flink deployment** with S3 Tables integration.

## ✨ Features

- **Dual Environment Support**: Same codebase runs locally and on AWS
- **Local Development**: Docker-based with filesystem Iceberg storage
- **AWS Production**: Managed Flink with S3 Tables catalog
- **Kafka Integration**: Local Kafka and AWS MSK with IAM authentication
- **Iceberg Format**: Efficient columnar storage with time-based partitioning
- **Maven Build**: Automated JAR packaging for AWS deployment

## 🏗️ Architecture

### Local Environment
```
Producer → Kafka (Docker) → Flink (Docker) → Iceberg (Filesystem) → ./warehouse/
```

### AWS Environment
```
Producer → MSK → Managed Flink → S3 Tables → S3 Bucket
```

## 📁 Project Structure

```
data_pipeline_aws/
├── flink/                      # Flink job code
│   ├── jobs/
│   │   └── order_ingest/       # Order ingestion job
│   └── common/                 # Shared utilities
│       ├── config.py           # Environment-aware configuration
│       ├── kafka_source.py     # Kafka source factory
│       └── iceberg_sink.py     # Iceberg sink factory
├── producer/                   # Local event producer (local-only)
├── infra/                      # Infrastructure (local-only)
│   ├── docker-compose.yml      # Local Docker setup
│   ├── Dockerfile.flink        # Custom Flink image
│   └── scripts/                # Helper scripts
├── assembly/                   # Maven assembly config
│   └── assembly.xml            # ZIP packaging rules
├── src/main/java/              # Java compatibility shims
├── pom.xml                     # Maven build configuration
├── streaming_job.py            # AWS entry point
├── application_properties.json # AWS runtime config
└── AWS_DEPLOYMENT.md           # Deployment guide
```

## 🚀 Quick Start

### Local Development

```bash
# 1. Build JARs
mvn clean package

# 2. Build Docker image
docker-compose -f infra/docker-compose.yml build

# 3. Start services
docker-compose -f infra/docker-compose.yml up -d

# 4. Run Flink job
docker-compose -f infra/docker-compose.yml exec jobmanager \
  python /opt/flink/usrlib/jobs/order_ingest/job.py

# 5. Produce events
cd producer
pip install -r requirements.txt
python cli.py produce orders --rate 10 --count 100

# 6. Verify data
ls -la warehouse/analytics.db/orders/data/
```

### AWS Deployment

```bash
# 1. Build deployment package
mvn clean package

# 2. Upload to S3
aws s3 cp target/data-pipeline-aws.zip s3://my-bucket/flink-apps/

# 3. Create/Update Managed Flink application via AWS Console
# See AWS_DEPLOYMENT.md for detailed steps
```

## 📚 Documentation

- **[AWS_DEPLOYMENT.md](AWS_DEPLOYMENT.md)**: Complete deployment guide
- **[flink/README.md](flink/README.md)**: Flink job documentation
- **[producer/README.md](producer/README.md)**: Producer documentation

## 🔧 Configuration

### Environment Variables

**Local (.env)**:
```ini
DEPLOYMENT_ENV=local
ICEBERG_WAREHOUSE=file:///opt/iceberg/warehouse
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

**AWS (Managed Flink Environment Properties)**:
```ini
DEPLOYMENT_ENV=aws
AWS_S3_TABLES_ARN=arn:aws:s3tables:region:account:bucket/name
AWS_MSK_BOOTSTRAP_SERVERS=b-1.msk.region.amazonaws.com:9098
```

## 🧪 Testing

```bash
# Run unit tests
cd flink
python -m pytest tests/

# Run end-to-end test
cd tests
python -m pytest e2e/
```

## 📊 Monitoring

### Local
- **Flink UI**: http://localhost:8081
- **Logs**: `docker-compose logs -f jobmanager`

### AWS
- **Flink Dashboard**: Via AWS Console
- **CloudWatch Logs**: `/aws/kinesis-analytics/data-pipeline-aws`
- **Metrics**: CloudWatch Metrics

## 🛠️ Development

### Adding a New Job

1. Create job directory: `flink/jobs/my_job/`
2. Implement job class extending `FlinkJobBase`
3. Update `streaming_job.py` to import new job
4. Test locally, then deploy to AWS

### Modifying Configuration

- **Local**: Update `.env` or `infra/docker-compose.yml`
- **AWS**: Update environment properties in Managed Flink application

## 📝 License

[Your License Here]

## 🤝 Contributing

[Your Contributing Guidelines Here]
