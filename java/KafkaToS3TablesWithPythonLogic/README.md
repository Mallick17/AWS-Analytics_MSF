# Kafka to S3 Tables with Python Business Logic

This project demonstrates how to integrate Python business logic (FastAPI) with a Java Flink streaming application for real-time data processing.

## Architecture

```
Kafka (MSK) → Flink (Java) → Python FastAPI → Flink → S3 Tables (Iceberg)
```

### Components

1. **Java Flink Application**: Handles stream processing, Kafka consumption, and Iceberg writes
2. **Python FastAPI Service**: Implements business logic (fraud detection, risk scoring, recommendations)
3. **Async Communication**: Flink calls Python API asynchronously using AsyncDataStream

## Folder Structure

Place this in the AWS samples repository:

```
java/KafkaToS3TablesWithPythonLogic/
├── src/
│   └── main/
│       ├── java/
│       │   └── com/
│       │       └── amazonaws/
│       │           └── services/
│       │               └── msf/
│       │                   ├── StreamingJob.java
│       │                   ├── EventRecord.java
│       │                   └── PythonLogicAsyncFunction.java
│       └── resources/
│           └── flink-application-properties-dev.json
├── python/
│   ├── fastapi_logic.py
│   ├── requirements.txt
│   └── Dockerfile
├── pom.xml
└── README.md
```

## Configuration

### Embedded Configuration

All configuration is embedded in `StreamingJob.java` with defaults. You can override via Runtime Properties:

```java
// Kafka Configuration
kafka.bootstrap.servers = your-msk-brokers:9098
kafka.topics = user_events,ride_events,payment_events
kafka.consumer.group = flink-s3-tables-python-logic
kafka.offset = earliest

// S3 Tables Configuration
s3.warehouse = arn:aws:s3tables:region:account:bucket/name
table.namespace = sink

// Python FastAPI Configuration
python.api.url = http://your-fastapi-service:8000/process
python.api.timeout.ms = 5000
python.api.retry.attempts = 3
python.logic.enabled = true  // Set to false to bypass Python logic

// Flink Configuration
parallelism = 2
checkpoint.interval = 60000
```

### Local Development Configuration

Create `src/main/resources/flink-application-properties-dev.json`:

```json
[
  {
    "PropertyGroupId": "ApplicationProperties",
    "PropertyMap": {
      "kafka.bootstrap.servers": "localhost:9092",
      "kafka.topics": "user_events",
      "kafka.consumer.group": "flink-local",
      "kafka.offset": "earliest",
      "s3.warehouse": "arn:aws:s3tables:ap-south-1:YOUR_ACCOUNT:bucket/YOUR_BUCKET",
      "table.namespace": "sink",
      "python.api.url": "http://localhost:8000/process",
      "python.api.timeout.ms": "5000",
      "python.api.retry.attempts": "3",
      "python.logic.enabled": "true",
      "parallelism": "1",
      "checkpoint.interval": "60000"
    }
  }
]
```

## Setup Instructions

### Step 1: Build Java Application

```bash
cd java/KafkaToS3TablesWithPythonLogic

# Build the fat JAR
mvn clean package

# The JAR will be created at:
# target/kafka-s3tables-python-logic-1.0.jar
```

### Step 2: Deploy Python FastAPI Service

#### Option A: Local Development

```bash
cd python

# Install dependencies
pip install -r requirements.txt

# Run FastAPI
python fastapi_logic.py

# Or use uvicorn directly
uvicorn fastapi_logic:app --host 0.0.0.0 --port 8000 --reload
```

Test the API:
```bash
curl http://localhost:8000/

curl -X POST http://localhost:8000/process \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "evt123",
    "user_id": "user456",
    "event_type": "ride_request",
    "timestamp": 1702990800000,
    "ride_id": "ride789",
    "surge_multiplier": 2.5,
    "estimated_wait_minutes": 15,
    "fare_amount": 45.50,
    "driver_rating": 4.8
  }'
```

#### Option B: Docker Deployment

```bash
cd python

# Build Docker image
docker build -t fastapi-logic:latest .

# Run container
docker run -d \
  --name fastapi-logic \
  -p 8000:8000 \
  fastapi-logic:latest

# Check logs
docker logs -f fastapi-logic
```

#### Option C: AWS Deployment (ECS/EKS)

**For ECS:**
```bash
# Push to ECR
aws ecr create-repository --repository-name fastapi-logic
aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin YOUR_ACCOUNT.dkr.ecr.ap-south-1.amazonaws.com
docker tag fastapi-logic:latest YOUR_ACCOUNT.dkr.ecr.ap-south-1.amazonaws.com/fastapi-logic:latest
docker push YOUR_ACCOUNT.dkr.ecr.ap-south-1.amazonaws.com/fastapi-logic:latest

# Create ECS Task Definition and Service
# Use the image: YOUR_ACCOUNT.dkr.ecr.ap-south-1.amazonaws.com/fastapi-logic:latest
```

**For EKS:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fastapi-logic
spec:
  replicas: 3
  selector:
    matchLabels:
      app: fastapi-logic
  template:
    metadata:
      labels:
        app: fastapi-logic
    spec:
      containers:
      - name: fastapi-logic
        image: YOUR_ACCOUNT.dkr.ecr.ap-south-1.amazonaws.com/fastapi-logic:latest
        ports:
        - containerPort: 8000
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
---
apiVersion: v1
kind: Service
metadata:
  name: fastapi-logic-service
spec:
  type: LoadBalancer
  ports:
  - port: 8000
    targetPort: 8000
  selector:
    app: fastapi-logic
```

### Step 3: Deploy to Amazon Managed Service for Apache Flink

#### Upload JAR to S3

```bash
aws s3 cp target/kafka-s3tables-python-logic-1.0.jar \
  s3://your-bucket/flink-apps/
```

#### Create Managed Flink Application

```bash
aws kinesisanalyticsv2 create-application \
  --application-name kafka-s3tables-python-logic \
  --runtime-environment FLINK-1_19 \
  --service-execution-role arn:aws:iam::YOUR_ACCOUNT:role/KinesisAnalyticsRole \
  --application-configuration '{
    "ApplicationCodeConfiguration": {
      "CodeContent": {
        "S3ContentLocation": {
          "BucketARN": "arn:aws:s3:::your-bucket",
          "FileKey": "flink-apps/kafka-s3tables-python-logic-1.0.jar"
        }
      },
      "CodeContentType": "ZIPFILE"
    },
    "EnvironmentProperties": {
      "PropertyGroups": [
        {
          "PropertyGroupId": "ApplicationProperties",
          "PropertyMap": {
            "kafka.bootstrap.servers": "YOUR_MSK_BROKERS",
            "kafka.topics": "user_events",
            "python.api.url": "http://YOUR_FASTAPI_SERVICE:8000/process",
            "python.logic.enabled": "true"
          }
        }
      ]
    },
    "FlinkApplicationConfiguration": {
      "CheckpointConfiguration": {
        "ConfigurationType": "CUSTOM",
        "CheckpointingEnabled": true,
        "CheckpointInterval": 60000
      },
      "ParallelismConfiguration": {
        "ConfigurationType": "CUSTOM",
        "Parallelism": 2,
        "ParallelismPerKPU": 1,
        "AutoScalingEnabled": true
      }
    }
  }'
```

#### Start the Application

```bash
aws kinesisanalyticsv2 start-application \
  --application-name kafka-s3tables-python-logic
```

## Running Locally

### Prerequisites

1. Java 11+
2. Maven 3.6+
3. Python 3.11+
4. Local Kafka (or use AWS MSK)
5. AWS credentials configured

### Steps

1. **Start Python Service**
   ```bash
   cd python
   python fastapi_logic.py
   ```

2. **Run Flink Job in IDE**
   - Open `StreamingJob.java` in IntelliJ IDEA
   - Set environment variable: `IS_LOCAL=true` (if needed)
   - Edit `flink-application-properties-dev.json` with local config
   - Run the main method

## Feature Flags

### Bypass Python Logic

To run without Python logic (direct passthrough):

```properties
python.logic.enabled=false
```

This is useful for:
- Testing the Flink pipeline independently
- Debugging performance issues
- Running when Python service is unavailable

## Testing

### Test Python API

```bash
# Health check
curl http://localhost:8000/health

# Process single event
curl -X POST http://localhost:8000/process \
  -H "Content-Type: application/json" \
  -d @test_event.json

# Batch processing
curl -X POST http://localhost:8000/batch-process \
  -H "Content-Type: application/json" \
  -d @test_events_batch.json
```

### Generate Test Data to Kafka

Use the data generator from the AWS samples repo:

```bash
cd python/data-generator
python stock.py --stream-name user_events --region ap-south-1
```

## Monitoring

### CloudWatch Metrics

The application automatically sends metrics to CloudWatch:
- `numRecordsIn`: Records read from Kafka
- `numRecordsOut`: Records written to S3 Tables
- `pythonApiCallDuration`: Time spent calling Python API
- `pythonApiFailures`: Failed Python API calls

### Logs

View logs in CloudWatch:
```bash
aws logs tail /aws/kinesis-analytics/kafka-s3tables-python-logic --follow
```

### Python API Metrics

Access FastAPI metrics at: `http://your-service:8000/metrics`

## Customizing Python Logic

Edit `fastapi_logic.py` to add your business logic:

```python
def detect_fraud(event: EventRequest) -> bool:
    # Your custom fraud detection logic
    if event.fare_amount > YOUR_THRESHOLD:
        return True
    return False

def calculate_risk_score(event: EventRequest) -> float:
    # Your custom risk scoring logic
    risk = 0.0
    # ... your logic
    return risk
```

## Troubleshooting

### Python API Not Responding

1. Check FastAPI logs: `docker logs fastapi-logic`
2. Verify network connectivity from Flink
3. Check firewall rules and security groups
4. Increase timeout: `python.api.timeout.ms=10000`

### High Latency

1. Increase parallelism: `parallelism=4`
2. Reduce batch size in Python
3. Enable connection pooling (already configured)
4. Scale Python service horizontally

### Flink Job Failing

1. Check CloudWatch logs
2. Verify IAM permissions for S3 Tables, MSK, CloudWatch
3. Check Python API URL is correct
4. Try with `python.logic.enabled=false` to isolate issue

## Performance Tuning

### Flink Optimization

```properties
# Increase parallelism
parallelism=4

# Adjust checkpoint interval
checkpoint.interval=120000

# Tune async capacity
python.async.capacity=200
```

### Python Service Optimization

1. Scale horizontally (multiple replicas)
2. Use load balancer in front of Python services
3. Implement caching for repeated lookups
4. Use async Python functions for I/O operations

## Security

### Network Security

- Deploy Python service in private subnet
- Use VPC endpoints for AWS services
- Restrict security group rules to Flink subnet only

### API Authentication

Add authentication to Python API:

```python
from fastapi import Header, HTTPException

@app.post("/process")
async def process_event(
    event: EventRequest,
    x_api_key: str = Header(None)
):
    if x_api_key != "your-secret-key":
        raise HTTPException(status_code=401, detail="Invalid API key")
    # ... rest of logic
```

Then update Java to include API key in headers.

## Cost Optimization

1. **Use Spot Instances** for Python service (if on ECS/EKS)
2. **Auto-scale** based on load
3. **Adjust Flink parallelism** based on throughput
4. **Enable Python logic conditionally** based on event type

## Support

For issues or questions:
1. Check AWS Managed Flink documentation
2. Review CloudWatch logs
3. Enable debug logging: `log4j.logger.com.amazonaws.services.msf=DEBUG`

## License

MIT-0 License - See LICENSE file