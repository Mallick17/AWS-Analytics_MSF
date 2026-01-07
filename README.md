# Amazon Managed Service for Apache Flink Examples

Example applications in Java, Python, Scala and SQL for Amazon Managed Service for Apache Flink (formerly known as Amazon Kinesis Data Analytics), illustrating various aspects of Apache Flink applications, and simple "getting started" base projects.

## Table of Contents

### Java Examples

#### Getting Started
- [**Getting Started - DataStream API**](./java/GettingStarted) - Skeleton project for a basic Flink Java application using DataStream API
- [**Getting Started - Table API & SQL**](./java/GettingStartedTable) - Basic Flink Java application using Table API & SQL with DataStream API

#### Connectors
- [**Kinesis Connectors**](./java/KinesisConnectors) - Examples of Flink Kinesis Connector source and sink (standard and EFO)
- [**Kinesis Source Deaggregation**](./java/KinesisSourceDeaggregation) - Handling Kinesis record deaggregation in the Kinesis source
- [**Kafka Connectors**](./java/KafkaConnectors) - Examples of Flink Kafka Connector source and sink
- [**Kafka Config Providers**](./java/KafkaConfigProviders) - Examples of using Kafka Config Providers for secure configuration management
- [**DynamoDB Stream Source**](./java/DynamoDBStreamSource) - Reading from DynamoDB Streams as a source
- [**Kinesis Firehose Sink**](./java/KinesisFirehoseSink) - Writing data to Amazon Kinesis Data Firehose
- [**SQS Sink**](./java/SQSSink) - Writing data to Amazon SQS
- [**Prometheus Sink**](./java/PrometheusSink) - Sending metrics to Prometheus
- [**Flink CDC**](./java/FlinkCDC) - Change Data Capture examples using Flink CDC
- [**JdbcSink**](./java/JdbcSink) - Writes to a relational database executing upsert statements

#### Reading and writing files and transactional data lake formats
- [**Iceberg**](./java/Iceberg) - Working with Apache Iceberg and Amazon S3 Tables
- [**S3 Sink**](./java/S3Sink) - Writing JSON data to Amazon S3
- [**S3 Avro Sink**](./java/S3AvroSink) - Writing Avro format data to Amazon S3
- [**S3 Avro Source**](./java/S3AvroSource) - Reading Avro format data from Amazon S3
- [**S3 Parquet Sink**](./java/S3ParquetSink) - Writing Parquet format data to Amazon S3
- [**S3 Parquet Source**](./java/S3ParquetSource) - Reading Parquet format data from Amazon S3

#### Data Formats & Schema Registry
- [**Avro with Glue Schema Registry - Kinesis**](./java/AvroGlueSchemaRegistryKinesis) - Using Avro format with AWS Glue Schema Registry and Kinesis
- [**Avro with Glue Schema Registry - Kafka**](./java/AvroGlueSchemaRegistryKafka) - Using Avro format with AWS Glue Schema Registry and Kafka

#### Stream Processing Patterns
- [**Serialization**](./java/Serialization) - Serialization of record and state
- [**Windowing**](./java/Windowing) - Time-based window aggregation examples
- [**Side Outputs**](./java/SideOutputs) - Using side outputs for data routing and filtering
- [**Async I/O**](./java/AsyncIO) - Asynchronous I/O patterns with retries for external API calls
- [**Custom Metrics**](./java/CustomMetrics) - Creating and publishing custom application metrics
- [**Fetching credentials from Secrets Manager**](./java/FetchSecrets) - Dynamically fetching credentials from AWS Secrets Manager

#### Utilities
- [**Fink Data Generator (JSON)**](java/FlinkDataGenerator) - How to use a Flink application as data generator, for functional and load testing.

### Python Examples

#### Getting Started
- [**Getting Started**](./python/GettingStarted) - Basic PyFlink application Table API & SQL

#### Handling Python dependencies
- [**Python Dependencies**](./python/PythonDependencies) - Managing Python dependencies in PyFlink applications using `requirements.txt`
- [**Packaged Python Dependencies**](./python/PackagedPythonDependencies) - Managing Python dependencies packaged with the PyFlink application at build time

#### Connectors
- [**Datastream Kafka Connector**](./python/DatastreamKafkaConnector) - Using Kafka connector with PyFlink DataStream API
- [**Kafka Config Providers**](./python/KafkaConfigProviders) - Secure configuration management for Kafka in PyFlink
- [**S3 Sink**](./python/S3Sink) - Writing data to Amazon S3 using PyFlink
- [**Firehose Sink**](./python/FirehoseSink) - Writing data to Amazon Kinesis Data Firehose
- [**Iceberg Sink**](./python/IcebergSink) - Writing data to Apache Iceberg tables
- [**Hudi Sink**](./python/HudiSink) - Writing data to Apache Hudi tables

#### Stream Processing Patterns
- [**Windowing**](./python/Windowing) - Time-based window aggregation examples with PyFlink/SQL
- [**User Defined Functions (UDF)**](./python/UDF) - Creating and using custom functions in PyFlink

#### Utilities
- [**Data Generator**](./python/data-generator) - Python script for generating sample data to Kinesis Data Streams
- [**Local Development on Apple Silicon**](./python/LocalDevelopmentOnAppleSilicon) - Setup guide for local development of Flink 1.15 on Apple Silicon Macs (not required with Flink 1.18 or later)


### Scala Examples

#### Getting Started
- [**Getting Started - DataStream API**](./scala/GettingStarted) - Skeleton project for a basic Flink Scala application using DataStream API

### Infrastructure & Operations

- [**Auto Scaling**](./infrastructure/AutoScaling) - Custom autoscaler for Amazon Managed Service for Apache Flink
- [**Scheduled Scaling**](./infrastructure/ScheduledScaling) - Scale applications up and down based on daily time schedules
- [**Monitoring**](./infrastructure/monitoring) - Extended CloudWatch Dashboard examples for monitoring applications
- [**Scripts**](./infrastructure/scripts) - Useful shell scripts for interacting with Amazon Managed Service for Apache Flink control plane API

---

## Contributing

See [Contributing Guidelines](CONTRIBUTING.md#security-issue-notifications) for more information.

## License Summary

This sample code is made available under the MIT-0 license. See the LICENSE file.



# Download ALL Flink JAR dependencies

These are **mandatory** for Flink jobs to work.

### 📂 Target directory

```bash
mkdir -p flink/usrlib
```

---

## 🔹 Kafka Connector JAR (REQUIRED)

```bash
curl -L -o flink/usrlib/flink-connector-kafka-3.2.0-1.19.jar \
https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar
```

---

## 🔹 Iceberg Runtime JAR (REQUIRED)

```bash
curl -L -o flink/usrlib/iceberg-flink-runtime-1.19-1.6.1.jar \
https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.6.1/iceberg-flink-runtime-1.19-1.6.1.jar
```

---

### ✅ Final expected structure

```text
flink/usrlib/
├── flink-connector-kafka-3.2.0-1.19.jar
└── iceberg-flink-runtime-1.19-1.6.1.jar
```

Flink will auto-load everything from `usrlib`.

Great question — this is **exactly the right thing to ask** at this stage 👍
Let’s clear this **precisely**, because **adding unnecessary JARs can break Flink**.

---

# ❓ Do we need **kafka-clients** and **Hadoop client** JARs?

Short answer:

> ⚠️ **NO — do NOT manually add them (in most cases)**
> ✅ They are **already transitively included** where required

Now I’ll explain **why**, **when you DO need them**, and **how to download them safely if required**.

---

# 🧠 Flink dependency model (CRITICAL to understand)

Flink has **three layers of dependencies**:

1️⃣ **Flink distribution (inside image)**
2️⃣ **Connector JARs (Kafka, Iceberg, etc.)**
3️⃣ **Transitive dependencies (kafka-clients, Hadoop, AWS SDK, etc.)**

You should **ONLY** add layer **#2 manually**.

---

# 🔹 Kafka client JAR (`kafka-clients-3.6.1`)

## ❌ Do you need to download it manually?

**NO**

### Why?

* `flink-connector-kafka-3.2.0-1.19.jar` **already bundles**:

  * `kafka-clients`
  * serializers
  * protocol classes

If you manually add:

```
kafka-clients-3.6.1.jar
```

You risk:

* ❌ Version conflicts
* ❌ `NoSuchMethodError`
* ❌ Consumer group instability

### ✅ Correct approach

✔ Add **ONLY**:

```
flink-connector-kafka-3.2.0-1.19.jar
```

Flink will resolve Kafka clients internally.

---

### Why?

* You are using **local filesystem Iceberg**:

  ```
  /iceberg/warehouse
  ```
* No HDFS
* No S3
* No GCS
* No Azure Blob

So Hadoop is **not required**.

That log line you saw:

```text
Hadoop FS is not available (not packaged with this application)
```

👉 This is **INFO**, not ERROR
👉 Completely safe to ignore

---

# ⚠️ WHEN do you actually NEED Hadoop JARs?

You need Hadoop **ONLY IF**:

| Use case            | Need Hadoop? |
| ------------------- | ------------ |
| HDFS                | ✅ YES        |
| S3A (Iceberg on S3) | ✅ YES        |
| Azure / GCS         | ✅ YES        |
| Local FS            | ❌ NO         |

---

# ✅ Correct Hadoop setup (FOR FUTURE – AWS S3)

When you move to **Iceberg on S3**, you will need:

### Required JARs

```
hadoop-common-3.3.6.jar
hadoop-aws-3.3.6.jar
aws-java-sdk-bundle-1.12.x.jar
```

### But ⚠️ NOT now.

---

# 🧱 Safe JAR matrix (THIS IS IMPORTANT)

| Component             | Should you add manually? |
| --------------------- | ------------------------ |
| flink-connector-kafka | ✅ YES                    |
| iceberg-flink-runtime | ✅ YES                    |
| kafka-clients         | ❌ NO                     |
| hadoop-client         | ❌ NO                     |
| aws-sdk               | ❌ NO (until S3)          |

---

# ❗ Why adding extra JARs is dangerous

Flink uses **child-first classloading**.

If you add:

* Wrong Kafka client version
* Wrong Hadoop version

You will get:

* `NoSuchMethodError`
* `ClassCastException`
* Random runtime failures

These are **nightmare bugs**.

---

# ✅ If you REALLY want to download them (for reference only)

### Kafka clients (NOT recommended now)

```bash
curl -LO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar
```

### Hadoop client (NOT recommended now)

```bash
curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client/3.3.6/hadoop-client-3.3.6.jar
```

⚠️ **Do NOT place them in `flink/usrlib` yet**

---

# 🧠 Final Rule (memorize this)

> **Only add “connector JARs”, never “client JARs”**

Flink connectors are **opinionated bundles** — they manage compatibility for you.

---

# ✅ Your CURRENT correct `flink/usrlib`

```text
flink/usrlib/
├── flink-connector-kafka-3.2.0-1.19.jar
└── iceberg-flink-runtime-1.19-1.6.1.jar
```

This is **perfect** for:

* Kafka → Flink
* Flink → Iceberg (local FS)

---

# What actually happened (root cause)

### Key facts from your output

1. **Job submission succeeded**

```
Job has been submitted with JobID 0857966e...
```

2. **TaskManager exited**

```
docker_local_testing-flink-taskmanager-1  Exited (137)
```

3. **JobManager still running**

```
docker_local_testing-flink-jobmanager-1  Up
```

4. **No running jobs**

```
flink list
No running jobs.
```

---

## ❗ Exit code 137 = OOM Kill (very important)

**Exit (137)** means:

> 💥 **Linux kernel killed the TaskManager due to memory pressure**

This is **not a Flink bug**
This is **Docker memory + Flink default memory**

---

# 🔎 Why this kills your job

Flink architecture:

```
JobManager (control plane)  ✅ alive
TaskManager (execution)    ❌ dead
```

When the **TaskManager dies**:

* The job **immediately fails**
* JobManager cleans it up
* `flink list` shows **nothing**

👉 That’s **exactly what you’re seeing**

---

# 🧠 Why it happens in YOUR setup

You are running:

* Kafka connector
* PyFlink
* Python gateway
* No explicit memory limits

Default Flink TM memory:

```
~1.6 GB process memory
```

Docker Desktop (Mac):

* Often limited to **2–4 GB total**
* Kafka + Zookeeper already consume memory

👉 **OOM kill is guaranteed**

---

# ✅ First FIX: keep TaskManager alive (MANDATORY)

### Update `docker-compose.yml`

Add **explicit memory limits** and reduce Flink memory usage.

```yaml
  flink-taskmanager:
    platform: linux/amd64
    build:
      context: .
      dockerfile: flink/Dockerfile
    command: taskmanager
    depends_on:
      - flink-jobmanager
    volumes:
      - ./iceberg:/iceberg
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 1
        taskmanager.memory.process.size: 1024m
        taskmanager.memory.jvm-metaspace.size: 128m
        taskmanager.memory.jvm-overhead.max: 128m
    deploy:
      resources:
        limits:
          memory: 1.2g
```

Then restart **cleanly**:

```bash
docker-compose down -v
docker-compose up -d
```

---

# ✅ Second FIX: run debug job correctly (shipping Python files)

You **must** use `-pyfs` (you already learned this 👌):

```bash
docker exec -it docker_local_testing-flink-jobmanager-1 \
  flink run -d \
  -pyfs /opt/flink/app \
  -py /opt/flink/app/kafka_debug.py
```

---

# 🔍 How to PROVE Kafka → Flink is working (step-by-step)

## 1️⃣ Confirm TaskManager stays alive

```bash
docker ps
```

You MUST see:

```
docker_local_testing-flink-taskmanager-1   Up
```

If it exits → memory is still insufficient.

---

## 2️⃣ Submit job

```bash
flink run -d -pyfs /opt/flink/app -py /opt/flink/app/kafka_debug.py
```

---

## 3️⃣ Verify job is running

```bash
docker exec -it docker_local_testing-flink-jobmanager-1 flink list
```

Expected:

```
Running jobs:
----------------------------------
KafkaDebugJob (RUNNING)
----------------------------------
```

---

## 4️⃣ Produce Kafka messages

```bash
docker exec -it docker_local_testing-kafka-1 \
  kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic orders
```

Send:

```json
{"event_id":"99","msg":"hello flink"}
```

---

## 5️⃣ Check TaskManager logs (THIS is the proof)

```bash
docker logs docker_local_testing-flink-taskmanager-1 --tail 50
```

You should see:

```
Received record: {"event_id":"99","msg":"hello flink"}
```

🎯 **That is absolute confirmation** that:

* Kafka is producing
* Flink is consuming
* Python → Java bridge works
* No Iceberg / SQL involved

---

# 🧠 Why `flink list` showed nothing earlier

Because:

```
TaskManager OOM → Job fails → Job removed → flink list empty
```

This is **normal Flink behavior**.

---

# ✅ Final mental model (remember this)

| Symptom                      | Meaning             |
| ---------------------------- | ------------------- |
| Exit (137)                   | OOM Kill            |
| Job submitted but disappears | TaskManager crashed |
| JobManager alive             | Control plane OK    |
| No running jobs              | Execution died      |

---

# 🚀 You are actually DONE with Kafka verification

At this point you’ve proven:

* Kafka topic works
* Producer works
* Consumer works
* Flink job submission works
* Only missing piece was **memory**

---

Below is a **clean, step-by-step runbook style documentation**, using **your exact commands**, only reorganized and clarified.
No logic or flow is changed.

---

# 📘 Local Kafka → Flink Setup Runbook

This document describes how to bring up the local Kafka–Flink environment using Docker Compose, create Kafka topics, verify dependencies, and submit a Flink job.

---

## 1️⃣ Navigate to Docker Compose Directory

Go to the directory where `docker-compose.yml` is present.

```bash
cd <path-to-docker-compose-directory>
```

---

## 2️⃣ Start the Docker Environment (Clean Setup)

Bring down any existing containers, rebuild images, and start everything fresh.

```bash
docker compose down -v
docker compose build --no-cache
docker compose up -d
```

✅ This ensures:

* No stale containers
* Fresh images
* Clean volumes

---

## 3️⃣ Copy Flink Job into JobManager Container

Copy the Flink Python job into the Flink JobManager container.

```bash
docker cp flink/job.py docker_local_testing-flink-jobmanager-1:/job.py
```

---

## 4️⃣ Kafka Topic Management

### 🔹 Create Kafka Topic

Create the `orders` topic.

```bash
docker exec -it docker_local_testing-kafka-1 \
  kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic orders \
  --partitions 1 \
  --replication-factor 1
```

---

### Add Events to the topic

```bash
docker exec -it docker_local_testing-kafka-1 \
  kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic orders
```

- Add the event
```
{"event_id":"1","order_id":"ORD-1","user_id":"U1","amount":100}
```

- Check the event weather it is received in the topics or not
```
docker exec -it docker_local_testing-kafka-1 \
  kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic orders \
  --from-beginning \
  --max-messages 1
```

---

### 

### 🔹 List Kafka Topics

Verify that the topic was created successfully.

```bash
docker exec -it docker_local_testing-kafka-1 \
  kafka-topics \
  --bootstrap-server kafka:9092 \
  --list
```

Expected output:

```
orders
```

---

## 5️⃣ Verify Python Dependency in Flink Container

Check that the required Python dependency is available inside the Flink JobManager container.

```bash
docker exec -it docker_local_testing-flink-jobmanager-1 \
  python -c "import ruamel.yaml; print('ruamel OK')"
```

Expected output:

```
ruamel OK
```

---

## 6️⃣ Verify Application Code Exists in Container

Ensure the application code is present inside the container.

### 🔹 Check `common` package

```bash
docker exec -it docker_local_testing-flink-jobmanager-1 \
  ls /opt/flink/app/common
```

### 🔹 Check application root

```bash
docker exec -it docker_local_testing-flink-jobmanager-1 \
  ls /opt/flink/app
```

Expected to see:

* `job.py`
* `common/`
* other project files

---

## 7️⃣ Submit Flink Job (Final Step)

Submit the Flink job in **detached (background) mode**.

```bash
docker exec -it docker_local_testing-flink-jobmanager-1 \
  flink run -d -py /opt/flink/app/job.py
```

✅ The job will:

* Be submitted to the Flink cluster
* Run in the background
* Continue running even after you exit the terminal

---

## 8️⃣ (Optional) Verify Job Status

Check running Flink jobs.

```bash
docker exec -it docker_local_testing-flink-jobmanager-1 flink list
```

You can also verify via the Flink UI:

```
http://localhost:8081
```

---

## ✅ End Result

At this point:

* Docker services are running
* Kafka topic is created
* Python dependencies are verified
* Flink job is successfully submitted and running

---