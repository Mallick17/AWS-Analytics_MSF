# PRD: Flink Analytics Platform – Local Development Environment

**Status:** Approved  
**Audience:** Data Engineering, Backend, DevOps  
**Purpose:** Enable fast, reliable local development of PyFlink jobs that consume Kafka events and write to Iceberg tables (simulating AWS S3 Tables).

---

## 1. Executive Summary

This document defines a **local-only development environment** for building and testing Apache Flink (Python) streaming jobs.

The production system ingests events from a backend producer into Kafka, processes them via Apache Flink, and stores results in **AWS S3 Tables**.  
For local development, this setup replaces AWS-managed components with **Docker-based services** while keeping **data formats and semantics identical**.

### Key Design Principles

- Kafka and Flink must run as **real runtimes**, not emulated
- Storage is **Apache Iceberg**, stored in **LocalStack S3**
- No Glue, no multi-environment setup, no stress testing
- Local setup must be **deployable to Cloud Flink later with minimal changes**

---

## 2. In-Scope & Out-of-Scope

### In Scope

- Docker-based Kafka (KRaft mode)
- Docker-based Apache Flink cluster with PyFlink
- LocalStack S3 used as Iceberg object storage
- Python CLI event producer (Faker-based)
- One reference use case: **Order Creation Event**
- Minimal Flink transformation
- Storage into Iceberg tables
- Local debugging, testing, and iteration

### Out of Scope

- Multiple environment profiles (dev/test/stress)
- Glue catalog
- Production deployment automation
- Backend PHP producer implementation
- Authentication / IAM
- Performance benchmarking or load testing

---

## 3. Target Architecture (Local Dev)

Python CLI (Order Events)
↓
Apache Kafka (Docker, KRaft)
↓
Apache Flink Cluster (Docker, PyFlink)
↓
LocalStack S3 tables


### Architectural Notes

- **Kafka & Flink are real** → behavior matches production
- **S3 Tables** → Use LocalStack provided s3 tables only
- **No Glue** → Hadoop-style Iceberg catalog is sufficient
- **Storage layout is abstracted** → No hardcoded local paths

---

## 4. Production Compatibility Considerations

Although this setup is local-only, it must remain **Cloud Flink compatible**.

### Flink Job Portability Requirements

- Jobs must:
  - Use environment-driven configuration
  - Avoid local filesystem dependencies
  - Use Iceberg APIs compatible with cloud runtimes
- No assumptions about:
  - Local paths
  - Docker networking
  - Static credentials

**Result:**  
The same job code should be deployable to:
- Local Flink (Docker)
- AWS Managed Flink / EMR Flink

---

## 5. Example Use Case: Order Creation Event

### 5.1 Event Description

An **Order Creation** event represents a successful order placed by a user.

### Event Flow

Order Created → Kafka Topic → Flink Job → S3 Table at LocalStack


---

### 5.2 Order Event Schema (v1)

```json
{
  "event_id": "uuid",
  "event_time": "ISO-8601 timestamp",
  "order_id": "string",
  "user_id": "string",
  "order_amount": "decimal",
  "currency": "string",
  "order_status": "CREATED"
}

### Schema Rules

- event_time is the event-time reference for Flink
- Schema must be:
  - Versioned
  - Backward compatible
- Validation happens at producer and Flink boundaries

6. Kafka Requirements
Kafka Setup

Single-broker Kafka (KRaft mode)

Docker-managed

Persistent volumes enabled

Topic Definition
Topic Name	Purpose
orders.created.v1	Order creation events
Topic Characteristics

Partition count: configurable

Key: order_id

Value format: JSON

Retention: development-friendly (days)

7. Flink Job Requirements
Job: order_ingest_job
Responsibilities

Consume orders.created.v1 topic

Deserialize and validate events

Assign event-time timestamps

Apply minimal transformation

Write to Iceberg table in S3

Minimal Transformation Rules

Add ingestion metadata:

processed_at

No aggregation

No joins

No enrichment

This job acts as a reference ingestion pipeline.

Flink Operational Requirements

PyFlink-based

JobManager + TaskManager

Checkpointing enabled

Restart strategy enabled

Logs accessible via:

Flink UI

Docker logs

8. Storage: Iceberg Tables (LocalStack S3)
Storage Strategy

Iceberg tables stored directly in LocalStack S3

No Glue catalog

No local filesystem paths exposed to jobs

Table Definition

Table: analytics.orders

Column	Type
event_id	string
event_time	timestamp
order_id	string
user_id	string
order_amount	decimal
currency	string
order_status	string
processed_at	timestamp
Partitioning Strategy

Partition by event date (event_time)

9. Python Order Event Producer
Producer Characteristics

CLI-based Python application

Uses Faker for realistic data

Produces directly to Kafka

Required Capabilities

Generate valid order events

Deterministic runs (seed support)

Configurable event rate

Schema validation before publish

Example Usage
analytics-producer produce orders --rate 10

10. Infrastructure Management
Requirements

Docker Compose orchestration

One-command startup

One-command shutdown

Health checks for:

Kafka

Flink

LocalStack

Non-Requirements

No environment switching

No performance tuning profiles

No monitoring stack (Prometheus/Grafana)

11. Testing Requirements
Required Test Levels

Unit Tests

Flink transformation logic

Schema validation

Integration Tests

Kafka → Flink connectivity

Flink → Iceberg writes

End-to-End Test

Produce order event

Verify row exists in Iceberg table

Explicitly Not Required

Load tests

Stress tests

Long-running soak tests

12. Success Criteria

The setup is considered successful if:

A developer can start the system locally in <30 minutes

Order events flow end-to-end without cloud dependencies

Flink jobs can be iterated without rebuilding containers

Stored Iceberg data can be queried locally

Job code is deployable to Cloud Flink with config-only changes

13. Known Limitations

LocalStack does not emulate AWS S3 Tables control plane

Performance characteristics differ from AWS

IAM and security are not validated locally

These are acceptable trade-offs for local development.

14. Summary

This PRD defines a focused, realistic, and future-proof local development setup for Flink analytics:

No over-engineering

No false AWS emulation

Strong production alignment

Clear reference use case (Order Creation)

It prioritizes developer velocity and correctness while keeping the path to cloud deployment clean.


---

If you want, next I can:

- Split this into **`ARCHITECTURE.md`, `DEV_WORKFLOW.md`, `ORDER_PIPELINE.md`**
- Convert this into **Cursor rules / repo guidelines**
- Or generate a **minimal reference implementation outline** (no code yet)

Just tell me the next step.


---

### Project Structure
rt_data_pipeline_local/
│
├── README.md
├── PRD.md
├── .env.example
│
├── infra/
│   ├── docker-compose.yml
│   ├── scripts/
│   │   ├── setup.sh
│   │   ├── start.sh
│   │   ├── stop.sh
│   │   └── health-check.sh
│   └── kafka/
│       └── topics.yaml
│
├── producer/
│   ├── README.md
│   ├── cli.py
│   ├── config.yaml
│   ├── schemas/
│   │   └── order_created_v1.py
│   ├── generators/
│   │   └── order_generator.py
│   ├── kafka/
│   │   └── producer.py
│   └── requirements.txt
│
├── flink/
│   ├── README.md
│   ├── common/
│   │   ├── config.py
│   │   ├── kafka_source.py
│   │   ├── iceberg_sink.py
│   │   ├── serialization.py
│   │   └── job_base.py
│   │
│   ├── jobs/
│   │   └── order_ingest/
│   │       ├── job.py
│   │       ├── config.yaml
│   │       └── README.md
│   │
│   └── tests/
│       ├── unit/
│       └── integration/
│
├── storage/
│   └── iceberg/
│       ├── schemas/
│       │   └── orders.sql
│       └── README.md
│
├── localstack/
│   ├── init/
│   │   └── s3.sh
│   └── README.md
│
├── tests/
│   └── e2e/
│       └── test_order_pipeline.py
│
└── docs/
    ├── architecture.md
    ├── dev-workflow.md
    ├── order-pipeline.md
    └── cloud-flink-notes.md

### Why This Structure Works (Key Design Rationale)
1. Single Responsibility per Folder

producer/ → event generation only

flink/ → streaming logic only

infra/ → runtime orchestration only

storage/ → table contracts only

Cursor/Windsurf performs much better when responsibilities are cleanly separated.

2. Cloud-Ready Without Cloud Noise

No local paths inside Flink jobs

No environment-specific logic

Only config files change for Cloud Flink

This avoids later refactors.

3. Order Pipeline Is Explicit

There is exactly one reference pipeline:

producer/schemas/order_created_v1.py
        ↓
flink/jobs/order_ingest/job.py
        ↓
storage/iceberg/schemas/orders.sql


This clarity is intentional.

4. AI-Tool Friendly

This layout helps Cursor/Windsurf:

Infer intent from folder names

Keep context small

Avoid hallucinating infra or AWS Glue logic

Suggest correct imports and boundaries