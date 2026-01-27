# Architecture

## Overview

This document describes the architecture of the Flink Analytics Platform local development environment.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Local Development Environment                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────────┐ │
│  │   Producer   │     │    Kafka     │     │      Flink       │ │
│  │   (Python)   │────▶│   (KRaft)    │────▶│  (JobManager +   │ │
│  │              │     │              │     │   TaskManager)   │ │
│  └──────────────┘     └──────────────┘     └────────┬─────────┘ │
│                                                      │          │
│                                                      ▼          │
│                                            ┌──────────────────┐ │
│                                            │   LocalStack S3  │ │
│                                            │   (Iceberg)      │ │
│                                            └──────────────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Event Producer (Python CLI)

- **Technology**: Python 3.11+, Click, Faker
- **Purpose**: Generate realistic order events
- **Output**: JSON messages to Kafka

### 2. Apache Kafka (KRaft Mode)

- **Technology**: Confluent Platform 7.5
- **Mode**: KRaft (no Zookeeper)
- **Purpose**: Event streaming and buffering
- **Topics**: `orders.created.v1`

### 3. Apache Flink Cluster

- **Technology**: Flink 1.18, PyFlink
- **Components**:
  - JobManager (coordination)
  - TaskManager (execution)
- **Purpose**: Stream processing

### 4. LocalStack S3 + Iceberg

- **Technology**: LocalStack 3.0, Apache Iceberg
- **Purpose**: Data lake storage
- **Catalog**: Hadoop-style (no Glue)

## Data Flow

```
1. Producer generates order event
   │
   ▼
2. Event published to Kafka (orders.created.v1)
   │
   ▼
3. Flink consumes event via Kafka connector
   │
   ▼
4. Flink validates and transforms event
   - Add processed_at timestamp
   │
   ▼
5. Write to Iceberg table (analytics.orders)
   │
   ▼
6. Data stored in LocalStack S3
```

## Configuration Strategy

All components use **environment-driven configuration**:

| Component | Config Source |
|-----------|---------------|
| Producer | `config.yaml` + env vars |
| Flink | `FlinkConfig` dataclass + env vars |
| Kafka | Docker Compose env |
| LocalStack | Docker Compose env |

This enables cloud deployment with config-only changes.

## Network Architecture

```
Docker Network: flink-network

┌─────────────────────────────────────────────────────┐
│                  flink-network                       │
│                                                      │
│  kafka:29092 (internal)                             │
│  kafka:9092 (host)                                  │
│                                                      │
│  localstack:4566 (internal & host)                  │
│                                                      │
│  jobmanager:8081 (internal & host)                  │
│  jobmanager:6123 (internal, RPC)                    │
│                                                      │
└─────────────────────────────────────────────────────┘
```

## Cloud Compatibility

The architecture is designed for easy cloud migration:

| Local | Cloud |
|-------|-------|
| LocalStack S3 | AWS S3 |
| Hadoop catalog | AWS Glue catalog (optional) |
| Docker Flink | AWS Managed Flink / EMR |
| Local Kafka | Amazon MSK |

Migration requires only configuration changes, no code changes.
