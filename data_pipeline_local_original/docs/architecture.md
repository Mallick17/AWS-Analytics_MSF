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
│  │   (Python)   │────▶│ (Zookeeper)  │────▶│  (JobManager +   │ │
│  │              │     │              │     │   TaskManager)   │ │
│  └──────────────┘     └──────────────┘     └──────────────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Event Producer (Python CLI)

- **Technology**: Python 3.11+, Click, Faker
- **Purpose**: Generate realistic order events
- **Output**: JSON messages to Kafka

### 2. Apache Kafka (Zookeeper Mode)

- **Technology**: Confluent Platform 7.5
- **Mode**: Zookeeper
- **Purpose**: Event streaming and buffering
- **Topics**: `orders.created.v1`

### 3. Apache Flink Cluster

- **Technology**: Flink 1.18, PyFlink
- **Components**:
  - JobManager (coordination)
  - TaskManager (execution)
- **Purpose**: Stream processing

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
5. Flink prints events via the built-in print connector
```

## Configuration Strategy

All components use **environment-driven configuration**:

| Component | Config Source |
|-----------|---------------|
| Producer | `config.yaml` + env vars |
| Flink | `FlinkConfig` dataclass + env vars |
| Kafka | Docker Compose env |

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
│  jobmanager:8081 (internal & host)                  │
│  jobmanager:6123 (internal, RPC)                    │
│                                                      │
└─────────────────────────────────────────────────────┘
```

## Cloud Compatibility

The architecture is designed for easy cloud migration:

| Local | Cloud |
|-------|-------|
| Docker Flink | AWS Managed Flink / EMR |
| Local Kafka | Amazon MSK |

Migration requires only configuration changes, no code changes.
