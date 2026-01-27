"""
Flink Configuration Manager

Environment-driven configuration for cloud-ready Flink jobs.
No hardcoded local paths or Docker-specific settings.
"""

import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass
class KafkaConfig:
    """Kafka source configuration"""
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_ORDERS", "orders.created.v1")
    )
    consumer_group: str = field(
        default_factory=lambda: os.getenv("KAFKA_CONSUMER_GROUP", "flink-order-ingest")
    )
    starting_offset: str = "earliest"


@dataclass
class S3Config:
    """S3/LocalStack configuration"""
    endpoint_url: str = field(
        default_factory=lambda: os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    )
    access_key: str = field(
        default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID", "test")
    )
    secret_key: str = field(
        default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    )
    region: str = field(
        default_factory=lambda: os.getenv("AWS_REGION", "us-east-1")
    )
    bucket: str = field(
        default_factory=lambda: os.getenv("S3_BUCKET", "analytics-data")
    )


@dataclass
class IcebergConfig:
    """Iceberg catalog and table configuration"""
    catalog_name: str = field(
        default_factory=lambda: os.getenv("ICEBERG_CATALOG_NAME", "local")
    )
    warehouse: str = field(
        default_factory=lambda: os.getenv("ICEBERG_WAREHOUSE", "s3://analytics-data/warehouse")
    )
    namespace: str = field(
        default_factory=lambda: os.getenv("ICEBERG_NAMESPACE", "analytics")
    )


@dataclass
class FlinkRuntimeConfig:
    """Flink runtime configuration"""
    parallelism: int = field(
        default_factory=lambda: int(os.getenv("FLINK_PARALLELISM", "1"))
    )
    checkpoint_interval: int = field(
        default_factory=lambda: int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "10000"))
    )
    checkpoint_dir: str = field(
        default_factory=lambda: os.getenv("FLINK_CHECKPOINT_DIR", "s3://analytics-data/checkpoints")
    )


@dataclass
class FlinkConfig:
    """
    Master configuration class for Flink jobs.
    
    All configuration is environment-driven for cloud portability.
    """
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    s3: S3Config = field(default_factory=S3Config)
    iceberg: IcebergConfig = field(default_factory=IcebergConfig)
    runtime: FlinkRuntimeConfig = field(default_factory=FlinkRuntimeConfig)

    @classmethod
    def from_env(cls) -> "FlinkConfig":
        """Create configuration from environment variables"""
        return cls()

    def to_dict(self) -> dict:
        """Convert to dictionary for logging/debugging"""
        return {
            "kafka": {
                "bootstrap_servers": self.kafka.bootstrap_servers,
                "topic": self.kafka.topic,
                "consumer_group": self.kafka.consumer_group,
            },
            "s3": {
                "endpoint_url": self.s3.endpoint_url,
                "region": self.s3.region,
                "bucket": self.s3.bucket,
            },
            "iceberg": {
                "catalog_name": self.iceberg.catalog_name,
                "warehouse": self.iceberg.warehouse,
                "namespace": self.iceberg.namespace,
            },
            "runtime": {
                "parallelism": self.runtime.parallelism,
                "checkpoint_interval": self.runtime.checkpoint_interval,
                "checkpoint_dir": self.runtime.checkpoint_dir,
            },
        }
