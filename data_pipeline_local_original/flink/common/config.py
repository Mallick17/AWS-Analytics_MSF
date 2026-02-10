"""
Flink Configuration Manager

Environment-driven configuration for cloud-ready Flink jobs.
No hardcoded local paths or Docker-specific settings.
"""

import os
from dataclasses import dataclass, field

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
    starting_offset: str = "earliest-offset"


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
        default_factory=lambda: os.getenv("FLINK_CHECKPOINT_DIR", "file:///opt/flink/checkpoints")
    )


@dataclass
class FlinkConfig:
    """
    Master configuration class for Flink jobs.
    
    All configuration is environment-driven for cloud portability.
    """
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
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
            "runtime": {
                "parallelism": self.runtime.parallelism,
                "checkpoint_interval": self.runtime.checkpoint_interval,
                "checkpoint_dir": self.runtime.checkpoint_dir,
            },
        }
