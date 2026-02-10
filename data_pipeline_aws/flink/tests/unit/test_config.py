"""
Unit tests for Flink configuration
"""

import os
import pytest

import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from flink.common.config import FlinkConfig, KafkaConfig, S3Config, IcebergConfig


class TestKafkaConfig:
    """Tests for KafkaConfig"""

    def test_default_values(self):
        """Test default configuration values"""
        config = KafkaConfig()

        assert config.bootstrap_servers == "localhost:9092"
        assert config.topic == "orders.created.v1"
        assert config.consumer_group == "flink-order-ingest"
        assert config.starting_offset == "earliest"

    def test_env_override(self, monkeypatch):
        """Test environment variable override"""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("KAFKA_TOPIC_ORDERS", "test.topic")

        config = KafkaConfig()

        assert config.bootstrap_servers == "kafka:9092"
        assert config.topic == "test.topic"


class TestS3Config:
    """Tests for S3Config"""

    def test_default_values(self):
        """Test default configuration values"""
        config = S3Config()

        assert config.endpoint_url == "http://localhost:4566"
        assert config.access_key == "test"
        assert config.secret_key == "test"
        assert config.region == "us-east-1"
        assert config.bucket == "analytics-data"

    def test_env_override(self, monkeypatch):
        """Test environment variable override"""
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://s3.amazonaws.com")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "real-key")

        config = S3Config()

        assert config.endpoint_url == "http://s3.amazonaws.com"
        assert config.access_key == "real-key"


class TestIcebergConfig:
    """Tests for IcebergConfig"""

    def test_default_values(self):
        """Test default configuration values"""
        config = IcebergConfig()

        assert config.catalog_name == "local"
        assert config.warehouse == "s3://analytics-data/warehouse"
        assert config.namespace == "analytics"


class TestFlinkConfig:
    """Tests for FlinkConfig master class"""

    def test_from_env(self):
        """Test creating config from environment"""
        config = FlinkConfig.from_env()

        assert config.kafka is not None
        assert config.s3 is not None
        assert config.iceberg is not None
        assert config.runtime is not None

    def test_to_dict(self):
        """Test converting config to dictionary"""
        config = FlinkConfig.from_env()
        config_dict = config.to_dict()

        assert "kafka" in config_dict
        assert "s3" in config_dict
        assert "iceberg" in config_dict
        assert "runtime" in config_dict

        assert "bootstrap_servers" in config_dict["kafka"]
        assert "endpoint_url" in config_dict["s3"]
        assert "warehouse" in config_dict["iceberg"]
        assert "parallelism" in config_dict["runtime"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
