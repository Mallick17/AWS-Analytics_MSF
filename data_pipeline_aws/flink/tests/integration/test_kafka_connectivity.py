"""
Integration tests for Kafka connectivity

These tests require Kafka to be running.
Run with: pytest tests/integration/ -v
"""

import json
import os
import pytest
import time

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable


# Skip if Kafka is not available
def kafka_available():
    """Check if Kafka is available"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            api_version_auto_timeout_ms=5000
        )
        producer.close()
        return True
    except NoBrokersAvailable:
        return False


pytestmark = pytest.mark.skipif(
    not kafka_available(),
    reason="Kafka not available"
)


class TestKafkaConnectivity:
    """Integration tests for Kafka connectivity"""

    @pytest.fixture
    def kafka_config(self):
        """Kafka configuration fixture"""
        return {
            "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "topic": "test.integration.v1"
        }

    def test_producer_connection(self, kafka_config):
        """Test producer can connect to Kafka"""
        producer = KafkaProducer(
            bootstrap_servers=kafka_config["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        assert producer.bootstrap_connected()
        producer.close()

    def test_produce_and_consume(self, kafka_config):
        """Test producing and consuming a message"""
        topic = kafka_config["topic"]
        test_message = {"test": "message", "timestamp": time.time()}

        # Produce
        producer = KafkaProducer(
            bootstrap_servers=kafka_config["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        future = producer.send(topic, test_message)
        result = future.get(timeout=10)
        producer.close()

        assert result.topic == topic

        # Consume
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_config["bootstrap_servers"],
            auto_offset_reset="earliest",
            consumer_timeout_ms=10000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )

        messages = []
        for message in consumer:
            messages.append(message.value)
            if message.value.get("test") == "message":
                break

        consumer.close()

        assert len(messages) > 0
        assert any(m.get("test") == "message" for m in messages)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
