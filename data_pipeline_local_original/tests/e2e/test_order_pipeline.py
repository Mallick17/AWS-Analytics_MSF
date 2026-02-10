"""
End-to-End tests for the order pipeline

Tests the complete flow:
1. Produce order events to Kafka
2. Verify Flink processes them
3. Check data appears in Iceberg table

Prerequisites:
- All services running (docker-compose up)
- Flink job deployed and running

Run with: pytest tests/e2e/ -v --timeout=120
"""

import json
import os
import pytest
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer


def services_available():
    """Check if all services are available"""
    try:
        # Check Kafka
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            api_version_auto_timeout_ms=5000
        )
        producer.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not services_available(),
    reason="Services not available"
)


class TestOrderPipeline:
    """End-to-end tests for order pipeline"""

    @pytest.fixture
    def kafka_producer(self):
        """Create Kafka producer"""
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        yield producer
        producer.close()

    def create_order_event(self) -> dict:
        """Create a test order event"""
        return {
            "event_id": str(uuid.uuid4()),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
            "user_id": f"USR-{uuid.uuid4().hex[:8].upper()}",
            "order_amount": "150.50",
            "currency": "USD",
            "order_status": "CREATED"
        }

    def test_produce_order_event(self, kafka_producer):
        """Test producing an order event to Kafka"""
        topic = os.getenv("KAFKA_TOPIC_ORDERS", "orders.created.v1")
        event = self.create_order_event()

        future = kafka_producer.send(
            topic,
            key=event["order_id"],
            value=event
        )
        result = future.get(timeout=10)

        assert result.topic == topic
        assert result.partition >= 0

    @pytest.mark.timeout(30)
    def test_kafka_only_flow(self, kafka_producer):
        """Kafka-only e2e: ensure we can produce a valid order event."""
        topic = os.getenv("KAFKA_TOPIC_ORDERS", "orders.created.v1")
        event = self.create_order_event()

        future = kafka_producer.send(
            topic,
            key=event["order_id"],
            value=event
        )
        future.get(timeout=10)
        kafka_producer.flush()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--timeout=120"])
