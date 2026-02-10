"""
Kafka Event Producer

Handles publishing events to Kafka with proper serialization and error handling.
"""

import json
import logging
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from ..schemas.order_created_v1 import OrderCreatedV1

logger = logging.getLogger(__name__)


class KafkaEventProducer:
    """
    Kafka producer for publishing events.
    
    Handles connection management, serialization, and delivery confirmation.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        acks: str = "all",
        retries: int = 3,
        linger_ms: int = 10,
        batch_size: int = 16384,
    ):
        """
        Initialize the Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Default topic to publish to
            acks: Acknowledgment level ('all', '1', '0')
            retries: Number of retries on failure
            linger_ms: Time to wait for batching
            batch_size: Maximum batch size in bytes
        """
        self.topic = topic
        self._producer: Optional[KafkaProducer] = None
        self._config = {
            "bootstrap_servers": bootstrap_servers.split(","),
            "acks": acks,
            "retries": retries,
            "linger_ms": linger_ms,
            "batch_size": batch_size,
            "key_serializer": lambda k: k.encode("utf-8") if k else None,
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        }
        self._delivery_count = 0
        self._error_count = 0

    def connect(self) -> None:
        """Establish connection to Kafka."""
        if self._producer is None:
            logger.info(f"Connecting to Kafka: {self._config['bootstrap_servers']}")
            self._producer = KafkaProducer(**self._config)
            logger.info("Connected to Kafka successfully")

    def close(self) -> None:
        """Close the Kafka connection."""
        if self._producer:
            logger.info("Closing Kafka producer...")
            self._producer.flush()
            self._producer.close()
            self._producer = None
            logger.info("Kafka producer closed")

    def _on_success(self, record_metadata) -> None:
        """Callback for successful delivery."""
        self._delivery_count += 1
        logger.debug(
            f"Delivered to {record_metadata.topic}[{record_metadata.partition}] "
            f"@ offset {record_metadata.offset}"
        )

    def _on_error(self, exception: Exception) -> None:
        """Callback for delivery failure."""
        self._error_count += 1
        logger.error(f"Delivery failed: {exception}")

    def send_order_event(
        self,
        event: OrderCreatedV1,
        topic: Optional[str] = None,
    ) -> bool:
        """
        Send an order event to Kafka.
        
        Args:
            event: The order event to send
            topic: Override topic (uses default if None)
            
        Returns:
            True if send was initiated successfully
        """
        if self._producer is None:
            self.connect()

        target_topic = topic or self.topic
        key = event.order_id  # Use order_id as partition key

        try:
            future = self._producer.send(
                target_topic,
                key=key,
                value=event.to_json()
            )
            future.add_callback(self._on_success)
            future.add_errback(self._on_error)
            return True
        except KafkaError as e:
            logger.error(f"Failed to send event: {e}")
            self._error_count += 1
            return False

    def flush(self, timeout: Optional[float] = None) -> None:
        """Flush pending messages."""
        if self._producer:
            self._producer.flush(timeout=timeout)

    @property
    def stats(self) -> dict:
        """Get delivery statistics."""
        return {
            "delivered": self._delivery_count,
            "errors": self._error_count,
        }

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
