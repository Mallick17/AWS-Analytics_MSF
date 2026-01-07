import json
import logging
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from schemas.order_created_v1 import OrderCreatedV1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaEventProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        acks: str = "all",
        retries: int = 3,
        linger_ms: int = 10,
        batch_size: int = 16384,
    ):
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

    def connect(self):
        if self._producer is None:
            self._producer = KafkaProducer(**self._config)

    def send_order_event(self, event: OrderCreatedV1):
        self.connect()
        self._producer.send(
            self.topic,
            key=event.order_id,
            value=event.to_json()
        )
        self._producer.flush()


if __name__ == "__main__":
    producer = KafkaEventProducer(
        bootstrap_servers="kafka:9092",
        topic="orders"
    )

    i = 1
    while True:
        event = OrderCreatedV1(
            order_id=str(i),
            user_id=f"user_{i}",
            order_amount=100.0 + i,
            currency="USD",
            order_status="CREATED"
        )
        producer.send_order_event(event)
        logger.info(f"Sent event {event}")
        i += 1
