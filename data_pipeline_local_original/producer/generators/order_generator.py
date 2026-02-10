"""
Order Event Generator

Generates realistic order creation events using Faker.
Supports deterministic generation via seed for reproducible testing.
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterator, Optional

from faker import Faker

from schemas.order_created_v1 import OrderCreatedV1, OrderStatus


class OrderGenerator:
    """
    Generates realistic order creation events.
    
    Attributes:
        faker: Faker instance for generating fake data
        currencies: List of supported currency codes
        min_amount: Minimum order amount
        max_amount: Maximum order amount
    """

    def __init__(
        self,
        seed: Optional[int] = None,
        currencies: Optional[list[str]] = None,
        min_amount: float = 10.00,
        max_amount: float = 5000.00,
    ):
        """
        Initialize the order generator.
        
        Args:
            seed: Random seed for deterministic generation
            currencies: List of currency codes to use
            min_amount: Minimum order amount
            max_amount: Maximum order amount
        """
        self.faker = Faker()
        if seed is not None:
            Faker.seed(seed)
            self.faker = Faker()
        
        self.currencies = currencies or ["USD", "EUR", "GBP", "INR"]
        self.min_amount = min_amount
        self.max_amount = max_amount

    def generate_one(self) -> OrderCreatedV1:
        """
        Generate a single order creation event.
        
        Returns:
            OrderCreatedV1: A validated order event
        """
        amount = Decimal(str(
            self.faker.pyfloat(
                min_value=self.min_amount,
                max_value=self.max_amount,
                right_digits=2,
                positive=True
            )
        )).quantize(Decimal("0.01"))

        return OrderCreatedV1(
            event_id=uuid.uuid4(),
            event_time=datetime.now(timezone.utc),
            order_id=f"ORD-{self.faker.uuid4()[:8].upper()}",
            user_id=f"USR-{self.faker.uuid4()[:8].upper()}",
            order_amount=amount,
            currency=self.faker.random_element(self.currencies),
            order_status=OrderStatus.CREATED
        )

    def generate_batch(self, count: int) -> list[OrderCreatedV1]:
        """
        Generate a batch of order creation events.
        
        Args:
            count: Number of events to generate
            
        Returns:
            List of OrderCreatedV1 events
        """
        return [self.generate_one() for _ in range(count)]

    def generate_stream(self, count: Optional[int] = None) -> Iterator[OrderCreatedV1]:
        """
        Generate a stream of order creation events.
        
        Args:
            count: Number of events to generate (None for infinite)
            
        Yields:
            OrderCreatedV1 events
        """
        generated = 0
        while count is None or generated < count:
            yield self.generate_one()
            generated += 1
