"""
Bid Event Generator

Generates realistic bid events using Faker.
"""

import uuid
import time
from typing import Iterator, Optional

from faker import Faker

from ..schemas.bid_event import BidEvent


class BidEventGenerator:
    """
    Generates realistic bid events.
    
    Attributes:
        faker: Faker instance for generating fake data
        platforms: List of supported platforms
        cities: List of city IDs
    """

    def __init__(
        self,
        seed: Optional[int] = None,
        platforms: Optional[list[str]] = None,
        cities: Optional[list[int]] = None,
    ):
        """
        Initialize the bid event generator.
        
        Args:
            seed: Random seed for deterministic generation
            platforms: List of platform names
            cities: List of city IDs
        """
        self.faker = Faker()
        if seed is not None:
            Faker.seed(seed)
            self.faker = Faker()
        
        self.platforms = platforms or ["web", "android", "ios", "mobile", "desktop"]
        self.cities = cities or [1, 2, 3, 4, 5]
        self.event_names = ["BID_PLACED", "BID_WON", "BID_LOST", "BID_UPDATED"]

    def generate_one(self) -> BidEvent:
        """
        Generate a single bid event.
        
        Returns:
            BidEvent: A validated bid event
        """
        return BidEvent(
            event_name=self.faker.random_element(self.event_names),
            user_id=self.faker.random_int(min=1000, max=999999),
            city_id=self.faker.random_element(self.cities),
            platform=self.faker.random_element(self.platforms),
            session_id=str(uuid.uuid4()),
            timestamp_ms=int(time.time() * 1000)
        )

    def generate_batch(self, count: int) -> list[BidEvent]:
        """
        Generate a batch of bid events.
        
        Args:
            count: Number of events to generate
            
        Returns:
            List of BidEvent events
        """
        return [self.generate_one() for _ in range(count)]

    def generate_stream(self, count: Optional[int] = None) -> Iterator[BidEvent]:
        """
        Generate a stream of bid events.
        
        Args:
            count: Number of events to generate (None for infinite)
            
        Yields:
            BidEvent events
        """
        generated = 0
        while count is None or generated < count:
            yield self.generate_one()
            generated += 1
