"""
User Event Generator

Generates realistic user activity events using Faker.
"""

import uuid
import time
from typing import Iterator, Optional

from faker import Faker

from ..schemas.user_event import UserEvent


class UserEventGenerator:
    """
    Generates realistic user activity events.
    
    Attributes:
        faker: Faker instance for generating fake data
        event_types: List of event types
        device_types: List of device types
        pages: List of page URLs
    """

    def __init__(
        self,
        seed: Optional[int] = None,
        event_types: Optional[list[str]] = None,
        device_types: Optional[list[str]] = None,
        pages: Optional[list[str]] = None,
    ):
        """
        Initialize the user event generator.
        
        Args:
            seed: Random seed for deterministic generation
            event_types: List of event type names
            device_types: List of device types
            pages: List of page URLs
        """
        self.faker = Faker()
        if seed is not None:
            Faker.seed(seed)
            self.faker = Faker()
        
        self.event_types = event_types or ["PAGE_VIEW", "BUTTON_CLICK", "FORM_SUBMIT", "SCROLL", "VIDEO_PLAY"]
        self.device_types = device_types or ["desktop", "mobile", "tablet", "smartphone"]
        self.pages = pages or ["/home", "/products", "/cart", "/checkout", "/profile"]

    def generate_one(self) -> UserEvent:
        """
        Generate a single user event.
        
        Returns:
            UserEvent: A validated user event
        """
        return UserEvent(
            event_type=self.faker.random_element(self.event_types),
            user_id=self.faker.random_int(min=1000, max=999999),
            page_url=self.faker.random_element(self.pages),
            device_type=self.faker.random_element(self.device_types),
            session_id=str(uuid.uuid4()),
            timestamp_ms=int(time.time() * 1000)
        )

    def generate_batch(self, count: int) -> list[UserEvent]:
        """
        Generate a batch of user events.
        
        Args:
            count: Number of events to generate
            
        Returns:
            List of UserEvent events
        """
        return [self.generate_one() for _ in range(count)]

    def generate_stream(self, count: Optional[int] = None) -> Iterator[UserEvent]:
        """
        Generate a stream of user events.
        
        Args:
            count: Number of events to generate (None for infinite)
            
        Yields:
            UserEvent events
        """
        generated = 0
        while count is None or generated < count:
            yield self.generate_one()
            generated += 1
