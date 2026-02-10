"""
Bid Events Generator
Generates realistic bid and ride events for testing
"""

import random
import time
from faker import Faker
from typing import Dict, Any


class BidEventsGenerator:
    """Generator for bid-events topic"""
    
    def __init__(self):
        self.fake = Faker()
        self.event_types = [
            "bid_booking_timeout", 
            "ride_requested", 
            "driver_assigned",
            "ride_started",
            "ride_completed",
            "ride_cancelled",
            "payment_completed"
        ]
        self.platforms = ["ios", "android", "web"]
        self.cities = list(range(1, 11))  # city_id 1-10
    
    def generate(self) -> Dict[str, Any]:
        """Generate a single bid event"""
        return {
            "event_name": random.choice(self.event_types),
            "user_id": random.randint(10000, 99999),
            "city_id": random.choice(self.cities),
            "platform": random.choice(self.platforms),
            "session_id": f"sess_{int(time.time())}{random.randint(1000, 9999)}",
            "timestamp_ms": int(time.time() * 1000)
        }
    
    def get_topic_name(self) -> str:
        """Get the Kafka topic name for this generator"""
        return "bid-events"
