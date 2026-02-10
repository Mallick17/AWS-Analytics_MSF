"""
User Events Generator
Generates realistic user activity events for testing
"""

import random
import time
from faker import Faker
from typing import Dict, Any


class UserEventsGenerator:
    """Generator for user-events topic"""
    
    def __init__(self):
        self.fake = Faker()
        self.event_types = [
            "page_view",
            "click",
            "scroll",
            "form_submit",
            "add_to_cart",
            "purchase",
            "search",
            "login",
            "logout"
        ]
        self.device_types = ["ios", "android", "web", "desktop", "tablet", "mobile"]
        self.pages = [
            "/home",
            "/products",
            "/product/12345",
            "/cart",
            "/checkout",
            "/account",
            "/search",
            "/category/electronics",
            "/about",
            "/contact"
        ]
    
    def generate(self) -> Dict[str, Any]:
        """Generate a single user event"""
        return {
            "event_type": random.choice(self.event_types),
            "user_id": random.randint(10000, 99999),
            "page_url": f"https://example.com{random.choice(self.pages)}",
            "device_type": random.choice(self.device_types),
            "session_id": f"sess_{int(time.time())}{random.randint(1000, 9999)}",
            "timestamp_ms": int(time.time() * 1000)
        }
    
    def get_topic_name(self) -> str:
        """Get the Kafka topic name for this generator"""
        return "user-events"
