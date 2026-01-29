"""
Sample Data Generator
Generates sample bid and user events for testing the Flink streaming application.
Can be used to populate Kafka topics with test data.
"""

import json
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SampleDataGenerator:
    """Generates sample streaming data for testing."""
    
    def __init__(self):
        """Initialize sample data generator."""
        self.user_ids = [f"user_{i:04d}" for i in range(1, 101)]
        self.auction_ids = [f"auction_{i:04d}" for i in range(1, 51)]
        self.countries = ["US", "UK", "CA", "AU", "DE", "FR", "JP", "ES", "IT", "NL"]
        self.device_types = ["mobile", "tablet", "desktop"]
        self.event_types = ["page_view", "click", "search", "add_to_cart", "purchase"]
        self.pages = [
            "/",
            "/home",
            "/products",
            "/product/12345",
            "/cart",
            "/checkout",
            "/account",
            "/search",
            "/category/electronics",
            "/category/clothing"
        ]
    
    def generate_bid_event(self) -> Dict:
        """
        Generate a sample bid event.
        
        Returns:
            Bid event dictionary
        """
        bid_id = f"bid_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        user_id = random.choice(self.user_ids)
        auction_id = random.choice(self.auction_ids)
        bid_amount = round(random.uniform(1.0, 5000.0), 2)
        device_type = random.choice(self.device_types)
        country = random.choice(self.countries)
        
        # Generate realistic IP address
        ip_address = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
        
        # Current timestamp
        bid_time = datetime.utcnow().isoformat() + "Z"
        
        return {
            "bid_id": bid_id,
            "user_id": user_id,
            "auction_id": auction_id,
            "bid_amount": bid_amount,
            "bid_time": bid_time,
            "device_type": device_type,
            "ip_address": ip_address,
            "country": country
        }
    
    def generate_user_event(self) -> Dict:
        """
        Generate a sample user event.
        
        Returns:
            User event dictionary
        """
        event_id = f"event_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        user_id = random.choice(self.user_ids)
        event_type = random.choice(self.event_types)
        session_id = f"session_{random.randint(10000, 99999)}"
        
        # Select random page
        page_url = f"https://example.com{random.choice(self.pages)}"
        
        # Generate referrer (sometimes empty)
        if random.random() > 0.3:
            referrer = f"https://google.com/search?q={random.choice(['products', 'deals', 'shopping'])}"
        else:
            referrer = ""
        
        # Generate user agent
        browsers = ["Chrome/120.0", "Firefox/121.0", "Safari/17.0", "Edge/120.0"]
        os_list = ["Windows NT 10.0", "Macintosh; Intel Mac OS X 10_15_7", "X11; Linux x86_64"]
        user_agent = f"Mozilla/5.0 ({random.choice(os_list)}) {random.choice(browsers)}"
        
        # Current timestamp
        event_time = datetime.utcnow().isoformat() + "Z"
        
        return {
            "event_id": event_id,
            "user_id": user_id,
            "event_type": event_type,
            "event_time": event_time,
            "page_url": page_url,
            "referrer": referrer,
            "user_agent": user_agent,
            "session_id": session_id
        }
    
    def generate_bid_events_batch(self, count: int) -> List[Dict]:
        """
        Generate a batch of bid events.
        
        Args:
            count: Number of events to generate
            
        Returns:
            List of bid events
        """
        return [self.generate_bid_event() for _ in range(count)]
    
    def generate_user_events_batch(self, count: int) -> List[Dict]:
        """
        Generate a batch of user events.
        
        Args:
            count: Number of events to generate
            
        Returns:
            List of user events
        """
        return [self.generate_user_event() for _ in range(count)]
    
    def print_sample_events(self):
        """Print sample events for inspection."""
        logger.info("=" * 80)
        logger.info("Sample Bid Event:")
        logger.info("=" * 80)
        bid_event = self.generate_bid_event()
        logger.info(json.dumps(bid_event, indent=2))
        
        logger.info("\n" + "=" * 80)
        logger.info("Sample User Event:")
        logger.info("=" * 80)
        user_event = self.generate_user_event()
        logger.info(json.dumps(user_event, indent=2))
        logger.info("=" * 80)


def write_events_to_file(events: List[Dict], filename: str):
    """
    Write events to a JSON file.
    
    Args:
        events: List of events
        filename: Output filename
    """
    with open(filename, 'w') as f:
        for event in events:
            f.write(json.dumps(event) + '\n')
    
    logger.info(f"Wrote {len(events)} events to {filename}")


def send_to_kafka(events: List[Dict], topic: str, bootstrap_servers: str = "localhost:9092"):
    """
    Send events to Kafka topic.
    Note: Requires kafka-python to be installed.
    
    Args:
        events: List of events
        topic: Kafka topic name
        bootstrap_servers: Kafka bootstrap servers
    """
    try:
        from kafka import KafkaProducer
        
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        for event in events:
            producer.send(topic, value=event)
        
        producer.flush()
        producer.close()
        
        logger.info(f"Sent {len(events)} events to Kafka topic: {topic}")
        
    except ImportError:
        logger.error("kafka-python is not installed. Cannot send to Kafka.")
        logger.error("Install it with: pip install kafka-python")
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")


def main():
    """Main function to generate and output sample data."""
    
    logger.info("Sample Data Generator")
    logger.info("=" * 80)
    
    # Create generator
    generator = SampleDataGenerator()
    
    # Print sample events
    generator.print_sample_events()
    
    # Generate batches
    logger.info("\nGenerating event batches...")
    
    bid_events = generator.generate_bid_events_batch(100)
    user_events = generator.generate_user_events_batch(100)
    
    # Write to files
    write_events_to_file(bid_events, "sample_bid_events.json")
    write_events_to_file(user_events, "sample_user_events.json")
    
    # Optionally send to Kafka (commented out by default)
    # Uncomment the lines below to send to Kafka
    
    # logger.info("\nSending events to Kafka...")
    # send_to_kafka(bid_events, "bid_events")
    # send_to_kafka(user_events, "user_events")
    
    logger.info("\nSample data generation complete!")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()