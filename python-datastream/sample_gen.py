#!/usr/bin/env python3
"""
Sample Data Generator for Kafka Topics
Generates sample events for bid-events and user-events topics
"""

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json
import time
import random
import sys

# MSK Configuration
BOOTSTRAP_SERVERS = [
    'b-1.rttestinganalyticsmsk.mbenee.c4.kafka.ap-south-1.amazonaws.com:9098',
    'b-2.rttestinganalyticsmsk.mbenee.c4.kafka.ap-south-1.amazonaws.com:9098'
]
AWS_REGION = 'ap-south-1'


class MSKTokenProvider(AbstractTokenProvider):
    """Token provider for AWS MSK IAM authentication."""
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token


def create_topic_if_not_exists(topic_name: str):
    """Create Kafka topic if it doesn't exist."""
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=MSKTokenProvider()
        )
        
        topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=2)
        admin.create_topics([topic])
        print(f"✓ Topic '{topic_name}' created")
        admin.close()
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"✓ Topic '{topic_name}' already exists")
        else:
            print(f"⚠ Topic creation error: {e}")


def generate_bid_event() -> dict:
    """Generate a sample bid event."""
    return {
        "event_name": random.choice([
            "bid_booking_timeout", 
            "ride_requested", 
            "driver_assigned",
            "ride_started",
            "ride_completed"
        ]),
        "user_id": random.randint(10000, 99999),
        "city_id": random.randint(1, 10),
        "platform": random.choice(["ios", "android", "web"]),
        "session_id": f"sess_{int(time.time())}{random.randint(1000, 9999)}",
        "timestamp_ms": int(time.time() * 1000)
    }


def generate_user_event() -> dict:
    """Generate a sample user event."""
    pages = [
        "/home",
        "/products",
        "/product/12345",
        "/cart",
        "/checkout",
        "/account",
        "/search",
        "/category/electronics"
    ]
    
    return {
        "event_type": random.choice([
            "page_view",
            "click",
            "scroll",
            "form_submit",
            "add_to_cart",
            "purchase"
        ]),
        "user_id": random.randint(10000, 99999),
        "page_url": f"https://example.com{random.choice(pages)}",
        "device_type": random.choice(["ios", "android", "web", "desktop", "tablet"]),
        "session_id": f"sess_{int(time.time())}{random.randint(1000, 9999)}",
        "timestamp_ms": int(time.time() * 1000)
    }


def stream_events(topic_name: str, event_generator):
    """Stream events to Kafka topic."""
    
    # Create topic if not exists
    create_topic_if_not_exists(topic_name)
    
    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=MSKTokenProvider(),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"\n{'='*60}")
    print(f"Streaming events to topic: {topic_name}")
    print(f"{'='*60}\n")
    print("Press Ctrl+C to stop\n")
    
    count = 0
    try:
        while True:
            event = event_generator()
            
            producer.send(topic_name, value=event)
            count += 1
            
            # Display event info
            if topic_name == "bid-events":
                print(f"[{count:04d}] ✓ {event['event_name']:25s} | user: {event['user_id']}")
            else:
                print(f"[{count:04d}] ✓ {event['event_type']:15s} | user: {event['user_id']} | device: {event['device_type']}")
            
            time.sleep(random.uniform(0.5, 2))
            
    except KeyboardInterrupt:
        print(f"\n\nStopped. Sent {count} events to '{topic_name}'")
    finally:
        producer.flush()
        producer.close()


def main():
    """Main function."""
    print("\n" + "="*60)
    print("KAFKA EVENT GENERATOR")
    print("="*60)
    print("\nSelect topic to generate events for:")
    print("  1. bid-events (ride/booking events)")
    print("  2. user-events (user activity events)")
    print("  3. both (alternate between topics)")
    print()
    
    choice = input("Enter choice (1/2/3): ").strip()
    
    if choice == "1":
        stream_events("bid-events", generate_bid_event)
    elif choice == "2":
        stream_events("user-events", generate_user_event)
    elif choice == "3":
        print("\nStreaming to both topics (press Ctrl+C to stop)...")
        
        # Create topics
        create_topic_if_not_exists("bid-events")
        create_topic_if_not_exists("user-events")
        
        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=MSKTokenProvider(),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        print(f"\n{'='*60}")
        print("Streaming to both topics")
        print(f"{'='*60}\n")
        
        count = 0
        try:
            while True:
                # Alternate between topics
                if random.random() > 0.5:
                    topic = "bid-events"
                    event = generate_bid_event()
                    producer.send(topic, value=event)
                    print(f"[{count:04d}] BID  ✓ {event['event_name']:25s} | user: {event['user_id']}")
                else:
                    topic = "user-events"
                    event = generate_user_event()
                    producer.send(topic, value=event)
                    print(f"[{count:04d}] USER ✓ {event['event_type']:15s} | user: {event['user_id']}")
                
                count += 1
                time.sleep(random.uniform(0.5, 2))
                
        except KeyboardInterrupt:
            print(f"\n\nStopped. Sent {count} total events")
        finally:
            producer.flush()
            producer.close()
    else:
        print("Invalid choice. Exiting.")
        sys.exit(1)


if __name__ == "__main__":
    main()