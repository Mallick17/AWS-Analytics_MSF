#!/usr/bin/env python3
"""
DEPRECATED: Legacy Sample Data Generator for Kafka Topics

⚠️  This script is deprecated. Use the new modular producer instead:
    python -m producer.cli --topic both --count 100 --rate 2

Generates sample events for bid-events and user-events topics

Supports two modes:
- Local mode (--local): Connects to local Kafka in Docker
- MSK mode (default): Connects to AWS MSK with IAM authentication
"""

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time
import random
import sys
import os
import argparse

# Default MSK Configuration (for AWS mode)
MSK_BOOTSTRAP_SERVERS = [
    'b-1.rttestinganalyticsmsk.mbenee.c4.kafka.ap-south-1.amazonaws.com:9098',
    'b-2.rttestinganalyticsmsk.mbenee.c4.kafka.ap-south-1.amazonaws.com:9098'
]
AWS_REGION = os.getenv('AWS_REGION', 'ap-south-1')

# Local Kafka Configuration (for Docker mode)
LOCAL_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Global flag for local mode
IS_LOCAL_MODE = False


def get_msk_token_provider():
    """Get MSK token provider (only imported when needed)."""
    from kafka.sasl.oauth import AbstractTokenProvider
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    
    class MSKTokenProvider(AbstractTokenProvider):
        """Token provider for AWS MSK IAM authentication."""
        def token(self):
            token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
            return token
    
    return MSKTokenProvider()


def get_bootstrap_servers():
    """Get bootstrap servers based on mode."""
    if IS_LOCAL_MODE:
        servers = LOCAL_BOOTSTRAP_SERVERS
        return servers.split(',') if isinstance(servers, str) else servers
    return MSK_BOOTSTRAP_SERVERS


def create_producer():
    """Create Kafka producer based on mode."""
    bootstrap_servers = get_bootstrap_servers()
    
    if IS_LOCAL_MODE:
        # Local Kafka - no authentication
        print(f"📍 Connecting to LOCAL Kafka: {bootstrap_servers}")
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    else:
        # MSK with IAM authentication
        print(f"☁️  Connecting to AWS MSK: {bootstrap_servers[0]}...")
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=get_msk_token_provider(),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )


def create_admin_client():
    """Create Kafka admin client based on mode."""
    bootstrap_servers = get_bootstrap_servers()
    
    if IS_LOCAL_MODE:
        return KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    else:
        return KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=get_msk_token_provider()
        )


def create_topic_if_not_exists(topic_name: str):
    """Create Kafka topic if it doesn't exist."""
    try:
        admin = create_admin_client()
        replication = 1 if IS_LOCAL_MODE else 2
        topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=replication)
        admin.create_topics([topic])
        print(f"✓ Topic '{topic_name}' created")
        admin.close()
    except Exception as e:
        if "already exists" in str(e).lower() or "topicexistsexception" in str(e).lower():
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
    
    # Create producer using helper function
    producer = create_producer()
    
    print(f"\n{'='*60}")
    print(f"Streaming events to topic: {topic_name}")
    print(f"Mode: {'LOCAL' if IS_LOCAL_MODE else 'AWS MSK'}")
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
    global IS_LOCAL_MODE
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Kafka Event Generator')
    parser.add_argument('--local', action='store_true', 
                        help='Use local Kafka (Docker) instead of AWS MSK')
    parser.add_argument('--topic', type=str, choices=['bid-events', 'user-events', 'both'],
                        help='Topic to produce to (skips interactive prompt)')
    parser.add_argument('--count', type=int, default=0,
                        help='Number of events to produce (0 = infinite)')
    parser.add_argument('--rate', type=float, default=1.0,
                        help='Events per second (approximate)')
    args = parser.parse_args()
    
    IS_LOCAL_MODE = args.local
    
    print("\n" + "="*60)
    print("KAFKA EVENT GENERATOR")
    print(f"Mode: {'LOCAL (Docker)' if IS_LOCAL_MODE else 'AWS MSK'}")
    print("="*60)
    
    # Determine topic choice
    if args.topic:
        choice = {'bid-events': '1', 'user-events': '2', 'both': '3'}[args.topic]
    else:
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
        
        # Create producer using helper function
        producer = create_producer()
        
        print(f"\n{'='*60}")
        print("Streaming to both topics")
        print(f"Mode: {'LOCAL' if IS_LOCAL_MODE else 'AWS MSK'}")
        print(f"{'='*60}\n")
        
        count = 0
        max_count = args.count if args.count > 0 else float('inf')
        delay = 1.0 / args.rate if args.rate > 0 else 1.0
        
        try:
            while count < max_count:
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
                time.sleep(delay * random.uniform(0.5, 1.5))
                
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