"""
CLI for the producer package
Main entry point for running the event producer
"""

import click
import time
import random
import os
from typing import Optional

from .kafka.client import KafkaClientManager
from .generators.bid_events import BidEventsGenerator
from .generators.user_events import UserEventsGenerator


@click.command()
@click.option('--local', is_flag=True, default=False,
              help='Use local Kafka (Docker) instead of AWS MSK')
@click.option('--topic', type=click.Choice(['bid-events', 'user-events', 'both']),
              help='Topic to produce to')
@click.option('--count', type=int, default=0,
              help='Number of events to produce (0 = infinite)')
@click.option('--rate', type=float, default=1.0,
              help='Events per second (approximate)')
def produce(local: bool, topic: Optional[str], count: int, rate: float):
    """Produce sample events to Kafka topics"""
    
    print("\n" + "="*60)
    print("KAFKA EVENT PRODUCER")
    print(f"Mode: {'LOCAL (Docker)' if local else 'AWS MSK'}")
    print("="*60)
    
    # Initialize Kafka client
    kafka_client = KafkaClientManager(is_local=local)
    
    # Determine topic choice
    if not topic:
        print("\nSelect topic to generate events for:")
        print("  1. bid-events (ride/booking events)")
        print("  2. user-events (user activity events)")
        print("  3. both (alternate between topics)")
        print()
        choice = click.prompt("Enter choice", type=click.Choice(['1', '2', '3']))
        topic_map = {'1': 'bid-events', '2': 'user-events', '3': 'both'}
        topic = topic_map[choice]
    
    if topic == 'both':
        produce_both_topics(kafka_client, count, rate)
    else:
        produce_single_topic(kafka_client, topic, count, rate)
    
    # Clean up
    kafka_client.close()
    print("\n✅ Producer finished")


def produce_single_topic(kafka_client: KafkaClientManager, topic_name: str, 
                        max_count: int, rate: float):
    """Produce events to a single topic"""
    
    # Select generator
    if topic_name == 'bid-events':
        generator = BidEventsGenerator()
    elif topic_name == 'user-events':
        generator = UserEventsGenerator()
    else:
        raise ValueError(f"Unknown topic: {topic_name}")
    
    # Create topic
    kafka_client.create_topic_if_not_exists(topic_name)
    
    # Get producer
    producer = kafka_client.get_producer()
    
    print(f"\n{'='*60}")
    print(f"Streaming events to topic: {topic_name}")
    print(f"{'='*60}\n")
    print("Press Ctrl+C to stop\n")
    
    count = 0
    delay = 1.0 / rate if rate > 0 else 1.0
    max_events = max_count if max_count > 0 else float('inf')
    
    try:
        while count < max_events:
            event = generator.generate()
            
            producer.send(topic_name, value=event)
            count += 1
            
            # Display event info
            if topic_name == "bid-events":
                print(f"[{count:04d}] ✓ {event['event_name']:25s} | user: {event['user_id']}")
            else:
                print(f"[{count:04d}] ✓ {event['event_type']:15s} | user: {event['user_id']} | device: {event['device_type']}")
            
            time.sleep(delay * random.uniform(0.8, 1.2))
            
    except KeyboardInterrupt:
        print(f"\n\nStopped. Sent {count} events to '{topic_name}'")


def produce_both_topics(kafka_client: KafkaClientManager, max_count: int, rate: float):
    """Produce events to both topics alternately"""
    
    # Initialize generators
    bid_gen = BidEventsGenerator()
    user_gen = UserEventsGenerator()
    
    # Create topics
    kafka_client.create_topic_if_not_exists("bid-events")
    kafka_client.create_topic_if_not_exists("user-events")
    
    # Get producer
    producer = kafka_client.get_producer()
    
    print(f"\n{'='*60}")
    print("Streaming to both topics")
    print(f"{'='*60}\n")
    print("Press Ctrl+C to stop\n")
    
    count = 0
    delay = 1.0 / rate if rate > 0 else 1.0
    max_events = max_count if max_count > 0 else float('inf')
    
    try:
        while count < max_events:
            # Alternate between topics
            if random.random() > 0.5:
                topic = "bid-events"
                event = bid_gen.generate()
                producer.send(topic, value=event)
                print(f"[{count:04d}] BID  ✓ {event['event_name']:25s} | user: {event['user_id']}")
            else:
                topic = "user-events"
                event = user_gen.generate()
                producer.send(topic, value=event)
                print(f"[{count:04d}] USER ✓ {event['event_type']:15s} | user: {event['user_id']}")
            
            count += 1
            time.sleep(delay * random.uniform(0.8, 1.2))
            
    except KeyboardInterrupt:
        print(f"\n\nStopped. Sent {count} total events")


if __name__ == "__main__":
    produce()
