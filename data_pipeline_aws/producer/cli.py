#!/usr/bin/env python3
"""
Analytics Producer CLI

Command-line interface for generating and publishing events to Kafka.
Supports multiple event types: orders, bids, users.

Usage:
    python cli.py produce orders --rate 10 --count 100
    python cli.py produce bids --rate 5 --count 50
    python cli.py produce users --rate 15 --count 200
    python cli.py produce all --rate 10 --count 100
"""

import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

import click
import yaml
from dotenv import load_dotenv

from generators.order_generator import OrderGenerator
from generators.bid_event_generator import BidEventGenerator
from generators.user_event_generator import UserEventGenerator
from kafka.producer import KafkaEventProducer

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config() -> dict:
    """Load configuration from config.yaml with environment variable substitution."""
    config_path = Path(__file__).parent / "config.yaml"
    
    with open(config_path) as f:
        config_str = f.read()
    
    # Substitute environment variables
    for key, value in os.environ.items():
        config_str = config_str.replace(f"${{{key}}}", value)
        config_str = config_str.replace(f"${{{key}:-", f"{{{key}:-")
    
    # Handle default values
    import re
    pattern = r'\$\{(\w+):-([^}]+)\}'
    
    def replace_with_default(match):
        env_var = match.group(1)
        default = match.group(2)
        return os.environ.get(env_var, default)
    
    config_str = re.sub(pattern, replace_with_default, config_str)
    
    return yaml.safe_load(config_str)


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def cli(verbose: bool):
    """Analytics Producer - Generate events for Flink Analytics Platform"""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


@cli.group()
def produce():
    """Produce events to Kafka"""
    pass


@produce.command("orders")
@click.option("--rate", "-r", type=float, default=None, help="Events per second")
@click.option("--count", "-c", type=int, default=None, help="Number of events")
@click.option("--seed", "-s", type=int, default=None, help="Random seed")
@click.option("--dry-run", is_flag=True, help="Generate without sending to Kafka")
def produce_orders(rate: Optional[float], count: Optional[int], seed: Optional[int], dry_run: bool):
    """Produce order creation events"""
    config = load_config()
    
    # Get configuration
    rate = rate or config['producer']['default_rate']
    count = count or config['producer']['default_count']
    seed = seed or config['producer']['seed']
    
    topic_config = config['topics']['orders.created.v1']
    
    logger.info(f"Generating order events (rate={rate}/s, count={count}, seed={seed})")
    
    # Create generator
    generator = OrderGenerator(
        seed=seed,
        currencies=topic_config['currencies'],
        min_amount=topic_config['min_amount'],
        max_amount=topic_config['max_amount']
    )
    
    if dry_run:
        logger.info("DRY RUN MODE - Events will not be sent to Kafka")
        for i, event in enumerate(generator.generate_stream(count), 1):
            logger.info(f"Event {i}: {event.to_json()}")
            time.sleep(1 / rate)
    else:
        producer = KafkaEventProducer(
            bootstrap_servers=config['kafka']['bootstrap_servers'],
            topic='orders.created.v1'
        )
        
        sent = 0
        for event in generator.generate_stream(count):
            producer.send(event.to_json())
            sent += 1
            if sent % 100 == 0:
                logger.info(f"Sent {sent} events...")
            time.sleep(1 / rate)
        
        producer.flush()
        logger.info(f"✓ Successfully sent {sent} order events")


@produce.command("bids")
@click.option("--rate", "-r", type=float, default=None, help="Events per second")
@click.option("--count", "-c", type=int, default=None, help="Number of events")
@click.option("--seed", "-s", type=int, default=None, help="Random seed")
@click.option("--dry-run", is_flag=True, help="Generate without sending to Kafka")
def produce_bids(rate: Optional[float], count: Optional[int], seed: Optional[int], dry_run: bool):
    """Produce bid events"""
    config = load_config()
    
    rate = rate or config['producer']['default_rate']
    count = count or config['producer']['default_count']
    seed = seed or config['producer']['seed']
    
    topic_config = config['topics']['bid-events']
    
    logger.info(f"Generating bid events (rate={rate}/s, count={count}, seed={seed})")
    
    generator = BidEventGenerator(
        seed=seed,
        platforms=topic_config['platforms'],
        cities=topic_config['cities']
    )
    
    if dry_run:
        logger.info("DRY RUN MODE - Events will not be sent to Kafka")
        for i, event in enumerate(generator.generate_stream(count), 1):
            logger.info(f"Event {i}: {event.to_json()}")
            time.sleep(1 / rate)
    else:
        producer = KafkaEventProducer(
            bootstrap_servers=config['kafka']['bootstrap_servers'],
            topic='bid-events'
        )
        
        sent = 0
        for event in generator.generate_stream(count):
            producer.send(event.to_json())
            sent += 1
            if sent % 100 == 0:
                logger.info(f"Sent {sent} events...")
            time.sleep(1 / rate)
        
        producer.flush()
        logger.info(f"✓ Successfully sent {sent} bid events")


@produce.command("users")
@click.option("--rate", "-r", type=float, default=None, help="Events per second")
@click.option("--count", "-c", type=int, default=None, help="Number of events")
@click.option("--seed", "-s", type=int, default=None, help="Random seed")
@click.option("--dry-run", is_flag=True, help="Generate without sending to Kafka")
def produce_users(rate: Optional[float], count: Optional[int], seed: Optional[int], dry_run: bool):
    """Produce user activity events"""
    config = load_config()
    
    rate = rate or config['producer']['default_rate']
    count = count or config['producer']['default_count']
    seed = seed or config['producer']['seed']
    
    topic_config = config['topics']['user-events']
    
    logger.info(f"Generating user events (rate={rate}/s, count={count}, seed={seed})")
    
    generator = UserEventGenerator(
        seed=seed,
        event_types=topic_config['event_types'],
        device_types=topic_config['device_types'],
        pages=topic_config['pages']
    )
    
    if dry_run:
        logger.info("DRY RUN MODE - Events will not be sent to Kafka")
        for i, event in enumerate(generator.generate_stream(count), 1):
            logger.info(f"Event {i}: {event.to_json()}")
            time.sleep(1 / rate)
    else:
        producer = KafkaEventProducer(
            bootstrap_servers=config['kafka']['bootstrap_servers'],
            topic='user-events'
        )
        
        sent = 0
        for event in generator.generate_stream(count):
            producer.send(event.to_json())
            sent += 1
            if sent % 100 == 0:
                logger.info(f"Sent {sent} events...")
            time.sleep(1 / rate)
        
        producer.flush()
        logger.info(f"✓ Successfully sent {sent} user events")


@produce.command("all")
@click.option("--rate", "-r", type=float, default=None, help="Events per second per topic")
@click.option("--count", "-c", type=int, default=None, help="Number of events per topic")
@click.option("--seed", "-s", type=int, default=None, help="Random seed")
def produce_all(rate: Optional[float], count: Optional[int], seed: Optional[int]):
    """Produce events to all topics simultaneously"""
    config = load_config()
    
    rate = rate or config['producer']['default_rate']
    count = count or config['producer']['default_count']
    
    logger.info(f"Producing to all topics (rate={rate}/s, count={count} per topic)")
    
    # Start producers for all topics
    import threading
    
    def produce_topic(topic_name, generator_class, topic_config_key, generator_kwargs):
        generator = generator_class(seed=seed, **generator_kwargs)
        producer = KafkaEventProducer(
            bootstrap_servers=config['kafka']['bootstrap_servers'],
            topic=topic_name
        )
        
        sent = 0
        for event in generator.generate_stream(count):
            producer.send(event.to_json())
            sent += 1
            time.sleep(1 / rate)
        
        producer.flush()
        logger.info(f"✓ {topic_name}: Sent {sent} events")
    
    threads = [
        threading.Thread(
            target=produce_topic,
            args=('orders.created.v1', OrderGenerator, 'orders.created.v1', {
                'currencies': config['topics']['orders.created.v1']['currencies'],
                'min_amount': config['topics']['orders.created.v1']['min_amount'],
                'max_amount': config['topics']['orders.created.v1']['max_amount']
            })
        ),
        threading.Thread(
            target=produce_topic,
            args=('bid-events', BidEventGenerator, 'bid-events', {
                'platforms': config['topics']['bid-events']['platforms'],
                'cities': config['topics']['bid-events']['cities']
            })
        ),
        threading.Thread(
            target=produce_topic,
            args=('user-events', UserEventGenerator, 'user-events', {
                'event_types': config['topics']['user-events']['event_types'],
                'device_types': config['topics']['user-events']['device_types'],
                'pages': config['topics']['user-events']['pages']
            })
        ),
    ]
    
    for thread in threads:
        thread.start()
    
    for thread in threads:
        thread.join()
    
    logger.info("✓ All topics completed")


if __name__ == "__main__":
    cli()
