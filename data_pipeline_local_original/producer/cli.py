#!/usr/bin/env python3
"""
Analytics Producer CLI

Command-line interface for generating and publishing events to Kafka.

Usage:
    python cli.py produce orders --rate 10 --count 100
    python cli.py produce orders --rate 5 --seed 42
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
from kafka_client.producer import KafkaEventProducer

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
@click.option(
    "--rate",
    "-r",
    type=float,
    default=None,
    help="Events per second (default: from config)"
)
@click.option(
    "--count",
    "-c",
    type=int,
    default=None,
    help="Number of events to produce (default: from config, 0 for infinite)"
)
@click.option(
    "--seed",
    "-s",
    type=int,
    default=None,
    help="Random seed for deterministic generation"
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Generate events without sending to Kafka"
)
def produce_orders(
    rate: Optional[float],
    count: Optional[int],
    seed: Optional[int],
    dry_run: bool
):
    """
    Produce order creation events.
    
    Examples:
        python cli.py produce orders --rate 10 --count 100
        python cli.py produce orders --rate 5 --seed 42
        python cli.py produce orders --dry-run --count 5
    """
    config = load_config()
    
    # Apply config defaults
    rate = rate or config["producer"]["default_rate"]
    count = count if count is not None else config["producer"]["default_count"]
    seed = seed or config["producer"].get("seed")
    
    # Initialize generator
    generator = OrderGenerator(
        seed=seed,
        currencies=config["order"]["currencies"],
        min_amount=config["order"]["min_amount"],
        max_amount=config["order"]["max_amount"],
    )
    
    click.echo(f"Starting order event producer...")
    click.echo(f"  Rate: {rate} events/second")
    click.echo(f"  Count: {count if count else 'infinite'}")
    click.echo(f"  Seed: {seed if seed else 'random'}")
    click.echo(f"  Dry run: {dry_run}")
    click.echo()
    
    if dry_run:
        # Dry run mode - just print events
        for i, event in enumerate(generator.generate_stream(count or 5)):
            click.echo(f"[{i+1}] {event.to_json()}")
            if rate:
                time.sleep(1 / rate)
        return
    
    # Real mode - send to Kafka
    kafka_config = config["kafka"]
    
    # Extract actual string values in case substitution produced dicts
    bootstrap_servers = kafka_config["bootstrap_servers"]
    topic = kafka_config["topic"]
    
    # Handle bootstrap_servers
    if isinstance(bootstrap_servers, dict):
        # Try common keys or take first value
        bootstrap_servers = (
            bootstrap_servers.get("servers") or
            bootstrap_servers.get("bootstrap_servers") or
            next(iter(bootstrap_servers.values()), None)
        )
    if not bootstrap_servers or bootstrap_servers == "None":
        bootstrap_servers = "localhost:9092"
    
    # Handle topic
    if isinstance(topic, dict):
        topic = (
            topic.get("topic") or
            topic.get("name") or
            next(iter(topic.values()), None)
        )
    if not topic or topic == "None":
        topic = "orders.created.v1"
    
    try:
        with KafkaEventProducer(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            acks=kafka_config["acks"],
            retries=kafka_config["retries"],
            linger_ms=kafka_config["linger_ms"],
            batch_size=kafka_config["batch_size"],
        ) as producer:
            
            click.echo(f"Connected to Kafka: {bootstrap_servers}")
            click.echo(f"Publishing to topic: {topic}")
            click.echo()
            
            interval = 1 / rate if rate > 0 else 0
            produced = 0
            
            try:
                for event in generator.generate_stream(count if count else None):
                    producer.send_order_event(event)
                    produced += 1
                    
                    if produced % 10 == 0:
                        stats = producer.stats
                        click.echo(
                            f"Progress: {produced} sent, "
                            f"{stats['delivered']} delivered, "
                            f"{stats['errors']} errors"
                        )
                    
                    if interval > 0:
                        time.sleep(interval)
                        
            except KeyboardInterrupt:
                click.echo("\nInterrupted by user")
            
            # Final flush
            producer.flush(timeout=10)
            stats = producer.stats
            
            click.echo()
            click.echo(f"Production complete!")
            click.echo(f"  Total sent: {produced}")
            click.echo(f"  Delivered: {stats['delivered']}")
            click.echo(f"  Errors: {stats['errors']}")
            
    except Exception as e:
        logger.error(f"Failed to produce events: {e}")
        sys.exit(1)


@cli.command("validate")
@click.option("--count", "-c", type=int, default=5, help="Number of events to validate")
@click.option("--seed", "-s", type=int, default=None, help="Random seed")
def validate_schema(count: int, seed: Optional[int]):
    """Validate schema by generating sample events."""
    config = load_config()
    
    generator = OrderGenerator(
        seed=seed,
        currencies=config["order"]["currencies"],
        min_amount=config["order"]["min_amount"],
        max_amount=config["order"]["max_amount"],
    )
    
    click.echo(f"Generating {count} sample events for validation...\n")
    
    for i, event in enumerate(generator.generate_stream(count)):
        click.echo(f"Event {i+1}:")
        click.echo(f"  event_id: {event.event_id}")
        click.echo(f"  event_time: {event.event_time.isoformat()}")
        click.echo(f"  order_id: {event.order_id}")
        click.echo(f"  user_id: {event.user_id}")
        click.echo(f"  order_amount: {event.order_amount} {event.currency}")
        click.echo(f"  order_status: {event.order_status}")
        click.echo()
    
    click.echo("All events validated successfully!")


if __name__ == "__main__":
    cli()
