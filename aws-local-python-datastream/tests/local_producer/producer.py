#!/usr/bin/env python3
"""
Local Event Producer
Generates synthetic data for enabled topics in topics.yaml and sends to local Kafka.
"""
import sys
import os
import time
import json
import random
import yaml
from datetime import datetime, timezone
import logging

try:
    from kafka import KafkaProducer
    from faker import Faker
except ImportError:
    print("Error: Missing dependencies.")
    print("Please run: pip install -r requirements.txt")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to path to import common config if needed, 
# but for simplicity we'll read YAML directly here to avoid dependency on Flink libs
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'config', 'topics.yaml')

fake = Faker()

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

def get_enabled_topics(config):
    topics = {} # topic_name -> schema
    for name, details in config.get('topics', {}).items():
        if details.get('enabled'):
            topics[name] = details.get('source_schema', [])
    return topics

def generate_value(field_type, field_name):
    """Generate fake value based on type and name context."""
    if field_type == 'STRING':
        if 'id' in field_name:
            if 'session' in field_name:
                return fake.uuid4()
            return str(fake.random_int(min=1000, max=9999))
        if 'url' in field_name:
            return fake.url()
        if 'device' in field_name:
            return random.choice(['mobile', 'desktop', 'tablet'])
        if 'platform' in field_name:
            return random.choice(['ios', 'android', 'web'])
        if 'event' in field_name:
            return random.choice(['click', 'view', 'purchase', 'login'])
        return fake.word()
    
    elif field_type in ['BIGINT', 'INT', 'LONG']:
        if 'timestamp' in field_name:
            return int(time.time() * 1000)
        if 'id' in field_name:
            return fake.random_int(min=1, max=100000)
        return fake.random_int(min=0, max=100)
    
    elif field_type == 'BOOLEAN':
        return fake.boolean()
        
    return None

def generate_event(schema):
    event = {}
    for field in schema:
        name = field['name']
        dtype = field['type']
        event[name] = generate_value(dtype, name)
    return event

def main():
    logger.info("Starting Local Event Producer...")
    
    # 1. Load Config
    try:
        config = load_config()
        enabled_topics = get_enabled_topics(config)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)
        
    if not enabled_topics:
        logger.warning("No enabled topics found in topics.yaml")
        sys.exit(0)
        
    logger.info(f"Target Topics: {list(enabled_topics.keys())}")
    
    # 2. Setup Kafka Producer
    # Assume local Kafka from docker-compose
    bootstrap_servers = 'localhost:9092' 
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"Connected to Kafka at {bootstrap_servers}")
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        logger.error("Ensure 'docker-compose up' is running.")
        sys.exit(1)
        
    # 3. Generate Loop
    try:
        while True:
            for topic, schema in enabled_topics.items():
                event = generate_event(schema)
                producer.send(topic, event)
                logger.info(f"Sent to {topic}: {event}")
            
            time.sleep(1) # 1 event per second per topic
            
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
        producer.close()

if __name__ == "__main__":
    main()
