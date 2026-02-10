# Event Producer

A modular event producer for generating sample data to test the Flink pipeline.

## Structure

```
producer/
├── __init__.py           # Package initialization
├── requirements.txt      # Python dependencies
├── config.yaml          # Configuration settings
├── cli.py               # CLI entry point
├── generators/          # Event generators
│   ├── __init__.py
│   ├── bid_events.py    # Bid/ride events
│   └── user_events.py   # User activity events
└── kafka/               # Kafka utilities
    ├── __init__.py
    └── client.py        # Kafka client manager
```

## Installation

```bash
cd producer
pip install -r requirements.txt
```

## Usage

### Command Line

```bash
# Interactive mode
python -m producer.cli

# Direct usage
python -m producer.cli --local --topic both --count 100 --rate 2.0
python -m producer.cli --local --topic bid-events --count 50
python -m producer.cli --topic user-events  # Uses MSK
```

### As Module

```python
from producer.kafka.client import KafkaClientManager
from producer.generators.bid_events import BidEventsGenerator

# Create Kafka client
client = KafkaClientManager(is_local=True)

# Create generator
generator = BidEventsGenerator()

# Produce event
producer = client.get_producer()
event = generator.generate()
producer.send("bid-events", value=event)
```

## Configuration

Edit `config.yaml` to customize:
- Kafka bootstrap servers
- Topic settings
- Generator parameters
