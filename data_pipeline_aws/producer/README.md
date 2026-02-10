# Analytics Producer

Python CLI application for generating and publishing events to Kafka.

## Installation

```bash
cd producer
pip install -r requirements.txt
```

## Usage

### Produce Order Events

```bash
# Produce 100 events at 10 events/second
python cli.py produce orders --rate 10 --count 100

# Produce with deterministic seed (reproducible)
python cli.py produce orders --rate 5 --seed 42

# Infinite stream at 1 event/second
python cli.py produce orders --rate 1 --count 0

# Dry run (print events without sending to Kafka)
python cli.py produce orders --dry-run --count 5
```

### Validate Schema

```bash
python cli.py validate --count 5
```

## Configuration

Configuration is loaded from `config.yaml` with environment variable support.

Key settings:
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses (default: `localhost:9092`)
- `KAFKA_TOPIC_ORDERS`: Topic for order events (default: `orders.created.v1`)

## Event Schema

### Order Created Event (v1)

```json
{
  "event_id": "uuid",
  "event_time": "ISO-8601 timestamp",
  "order_id": "string",
  "user_id": "string",
  "order_amount": "decimal",
  "currency": "string",
  "order_status": "CREATED"
}
```

## Project Structure

```
producer/
├── cli.py              # CLI entry point
├── config.yaml         # Configuration
├── requirements.txt    # Dependencies
├── schemas/
│   └── order_created_v1.py  # Event schema
├── generators/
│   └── order_generator.py   # Faker-based generator
└── kafka/
    └── producer.py     # Kafka client
```
