#!/bin/bash
set -e

# Change to the project root directory
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR/../.."

# Load environment variables if .env exists
if [ -f .env ]; then
  echo "Loading environment variables from .env"
  # Export variables from .env (ignoring comments)
  export $(grep -v '^#' .env | xargs)
fi

echo "Waiting for Kafka to be ready..."
# Simple sleep loop to wait for Kafka
sleep 10

echo "Creating 'bid-events' topic..."
docker-compose -f infra/docker-compose.yml exec -T kafka kafka-topics --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic bid-events

echo "Creating 'user-events' topic..."
docker-compose -f infra/docker-compose.yml exec -T kafka kafka-topics --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic user-events

echo "Listing topics:"
docker-compose -f infra/docker-compose.yml exec -T kafka kafka-topics --list --bootstrap-server localhost:9092
