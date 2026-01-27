#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INFRA_DIR="$PROJECT_ROOT/infra"

echo "=========================================="
echo "  Starting Flink Analytics Platform"
echo "=========================================="

cd "$INFRA_DIR"

# Check if services are already running
if docker compose ps --status running 2>/dev/null | grep -q "kafka\|localstack\|jobmanager"; then
    echo "Some services are already running."
    echo "Use './stop.sh' first if you want to restart."
    echo ""
    docker compose ps
    exit 0
fi

# Start all services
echo "Starting services..."
docker compose up -d

echo ""
echo "Waiting for services to be healthy..."

# Wait for Kafka
echo -n "  Kafka: "
timeout=60
while [ $timeout -gt 0 ]; do
    if docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; then
        echo "✓ Ready"
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done
if [ $timeout -le 0 ]; then
    echo "✗ Timeout"
fi

# Wait for LocalStack
echo -n "  LocalStack: "
timeout=60
while [ $timeout -gt 0 ]; do
    if curl -s http://localhost:4566/_localstack/health | grep -q '"s3": "running"'; then
        echo "✓ Ready"
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done
if [ $timeout -le 0 ]; then
    echo "✗ Timeout"
fi

# Wait for Flink JobManager
echo -n "  Flink JobManager: "
timeout=90
while [ $timeout -gt 0 ]; do
    if curl -sf http://localhost:8081/overview &>/dev/null; then
        echo "✓ Ready"
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done
if [ $timeout -le 0 ]; then
    echo "✗ Timeout"
fi

echo ""
echo "=========================================="
echo "  Services Started!"
echo "=========================================="
echo ""
echo "Access Points:"
echo "  - Kafka: localhost:9092"
echo "  - LocalStack S3: localhost:4566"
echo "  - Flink UI: http://localhost:8081"
echo ""
echo "Run './health-check.sh' to verify all services."
echo ""
