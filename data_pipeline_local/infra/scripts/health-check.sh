#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INFRA_DIR="$PROJECT_ROOT/infra"

echo "=========================================="
echo "  Health Check - Flink Analytics Platform"
echo "=========================================="
echo ""

cd "$INFRA_DIR"

ALL_HEALTHY=true

# Check Kafka
echo -n "Kafka:          "
if docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; then
    TOPICS=$(docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | tr '\n' ', ' | sed 's/,$//')
    echo "✓ Healthy (Topics: $TOPICS)"
else
    echo "✗ Unhealthy"
    ALL_HEALTHY=false
fi

# Check LocalStack S3
echo -n "LocalStack S3:  "
if curl -s http://localhost:4566/_localstack/health | grep -q '"s3": "running"'; then
    BUCKETS=$(aws --endpoint-url=http://localhost:4566 s3 ls 2>/dev/null | awk '{print $3}' | tr '\n' ', ' | sed 's/,$//')
    if [ -n "$BUCKETS" ]; then
        echo "✓ Healthy (Buckets: $BUCKETS)"
    else
        echo "✓ Healthy (No buckets)"
    fi
else
    echo "✗ Unhealthy"
    ALL_HEALTHY=false
fi

# Check Flink JobManager
echo -n "Flink JobManager: "
if curl -sf http://localhost:8081/overview &>/dev/null; then
    JOBS=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | grep -o '"running":[0-9]*' | head -1 | cut -d: -f2)
    echo "✓ Healthy (Running jobs: ${JOBS:-0})"
else
    echo "✗ Unhealthy"
    ALL_HEALTHY=false
fi

# Check Flink TaskManager
echo -n "Flink TaskManager: "
TM_COUNT=$(curl -s http://localhost:8081/taskmanagers 2>/dev/null | grep -o '"id"' | wc -l | tr -d ' ')
if [ "$TM_COUNT" -gt 0 ]; then
    echo "✓ Healthy (TaskManagers: $TM_COUNT)"
else
    echo "✗ Unhealthy (No TaskManagers)"
    ALL_HEALTHY=false
fi

echo ""
echo "=========================================="

if [ "$ALL_HEALTHY" = true ]; then
    echo "  All services are healthy!"
    echo "=========================================="
    exit 0
else
    echo "  Some services are unhealthy!"
    echo "=========================================="
    echo ""
    echo "Troubleshooting:"
    echo "  - Check logs: docker compose -f $INFRA_DIR/docker-compose.yml logs <service>"
    echo "  - Restart: ./stop.sh && ./start.sh"
    exit 1
fi
