#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INFRA_DIR="$PROJECT_ROOT/infra"

echo "=========================================="
echo "  Stopping Flink Analytics Platform"
echo "=========================================="

cd "$INFRA_DIR"

# Check if any services are running
if ! docker compose ps --status running 2>/dev/null | grep -q "kafka\|localstack\|jobmanager"; then
    echo "No services are running."
    exit 0
fi

# Parse arguments
REMOVE_VOLUMES=false
if [ "$1" == "--clean" ] || [ "$1" == "-c" ]; then
    REMOVE_VOLUMES=true
fi

if [ "$REMOVE_VOLUMES" = true ]; then
    echo "Stopping services and removing volumes..."
    docker compose down -v
    echo ""
    echo "All services stopped and volumes removed."
else
    echo "Stopping services (keeping volumes)..."
    docker compose down
    echo ""
    echo "All services stopped. Data volumes preserved."
    echo "Use '--clean' flag to also remove volumes."
fi

echo ""
echo "=========================================="
echo "  Services Stopped"
echo "=========================================="
