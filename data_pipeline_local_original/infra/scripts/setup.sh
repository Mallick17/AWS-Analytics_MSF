#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INFRA_DIR="$PROJECT_ROOT/infra"

echo "=========================================="
echo "  Flink Analytics Platform - Setup"
echo "=========================================="

# Check for Docker
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed. Please install Docker first."
    exit 1
fi

# Check for Docker Compose
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "ERROR: Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create .env file if not exists
if [ ! -f "$PROJECT_ROOT/.env" ]; then
    echo "Creating .env file from .env.example..."
    cp "$PROJECT_ROOT/.env.example" "$PROJECT_ROOT/.env"
fi

# Pull Docker images
echo ""
echo "Pulling Docker images (this may take a while)..."
cd "$INFRA_DIR"

# Try to pull with --ignore-buildable flag if supported
if docker compose pull --help 2>&1 | grep -q -e "--ignore-buildable" -e "ignore-buildable"; then
    docker compose pull --ignore-buildable 2>/dev/null || true
else
    # Pull only non-buildable images
    docker compose pull zookeeper kafka 2>/dev/null || true
fi

# Build custom Flink image
echo ""
echo "Building custom Flink image..."
docker compose build jobmanager taskmanager

echo ""
echo "=========================================="
echo "  Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Run './infra/scripts/start.sh' to start all services"
echo "  2. Run './infra/scripts/health-check.sh' to verify services"
echo "  3. Install Python dependencies (uv): uv sync --group producer --group test"
echo ""
