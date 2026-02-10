#!/bin/bash
# ==================================================================================
# Start Script for Local Kafka + Flink → AWS S3 Tables Pipeline
# ==================================================================================
# This script builds and starts the local development environment:
# - Local Kafka (KRaft mode)
# - Flink JobManager & TaskManager with PyFlink
# - Writes to AWS S3 Tables (Iceberg)
# ==================================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Change to the project root directory
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR/../.."
PROJECT_ROOT=$(pwd)

echo -e "${BLUE}"
echo "============================================================"
echo "  🚀 Starting Local Flink + Kafka Pipeline"
echo "============================================================"
echo -e "${NC}"

# Check if .env file exists
if [ ! -f .env ]; then
    echo -e "${RED}❌ Error: .env file not found!${NC}"
    echo "Please create a .env file from .env.example:"
    echo "  cp .env.example .env"
    echo "Then fill in your AWS credentials and S3 Tables ARN."
    exit 1
fi

# Load and validate environment variables
source .env
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ] || [ -z "$S3_WAREHOUSE" ]; then
    echo -e "${RED}❌ Error: Missing required environment variables in .env${NC}"
    echo "Required: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_WAREHOUSE"
    exit 1
fi

echo -e "${GREEN}✓ Environment variables loaded${NC}"
echo "  AWS_REGION: ${AWS_REGION:-ap-south-1}"
echo "  S3_WAREHOUSE: ${S3_WAREHOUSE:0:50}..."

# Parse command line arguments
DO_BUILD=""
NO_CACHE_FLAG=""
DEBUG_FLAG=""
PRODUCER_FLAG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --build)
            DO_BUILD="yes"
            shift
            ;;
        --no-cache)
            DO_BUILD="yes"
            NO_CACHE_FLAG="--no-cache"
            shift
            ;;
        --debug)
            DEBUG_FLAG="--profile debug"
            shift
            ;;
        --producer)
            PRODUCER_FLAG="--profile producer"
            shift
            ;;
        --help)
            echo ""
            echo "Usage: ./start.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --build      Rebuild Docker images before starting"
            echo "  --no-cache   Rebuild Docker images without cache"
            echo "  --debug      Start Kafka UI for debugging (port 8080)"
            echo "  --producer   Start sample event producer"
            echo "  --help       Show this help message"
            echo ""
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Build if requested
if [ -n "$DO_BUILD" ]; then
    echo ""
    echo -e "${YELLOW}📦 Building Docker images...${NC}"
    docker-compose -f infra/docker-compose.yml --env-file .env build $NO_CACHE_FLAG
fi

# Start services
echo ""
echo -e "${YELLOW}🐳 Starting Docker containers...${NC}"
docker-compose -f infra/docker-compose.yml --env-file .env $DEBUG_FLAG $PRODUCER_FLAG up -d

# Wait for services to be healthy
echo ""
echo -e "${YELLOW}⏳ Waiting for services to be healthy...${NC}"
sleep 5

# Check Kafka health
echo -n "  Kafka: "
for i in {1..30}; do
    if docker-compose -f infra/docker-compose.yml --env-file .env exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Ready${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}✗ Not responding${NC}"
    fi
    sleep 2
done

# Check Flink JobManager health
echo -n "  Flink JobManager: "
for i in {1..30}; do
    if curl -s http://localhost:8081/overview > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Ready${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}✗ Not responding${NC}"
    fi
    sleep 2
done

# Show status
echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  ✅ Local environment is ready!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo "  📊 Access Points:"
echo "     • Flink UI:     http://localhost:8081"
if [ -n "$DEBUG_FLAG" ]; then
echo "     • Kafka UI:     http://localhost:8080"
fi
echo "     • Kafka:        localhost:9092"
echo ""
echo "  🔧 Useful Commands:"
echo "     • View logs:    docker-compose -f infra/docker-compose.yml --env-file .env logs -f"
echo "     • Stop:         ./infra/scripts/stop.sh"
echo "     • Health check: ./infra/scripts/health-check.sh"
echo ""
echo "  ▶️  Run Flink Job:"
echo "     docker-compose -f infra/docker-compose.yml --env-file .env exec jobmanager \\"
echo "       python /opt/flink/usrlib/streaming_job.py"
echo ""
echo "  📤 Start Event Producer (Containerized):"
echo "     docker-compose -f infra/docker-compose.yml --env-file .env --profile producer up -d"
echo "     docker logs kafka-producer -f"
echo ""
echo "  📤 Custom Producer Commands:"
echo "     docker-compose -f infra/docker-compose.yml --env-file .env exec kafka-producer \\"
echo "       python -m producer.cli --topic both --count 100 --rate 2"
echo ""
