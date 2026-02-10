#!/bin/bash
# ==================================================================================
# Stop Script for Local Kafka + Flink Pipeline
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

echo -e "${BLUE}"
echo "============================================================"
echo "  🛑 Stopping Local Flink + Kafka Pipeline"
echo "============================================================"
echo -e "${NC}"

# Parse command line arguments
CLEAN_FLAG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN_FLAG="true"
            shift
            ;;
        --help)
            echo ""
            echo "Usage: ./stop.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --clean    Remove volumes (deletes all data)"
            echo "  --help     Show this help message"
            echo ""
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Stop all profiles
echo -e "${YELLOW}Stopping containers...${NC}"
docker-compose -f infra/docker-compose.yml --env-file .env --profile debug --profile producer down

if [ -n "$CLEAN_FLAG" ]; then
    echo -e "${YELLOW}Removing volumes...${NC}"
    docker-compose -f infra/docker-compose.yml --env-file .env down -v
fi

echo ""
echo -e "${GREEN}✅ All containers stopped.${NC}"
echo ""
