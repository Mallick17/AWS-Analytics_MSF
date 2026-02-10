#!/bin/bash
# ==================================================================================
# Health Check Script for Local Kafka + Flink Pipeline
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
echo "  🔍 Health Check: Local Flink + Kafka Pipeline"
echo "============================================================"
echo -e "${NC}"

# Check Docker containers status
echo -e "${YELLOW}Container Status:${NC}"
docker-compose -f infra/docker-compose.yml --env-file .env ps

echo ""
echo -e "${YELLOW}Service Health:${NC}"

# Check Kafka
echo -n "  Kafka:           "
if docker-compose -f infra/docker-compose.yml --env-file .env exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Healthy${NC}"
    TOPICS=$(docker-compose -f infra/docker-compose.yml --env-file .env exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | tr '\n' ', ')
    echo "                   Topics: ${TOPICS%,}"
else
    echo -e "${RED}✗ Unhealthy${NC}"
fi

# Check Flink JobManager
echo -n "  Flink JobManager: "
if curl -s http://localhost:8081/overview > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Healthy${NC}"
    JOBS=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | grep -o '"state":"[^"]*"' | head -3 || echo "No jobs")
    echo "                   Jobs: ${JOBS:-No running jobs}"
else
    echo -e "${RED}✗ Unhealthy${NC}"
fi

# Check Flink TaskManager
echo -n "  Flink TaskManager: "
TASKMANAGERS=$(curl -s http://localhost:8081/taskmanagers 2>/dev/null | grep -o '"taskmanagers":\[[^]]*\]' | grep -o '"id"' | wc -l || echo 0)
if [ "$TASKMANAGERS" -gt 0 ]; then
    echo -e "${GREEN}✓ Healthy (${TASKMANAGERS} registered)${NC}"
else
    echo -e "${RED}✗ No TaskManagers registered${NC}"
fi

# Check Kafka UI (if running)
echo -n "  Kafka UI:        "
if curl -s http://localhost:8080 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Running (http://localhost:8080)${NC}"
else
    echo -e "${YELLOW}○ Not running (start with --debug flag)${NC}"
fi

echo ""
echo -e "${BLUE}============================================================${NC}"
echo ""
