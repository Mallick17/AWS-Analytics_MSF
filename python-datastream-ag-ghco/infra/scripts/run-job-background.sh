#!/bin/bash
# ==================================================================================
# RUN JOB IN BACKGROUND SCRIPT - Execute PyFlink Streaming Job in Background
# ==================================================================================
# This script starts the PyFlink streaming job in the background and detaches.
# The job will continue running even after the script exits.
# ==================================================================================

set -e

# Get script directory and navigate to project root
SCRIPT_DIR=$(dirname "$0")
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

echo "=========================================="
echo "PyFlink Background Job Executor"
echo "=========================================="
echo "Project Root: $PROJECT_ROOT"
echo "Time: $(date)"
echo

# Check if Docker Compose is running
if ! docker-compose -f "$PROJECT_ROOT/infra/docker-compose.yml" ps | grep -q "Up"; then
    echo "❌ Error: Docker containers are not running!"
    echo "   Please run: ./infra/scripts/start.sh"
    exit 1
fi

echo "✓ Docker containers are running"

# Check if Flink JobManager is ready
echo "⏳ Waiting for Flink JobManager to be ready..."
for i in {1..30}; do
    if docker-compose -f "$PROJECT_ROOT/infra/docker-compose.yml" exec -T jobmanager curl -s http://localhost:8081/overview > /dev/null 2>&1; then
        echo "✓ Flink JobManager is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Timeout: Flink JobManager not ready after 30 attempts"
        exit 1
    fi
    sleep 2
done

# Start the job in background
echo "🚀 Starting PyFlink Streaming Job in background..."
echo "   Job will run continuously in the background"

# Create a detached screen session for the job
docker-compose -f "$PROJECT_ROOT/infra/docker-compose.yml" exec -d jobmanager \
    bash -c "cd /opt/flink/usrlib && python streaming_job.py"

echo "✓ Job started in background"

# Give it a moment to initialize
sleep 5

# Check if job is running
echo "🔍 Checking job status..."
if docker-compose -f "$PROJECT_ROOT/infra/docker-compose.yml" exec -T jobmanager \
    curl -s http://localhost:8081/jobs | grep -q "RUNNING"; then
    echo "✅ Job is running successfully"
else
    echo "⚠️ Job status unknown - check Flink dashboard"
fi

echo
echo "=========================================="
echo "Background Job Started"
echo "Time: $(date)"
echo "=========================================="
echo "📊 Monitor jobs: http://localhost:8081"
echo "📋 View logs: docker-compose -f infra/docker-compose.yml logs -f jobmanager"
echo "🛑 Stop pipeline: ./infra/scripts/stop.sh"
echo
echo "💡 The job will continue running in the background even if you close this terminal."
