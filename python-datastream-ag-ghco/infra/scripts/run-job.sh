#!/bin/bash
# ==================================================================================
# RUN JOB SCRIPT - Execute PyFlink Streaming Job
# ==================================================================================
# This script executes the PyFlink streaming job inside the Flink container.
# The job will run in the background and process data continuously.
# ==================================================================================

set -e

# Get script directory and navigate to project root
SCRIPT_DIR=$(dirname "$0")
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

echo "=========================================="
echo "PyFlink Streaming Job Executor"
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

# Execute the PyFlink job
echo "🚀 Starting PyFlink Streaming Job..."
echo "   Job will run in the background continuously"
echo "   Press Ctrl+C to stop monitoring (job will continue running)"
echo

# Run the job with proper error handling
if docker-compose -f "$PROJECT_ROOT/infra/docker-compose.yml" exec -T jobmanager \
    bash -c "cd /opt/flink/usrlib && python streaming_job.py"; then
    echo "✓ Job executed successfully"
else
    echo "❌ Job execution failed"
    echo "📋 Check logs with: docker-compose -f infra/docker-compose.yml logs jobmanager"
    exit 1
fi

echo
echo "=========================================="
echo "Job Execution Complete"
echo "Time: $(date)"
echo "=========================================="
echo "📊 Monitor jobs: http://localhost:8081"
echo "📋 View logs: docker-compose -f infra/docker-compose.yml logs -f jobmanager"
echo "🛑 Stop pipeline: ./infra/scripts/stop.sh"
