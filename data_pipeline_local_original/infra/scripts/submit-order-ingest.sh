#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if ! docker ps --format '{{.Names}}' | grep -q '^flink-jobmanager$'; then
  echo "ERROR: flink-jobmanager container is not running. Start services first: ./infra/scripts/start.sh"
  exit 1
fi

echo "Submitting OrderIngestJob to Flink (inside flink-jobmanager)..."

docker exec -it flink-jobmanager bash -c "
cd /opt/flink/usrlib && 
zip -rq /tmp/flink-deps.zip common __init__.py && 
flink run -d --pyFiles /tmp/flink-deps.zip --python /opt/flink/usrlib/jobs/order_ingest/job.py
"

echo ""
echo "Submitted. Check Flink UI: http://localhost:8081"
echo "To view output, tail logs: docker logs -f flink-taskmanager"
