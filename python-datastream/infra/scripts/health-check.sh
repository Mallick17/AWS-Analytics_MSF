#!/bin/bash
# Change to the project root directory
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR/../.."

# Check if containers are running
docker-compose -f infra/docker-compose.yml ps
