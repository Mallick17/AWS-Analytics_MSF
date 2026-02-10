#!/bin/bash
# Change to the project root directory
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR/../.."

docker-compose -f infra/docker-compose.yml down
