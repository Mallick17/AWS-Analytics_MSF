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

# Create LocalStack init directory
mkdir -p "$INFRA_DIR/localstack-init"

# Create S3 bucket initialization script
cat > "$INFRA_DIR/localstack-init/init-s3.sh" << 'EOF'
#!/bin/bash
echo "Initializing LocalStack S3 buckets..."

# Create the analytics bucket
awslocal s3 mb s3://analytics-data

# Create directory structure
awslocal s3api put-object --bucket analytics-data --key warehouse/
awslocal s3api put-object --bucket analytics-data --key checkpoints/
awslocal s3api put-object --bucket analytics-data --key savepoints/

echo "S3 buckets initialized successfully!"
awslocal s3 ls
EOF

chmod +x "$INFRA_DIR/localstack-init/init-s3.sh"

# Pull Docker images
echo ""
echo "Pulling Docker images (this may take a while)..."
cd "$INFRA_DIR"
docker compose pull

echo ""
echo "=========================================="
echo "  Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Run './infra/scripts/start.sh' to start all services"
echo "  2. Run './infra/scripts/health-check.sh' to verify services"
echo "  3. Install producer dependencies: cd producer && pip install -r requirements.txt"
echo ""
