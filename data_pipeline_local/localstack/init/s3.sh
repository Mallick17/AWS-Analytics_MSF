#!/bin/bash
# LocalStack S3 Initialization Script
# Creates required buckets and directory structure for Iceberg

set -e

echo "=========================================="
echo "  Initializing LocalStack S3"
echo "=========================================="

# Wait for LocalStack to be ready
echo "Waiting for LocalStack..."
until awslocal s3 ls 2>/dev/null; do
    sleep 1
done

# Create analytics data bucket
echo "Creating S3 bucket: analytics-data"
awslocal s3 mb s3://analytics-data 2>/dev/null || true

# Create directory structure
echo "Creating directory structure..."
awslocal s3api put-object --bucket analytics-data --key warehouse/ 2>/dev/null || true
awslocal s3api put-object --bucket analytics-data --key checkpoints/ 2>/dev/null || true
awslocal s3api put-object --bucket analytics-data --key savepoints/ 2>/dev/null || true

# Verify
echo ""
echo "Bucket contents:"
awslocal s3 ls s3://analytics-data/

echo ""
echo "=========================================="
echo "  LocalStack S3 initialized successfully"
echo "=========================================="
