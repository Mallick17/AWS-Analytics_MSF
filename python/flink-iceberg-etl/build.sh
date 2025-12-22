#!/bin/bash

# Build script for PyFlink Iceberg ETL Application
# This script downloads dependencies and creates deployment package

set -e

echo "=================================="
echo "PyFlink Iceberg ETL Build Script"
echo "=================================="
echo ""
echo "Target S3 Bucket: testing-python-flink-connector"
echo "S3 Tables Bucket: testing-python-flink-table-bucket"
echo "Namespace: sink"
echo "Region: ap-south-1"
echo ""

# Configuration
APP_NAME="flink-iceberg-etl"
BUILD_DIR="build"
LIB_DIR="lib"
FLINK_VERSION="1.18.0"
ICEBERG_VERSION="1.4.3"
AWS_SDK_VERSION="2.20.18"

# Create directories
echo "Creating build directories..."
rm -rf $BUILD_DIR
mkdir -p $BUILD_DIR/$LIB_DIR

# Copy Python application files
echo "Copying application files..."
cp main.py $BUILD_DIR/
cp requirements.txt $BUILD_DIR/
cp .pyiceberg.yaml $BUILD_DIR/ 2>/dev/null || echo "No .pyiceberg.yaml found, skipping..."

# Download JAR dependencies
echo ""
echo "Downloading JAR dependencies..."
cd $BUILD_DIR/$LIB_DIR

# Function to download with retry
download_jar() {
    local url=$1
    local filename=$(basename $url)
    
    echo "Downloading $filename..."
    
    if [ -f "$filename" ]; then
        echo "  $filename already exists, skipping..."
        return 0
    fi
    
    for i in {1..3}; do
        if wget -q --show-progress "$url"; then
            echo "  ✓ Downloaded $filename"
            return 0
        else
            echo "  ✗ Attempt $i failed"
            sleep 2
        fi
    done
    
    echo "  ✗ Failed to download $filename after 3 attempts"
    return 1
}

# Flink Kafka Connector
download_jar "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_VERSION}/flink-sql-connector-kafka-${FLINK_VERSION}.jar"

# AWS MSK IAM Auth
download_jar "https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.6/aws-msk-iam-auth-1.1.6-all.jar"

# Iceberg Flink Runtime
download_jar "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/${ICEBERG_VERSION}/iceberg-flink-runtime-1.18-${ICEBERG_VERSION}.jar"

# Iceberg AWS Bundle
download_jar "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# AWS SDK Bundle (for S3 Tables support)
download_jar "https://repo.maven.apache.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar"

cd ../..

# Create deployment package
echo ""
echo "Creating deployment package..."
cd $BUILD_DIR
zip -r ../${APP_NAME}.zip * -q

cd ..

# Display results
echo ""
echo "=================================="
echo "Build Complete!"
echo "=================================="
echo "Package: ${APP_NAME}.zip"
echo "Size: $(du -h ${APP_NAME}.zip | cut -f1)"
echo ""
echo "Contents:"
unzip -l ${APP_NAME}.zip | head -n 25
echo ""
echo "To deploy to S3:"
echo "  aws s3 cp ${APP_NAME}.zip s3://testing-python-flink-connector/applications/ --region ap-south-1"
echo ""
echo "To verify upload:"
echo "  aws s3 ls s3://testing-python-flink-connector/applications/ --region ap-south-1"
echo ""
echo "JAR files included:"
ls -lh $BUILD_DIR/$LIB_DIR/
echo ""

# Cleanup option
read -p "Clean up build directory? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -rf $BUILD_DIR
    echo "Build directory cleaned"
fi

echo ""
echo "✓ Done! Package ready for deployment to AWS Managed Flink"
echo ""
echo "Next steps:"
echo "1. Upload: aws s3 cp ${APP_NAME}.zip s3://testing-python-flink-connector/applications/ --region ap-south-1"
echo "2. Create Flink application in AWS Console or using CLI"
echo "3. Configure environment variables (see DEPLOYMENT.md)"
echo "4. Start the application"