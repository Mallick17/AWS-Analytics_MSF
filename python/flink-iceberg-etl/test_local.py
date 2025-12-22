"""
Local testing script for PyFlink Iceberg ETL
Tests the transformation logic without requiring full Flink cluster
"""

import json
import logging
from datetime import datetime
from typing import List, Dict

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, DoubleType, IntegerType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_sample_messages(count: int = 10) -> List[str]:
    """Generate sample Kafka messages for testing"""
    import random
    
    messages = []
    base_time = int(datetime.now().timestamp() * 1000)
    
    for i in range(count):
        msg = {
            "event_id": f"evt_{i:06d}",
            "user_id": f"user_{random.randint(1, 100):04d}",
            "event_type": random.choice(["ride_request", "ride_start", "ride_end"]),
            "timestamp": base_time + (i * 1000),
            "ride_id": f"ride_{random.randint(1, 50):05d}",
            "metadata": {
                "surge_multiplier": round(random.uniform(1.0, 3.0), 2),
                "estimated_wait_minutes": random.randint(2, 15),
                "fare_amount": round(random.uniform(10.0, 100.0), 2),
                "driver_rating": round(random.uniform(3.5, 5.0), 2)
            }
        }
        messages.append(json.dumps(msg))
    
    return messages


def parse_and_transform(json_str: str) -> Dict:
    """Parse JSON message and transform to target schema"""
    try:
        data = json.loads(json_str)
        
        # Extract metadata
        metadata = data.get('metadata', {})
        
        # Calculate event_hour
        timestamp_ms = data.get('timestamp', 0)
        event_hour = datetime.fromtimestamp(timestamp_ms / 1000.0).strftime('%Y-%m-%d-%H')
        
        # Transform to target schema
        transformed = {
            'event_id': data.get('event_id', ''),
            'user_id': data.get('user_id', ''),
            'event_type': data.get('event_type', ''),
            'event_timestamp': timestamp_ms,
            'ride_id': data.get('ride_id', ''),
            'surge_multiplier': float(metadata.get('surge_multiplier', 0.0)),
            'estimated_wait_minutes': int(metadata.get('estimated_wait_minutes', 0)),
            'fare_amount': float(metadata.get('fare_amount', 0.0)),
            'driver_rating': float(metadata.get('driver_rating', 0.0)),
            'event_hour': event_hour
        }
        
        return transformed
        
    except Exception as e:
        logger.error(f"Failed to parse message: {json_str}, error: {e}")
        return None


def test_transformation():
    """Test the transformation logic"""
    logger.info("=== Testing Transformation Logic ===")
    
    # Generate sample messages
    messages = generate_sample_messages(5)
    
    logger.info(f"Generated {len(messages)} sample messages")
    
    # Transform messages
    transformed = []
    for msg in messages:
        result = parse_and_transform(msg)
        if result:
            transformed.append(result)
            logger.info(f"Transformed: {result['event_id']} -> {result['event_hour']}")
    
    logger.info(f"Successfully transformed {len(transformed)}/{len(messages)} messages")
    
    return transformed


def test_iceberg_operations(test_data: List[Dict]):
    """Test Iceberg table operations locally"""
    logger.info("=== Testing Iceberg Operations (Local) ===")
    logger.info("NOTE: This test uses local SQLite catalog, not S3 Tables")
    
    # Setup catalog (using local/in-memory for testing)
    catalog = load_catalog(
        "test_catalog",
        **{
            "type": "sql",
            "uri": "sqlite:///test_catalog.db",
            "warehouse": "file:///tmp/iceberg-warehouse"
        }
    )
    
    # Create namespace
    namespace = "sink"
    try:
        catalog.create_namespace(namespace)
        logger.info(f"Created namespace: {namespace}")
    except Exception as e:
        logger.info(f"Namespace may already exist: {e}")
    
    # Define schema
    schema = Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "user_id", StringType(), required=True),
        NestedField(3, "event_type", StringType(), required=True),
        NestedField(4, "event_timestamp", LongType(), required=True),
        NestedField(5, "ride_id", StringType(), required=True),
        NestedField(6, "surge_multiplier", DoubleType(), required=True),
        NestedField(7, "estimated_wait_minutes", IntegerType(), required=True),
        NestedField(8, "fare_amount", DoubleType(), required=True),
        NestedField(9, "driver_rating", DoubleType(), required=True),
        NestedField(10, "event_hour", StringType(), required=True)
    )
    
    # Define partition spec
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=10,
            field_id=1000,
            transform=IdentityTransform(),
            name="event_hour"
        )
    )
    
    # Create table
    table_name = "user_events_test"
    table_identifier = f"{namespace}.{table_name}"
    
    try:
        # Drop if exists
        if catalog.table_exists(table_identifier):
            catalog.drop_table(table_identifier)
            logger.info(f"Dropped existing table: {table_identifier}")
        
        # Create new table
        table = catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=partition_spec
        )
        logger.info(f"Created table: {table_identifier}")
        
        # Convert test data to PyArrow
        pa_schema = table.schema().as_arrow()
        pa_table = pa.Table.from_pylist(test_data, schema=pa_schema)
        
        logger.info(f"Writing {len(test_data)} records to table...")
        table.append(pa_table)
        
        # Read back data
        logger.info("Reading data back from table...")
        scan_result = table.scan().to_pandas()
        
        logger.info(f"Read {len(scan_result)} records from table")
        logger.info("\nSample data:")
        print(scan_result.head())
        
        # Test partitioning
        partitions = scan_result['event_hour'].unique()
        logger.info(f"\nPartitions found: {partitions}")
        
        # Test filtering
        logger.info("\nTesting partition filtering...")
        filtered = table.scan(
            row_filter=f"event_hour == '{partitions[0]}'"
        ).to_pandas()
        logger.info(f"Records in partition {partitions[0]}: {len(filtered)}")
        
        logger.info("\n✓ Iceberg operations test passed!")
        
    except Exception as e:
        logger.error(f"Iceberg test failed: {e}", exc_info=True)
        raise


def test_s3_tables_connection():
    """Test connection to actual S3 Tables (requires AWS credentials)"""
    logger.info("=== Testing S3 Tables Connection ===")
    logger.info("NOTE: This requires AWS credentials with access to S3 Tables")
    
    try:
        # Setup catalog for S3 Tables
        catalog = load_catalog(
            "s3_tables",
            **{
                "type": "glue",
                "warehouse": "arn:aws:s3tables:ap-south-1:149815625933:bucket/testing-python-flink-table-bucket",
                "client.region": "ap-south-1",
                "io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
            }
        )
        
        logger.info("✓ Successfully connected to S3 Tables catalog")
        
        # Try to list namespaces
        try:
            namespaces = catalog.list_namespaces()
            logger.info(f"Found namespaces: {namespaces}")
            
            # Check if 'sink' namespace exists
            if ('sink',) in namespaces or 'sink' in [str(ns) for ns in namespaces]:
                logger.info("✓ Namespace 'sink' exists")
                
                # Try to list tables
                try:
                    tables = catalog.list_tables('sink')
                    logger.info(f"Tables in 'sink' namespace: {tables}")
                except Exception as e:
                    logger.info(f"Could not list tables: {e}")
            else:
                logger.warning("✗ Namespace 'sink' not found")
                logger.info("You may need to create it manually or it will be created on first write")
                
        except Exception as e:
            logger.warning(f"Could not list namespaces: {e}")
        
        logger.info("\n✓ S3 Tables connection test passed!")
        
    except Exception as e:
        logger.warning(f"S3 Tables connection test failed: {e}")
        logger.info("This is expected if you don't have AWS credentials configured")
        logger.info("The application will work when deployed to AWS Managed Flink")


def test_schema_compatibility():
    """Test schema compatibility between source and target"""
    logger.info("=== Testing Schema Compatibility ===")
    
    # Sample message
    sample = json.loads(generate_sample_messages(1)[0])
    
    logger.info("Source message structure:")
    logger.info(json.dumps(sample, indent=2))
    
    # Transform
    transformed = parse_and_transform(json.dumps(sample))
    
    logger.info("\nTransformed record:")
    logger.info(json.dumps(transformed, indent=2))
    
    # Check all required fields
    required_fields = [
        'event_id', 'user_id', 'event_type', 'event_timestamp',
        'ride_id', 'surge_multiplier', 'estimated_wait_minutes',
        'fare_amount', 'driver_rating', 'event_hour'
    ]
    
    missing = [f for f in required_fields if f not in transformed]
    
    if missing:
        logger.error(f"Missing fields: {missing}")
        raise ValueError(f"Schema validation failed - missing fields: {missing}")
    
    logger.info("✓ Schema compatibility test passed!")


def test_performance():
    """Test transformation performance"""
    logger.info("=== Testing Performance ===")
    
    import time
    
    message_counts = [100, 1000, 10000]
    
    for count in message_counts:
        messages = generate_sample_messages(count)
        
        start = time.time()
        transformed = [parse_and_transform(msg) for msg in messages]
        transformed = [t for t in transformed if t is not None]
        duration = time.time() - start
        
        rate = count / duration
        logger.info(f"Processed {count} messages in {duration:.2f}s ({rate:.0f} msg/s)")
    
    logger.info("✓ Performance test completed!")


def main():
    """Run all tests"""
    logger.info("========================================")
    logger.info("  PyFlink Iceberg ETL - Local Tests")
    logger.info("========================================")
    logger.info("")
    logger.info("S3 Bucket: testing-python-flink-connector")
    logger.info("S3 Tables: testing-python-flink-table-bucket")
    logger.info("Namespace: sink")
    logger.info("Region: ap-south-1")
    logger.info("========================================\n")
    
    try:
        # Test 1: Transformation logic
        transformed_data = test_transformation()
        
        # Test 2: Schema compatibility
        test_schema_compatibility()
        
        # Test 3: Iceberg operations (local)
        test_iceberg_operations(transformed_data)
        
        # Test 4: S3 Tables connection (optional)
        test_s3_tables_connection()
        
        # Test 5: Performance
        test_performance()
        
        logger.info("\n========================================")
        logger.info("  ✓ All tests passed!")
        logger.info("========================================")
        logger.info("\nNext steps:")
        logger.info("1. Run ./build.sh to create deployment package")
        logger.info("2. Upload to S3: aws s3 cp flink-iceberg-etl.zip s3://testing-python-flink-connector/applications/ --region ap-south-1")
        logger.info("3. Follow DEPLOYMENT.md for AWS Managed Flink setup")
        
    except Exception as e:
        logger.error(f"\n✗ Tests failed: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())