"""
PyFlink ETL from MSK Kafka to S3 Tables (Iceberg)
AWS Managed Flink Python Application

This application uses PyIceberg to write to S3 Tables (not standard S3 buckets).
S3 Tables is AWS's managed Iceberg service with built-in optimization.
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List
from collections import defaultdict
import time

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, DoubleType, IntegerType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class S3TablesWriter:
    """
    Batch writer for S3 Tables using PyIceberg
    S3 Tables provides managed Iceberg with automatic optimization
    """
    
    def __init__(self, catalog, namespace: str, table_name: str, batch_size: int = 100):
        self.catalog = catalog
        self.namespace = namespace
        self.table_name = table_name
        self.batch_size = batch_size
        self.buffer = []
        self.table = None
        self.last_flush = time.time()
        self.flush_interval = 10  # seconds
        
    def initialize_table(self):
        """Load or create the S3 Tables table"""
        if self.table is not None:
            return
            
        table_identifier = f"{self.namespace}.{self.table_name}"
        
        try:
            # Try to load existing table
            self.table = self.catalog.load_table(table_identifier)
            logger.info(f"Loaded existing S3 table: {table_identifier}")
            
        except Exception as e:
            logger.info(f"Table doesn't exist, will be created: {e}")
            # Table will be created on first write by create_iceberg_table_if_not_exists
    
    def add_record(self, record: Dict):
        """Add a record to the buffer"""
        self.buffer.append(record)
        
        # Flush if batch size reached or time threshold exceeded
        should_flush = (
            len(self.buffer) >= self.batch_size or
            (time.time() - self.last_flush) >= self.flush_interval
        )
        
        if should_flush:
            self.flush()
    
    def flush(self):
        """Flush buffered records to S3 Tables"""
        if not self.buffer:
            return
            
        try:
            if self.table is None:
                self.initialize_table()
            
            # Convert to PyArrow table
            pa_schema = self.table.schema().as_arrow()
            pa_table = pa.Table.from_pylist(self.buffer, schema=pa_schema)
            
            # Write to S3 Tables using PyIceberg
            # S3 Tables handles file optimization and compaction automatically
            self.table.append(pa_table)
            
            logger.info(f"✓ Flushed {len(self.buffer)} records to S3 table {self.namespace}.{self.table_name}")
            
            # Clear buffer
            self.buffer = []
            self.last_flush = time.time()
            
        except Exception as e:
            logger.error(f"✗ Failed to flush to S3 Tables: {e}", exc_info=True)
            # Clear buffer to prevent memory buildup
            self.buffer = []
    
    def close(self):
        """Flush remaining records and clean up"""
        self.flush()
        logger.info(f"Closed S3 Tables writer for {self.namespace}.{self.table_name}")


class KafkaToS3TablesETL:
    """
    Main ETL class for processing Kafka messages to S3 Tables
    S3 Tables is AWS's managed Apache Iceberg service
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.env = None
        self.catalog = None
        self.writers = {}  # Table writers per topic
        
    def setup_environment(self):
        """Setup PyFlink execution environment"""
        logger.info("=" * 60)
        logger.info("Setting up Flink execution environment")
        logger.info("=" * 60)
        
        self.env = StreamExecutionEnvironment.get_execution_environment()
        
        # Set parallelism
        parallelism = int(self.config.get('parallelism', 1))
        self.env.set_parallelism(parallelism)
        
        # Enable checkpointing for exactly-once semantics
        checkpoint_interval = int(self.config.get('checkpoint.interval', 60000))
        self.env.enable_checkpointing(checkpoint_interval)
        
        # Configure checkpoint settings
        checkpoint_config = self.env.get_checkpoint_config()
        checkpoint_config.set_min_pause_between_checkpoints(checkpoint_interval // 2)
        checkpoint_config.set_checkpoint_timeout(600000)
        checkpoint_config.set_max_concurrent_checkpoints(1)
        checkpoint_config.set_tolerable_checkpoint_failure_number(3)
        
        logger.info(f"✓ Parallelism: {parallelism}")
        logger.info(f"✓ Checkpoint interval: {checkpoint_interval}ms")
        
    def setup_s3_tables_catalog(self):
        """
        Setup PyIceberg catalog for S3 Tables
        
        S3 Tables is different from regular S3:
        - ARN format: arn:aws:s3tables:region:account:bucket/bucket-name
        - Managed Iceberg with automatic optimization
        - Built-in compaction and metadata management
        """
        logger.info("=" * 60)
        logger.info("Setting up S3 Tables catalog")
        logger.info("=" * 60)
        
        warehouse = self.config.get('s3.warehouse')
        region = self.config.get('aws.region', 'ap-south-1')
        
        logger.info(f"S3 Tables ARN: {warehouse}")
        logger.info(f"Region: {region}")
        
        # Configure catalog specifically for S3 Tables
        # Note: S3 Tables uses AWS Glue Data Catalog under the hood
        self.catalog = load_catalog(
            "s3_tables",
            **{
                "type": "glue",  # S3 Tables integrates with Glue
                "warehouse": warehouse,
                "client.region": region,
                "io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
            }
        )
        
        logger.info("✓ S3 Tables catalog configured successfully")
        
    def create_kafka_source(self, topic: str) -> KafkaSource:
        """Create Kafka source with MSK IAM authentication"""
        logger.info(f"Creating Kafka source for topic: {topic}")
        
        bootstrap_servers = self.config.get('kafka.bootstrap.servers')
        consumer_group = self.config.get('kafka.consumer.group', 'flink-python-consumer')
        start_offset = self.config.get('kafka.offset', 'earliest')
        
        # Determine offset initializer
        offset_init = (KafkaOffsetsInitializer.earliest() 
                      if start_offset == 'earliest' 
                      else KafkaOffsetsInitializer.latest())
        
        # MSK IAM authentication properties
        kafka_props = {
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'AWS_MSK_IAM',
            'sasl.jaas.config': 'software.amazon.msk.auth.iam.IAMLoginModule required;',
            'sasl.client.callback.handler.class': 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
        }
        
        source = (KafkaSource.builder()
                 .set_bootstrap_servers(bootstrap_servers)
                 .set_topics(topic)
                 .set_group_id(consumer_group)
                 .set_starting_offsets(offset_init)
                 .set_value_only_deserializer(SimpleStringSchema())
                 .set_properties(kafka_props)
                 .build())
        
        logger.info(f"✓ Kafka source created for topic: {topic}")
        return source
        
    def create_s3_table_if_not_exists(self, namespace: str, table_name: str):
        """
        Create S3 Tables table if it doesn't exist
        
        S3 Tables features:
        - Automatic file optimization
        - Managed compaction
        - Integrated with Glue Data Catalog
        """
        logger.info(f"Checking/creating S3 table: {namespace}.{table_name}")
        
        # Create namespace if needed
        try:
            self.catalog.create_namespace_if_not_exists(namespace)
            logger.info(f"✓ Namespace '{namespace}' ready")
        except Exception as e:
            logger.info(f"Namespace may exist: {e}")
        
        # Check if table exists
        table_identifier = f"{namespace}.{table_name}"
        
        if not self.catalog.table_exists(table_identifier):
            logger.info(f"Creating new S3 table: {table_identifier}")
            
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
            
            # Define partition spec - partition by event_hour for efficient queries
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=10,  # event_hour field
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="event_hour"
                )
            )
            
            # S3 Tables properties
            # Format version 2 is required for features like row-level deletes
            table_props = {
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "snappy",
                "format-version": "2",  # Required for S3 Tables features
                "write.upsert.enabled": "true",  # Enable upsert operations
            }
            
            # Create table in S3 Tables
            self.catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=table_props
            )
            
            logger.info(f"✓ S3 table created: {table_identifier}")
            logger.info(f"  - Partitioned by: event_hour")
            logger.info(f"  - Format: Parquet (Snappy)")
            logger.info(f"  - Upsert enabled: Yes")
        else:
            logger.info(f"✓ S3 table already exists: {table_identifier}")
    
    def parse_and_transform(self, json_str: str) -> Dict:
        """Parse JSON message and transform to target schema"""
        try:
            data = json.loads(json_str)
            
            # Extract metadata
            metadata = data.get('metadata', {})
            
            # Calculate event_hour for partitioning
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
            logger.error(f"Failed to parse message: {json_str[:100]}..., error: {e}")
            return None
    
    def process_topic(self, topic: str):
        """Process a single Kafka topic and write to S3 Tables"""
        logger.info("=" * 60)
        logger.info(f"Processing topic: {topic}")
        logger.info("=" * 60)
        
        # Generate table name from topic
        table_name = topic.replace('-', '_').replace('.', '_').lower()
        namespace = self.config.get('table.namespace', 'sink')
        
        logger.info(f"Topic: {topic} -> S3 Table: {namespace}.{table_name}")
        
        # Create S3 table if not exists
        self.create_s3_table_if_not_exists(namespace, table_name)
        
        # Create Kafka source
        kafka_source = self.create_kafka_source(topic)
        
        # Create data stream
        kafka_stream = self.env.from_source(
            kafka_source,
            WatermarkStrategy.no_watermarks(),
            f"Kafka-{topic}"
        )
        
        # Transform records
        transformed_stream = kafka_stream.map(
            lambda msg: self.parse_and_transform(msg),
            output_type=Types.PICKLED_BYTE_ARRAY()
        ).filter(lambda x: x is not None)
        
        # Create S3 Tables writer for this topic
        writer = S3TablesWriter(
            catalog=self.catalog,
            namespace=namespace,
            table_name=table_name,
            batch_size=100  # Batch size for efficient writes
        )
        self.writers[topic] = writer
        
        # Write to S3 Tables
        # Note: This is a simplified approach for demonstration
        # In production, use proper Flink sinks or process functions
        transformed_stream.map(
            lambda record: writer.add_record(record),
            output_type=Types.PICKLED_BYTE_ARRAY()
        )
        
        logger.info(f"✓ Stream pipeline configured: {topic} -> {namespace}.{table_name}")
    
    def run(self):
        """Main execution method"""
        logger.info("=" * 60)
        logger.info("Starting PyFlink to S3 Tables ETL")
        logger.info("=" * 60)
        
        # Log configuration
        logger.info("Configuration:")
        logger.info(f"  Kafka Brokers: {self.config.get('kafka.bootstrap.servers')}")
        logger.info(f"  Topics: {self.config.get('kafka.topics')}")
        logger.info(f"  S3 Tables: {self.config.get('s3.warehouse')}")
        logger.info(f"  Namespace: {self.config.get('table.namespace')}")
        logger.info(f"  Region: {self.config.get('aws.region')}")
        
        # Setup
        self.setup_environment()
        self.setup_s3_tables_catalog()
        
        # Get topics
        topics_str = self.config.get('kafka.topics', 'user_events')
        topics = [t.strip() for t in topics_str.split(',')]
        
        logger.info(f"Processing {len(topics)} topic(s): {topics}")
        
        # Process each topic
        for topic in topics:
            self.process_topic(topic)
        
        # Execute Flink job
        logger.info("=" * 60)
        logger.info("Starting Flink job execution...")
        logger.info("=" * 60)
        
        self.env.execute("PyFlink MSK to S3 Tables ETL")


def load_config() -> Dict:
    """Load configuration from environment variables"""
    config = {
        # Kafka configuration
        'kafka.bootstrap.servers': os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS',
            'b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,'
            'b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,'
            'b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098'
        ),
        'kafka.topics': os.getenv('KAFKA_TOPICS', 'user_events'),
        'kafka.consumer.group': os.getenv('KAFKA_CONSUMER_GROUP', 'flink-python-consumer'),
        'kafka.offset': os.getenv('KAFKA_OFFSET', 'earliest'),
        
        # S3 Tables configuration (NOT regular S3)
        's3.warehouse': os.getenv(
            'S3_WAREHOUSE',
            'arn:aws:s3tables:ap-south-1:149815625933:bucket/testing-python-flink-table-bucket'
        ),
        'table.namespace': os.getenv('TABLE_NAMESPACE', 'sink'),
        'aws.region': os.getenv('AWS_REGION', 'ap-south-1'),
        
        # Flink configuration
        'parallelism': os.getenv('PARALLELISM', '1'),
        'checkpoint.interval': os.getenv('CHECKPOINT_INTERVAL', '60000')
    }
    
    return config


def main():
    """Main entry point"""
    try:
        # Load configuration
        config = load_config()
        
        # Create and run ETL
        etl = KafkaToS3TablesETL(config)
        etl.run()
        
    except Exception as e:
        logger.error(f"Fatal error in ETL: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()