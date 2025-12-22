"""
PyFlink ETL using Native Flink Iceberg Connector
This version uses Flink's built-in Iceberg support for better performance
"""

import os
import json
import logging
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col, lit
from pyflink.table.types import DataTypes
from pyflink.table.table_descriptor import TableDescriptor, Schema as TableSchema
from pyflink.table.udf import udf

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FlinkIcebergETL:
    """ETL using Flink Table API with native Iceberg connector"""
    
    def __init__(self, config: dict):
        self.config = config
        self.env = None
        self.table_env = None
        
    def setup_environment(self):
        """Setup Flink environment with Iceberg support"""
        logger.info("Setting up Flink environment with Iceberg connector")
        
        # Create execution environment
        env_settings = EnvironmentSettings.in_streaming_mode()
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.table_env = StreamTableEnvironment.create(self.env, env_settings)
        
        # Set parallelism
        parallelism = int(self.config.get('parallelism', 1))
        self.env.set_parallelism(parallelism)
        
        # Enable checkpointing
        checkpoint_interval = int(self.config.get('checkpoint.interval', 60000))
        self.env.enable_checkpointing(checkpoint_interval)
        
        # Add Iceberg and required JARs
        self._add_iceberg_dependencies()
        
        logger.info(f"Environment configured with parallelism: {parallelism}")
    
    def _add_iceberg_dependencies(self):
        """Add required JAR dependencies for Iceberg"""
        # Note: These JARs need to be available in the Flink lib directory
        # For AWS Managed Flink, upload these to S3 and reference them
        jars = [
            # Flink Iceberg connector
            "flink-sql-connector-iceberg-1.18.jar",
            # AWS SDK dependencies
            "iceberg-aws-bundle.jar",
            "bundle-2.20.18.jar",
        ]
        
        for jar in jars:
            logger.info(f"Required JAR: {jar}")
    
    def create_kafka_source_table(self, topic: str, table_name: str):
        """Create Kafka source table using Table API"""
        logger.info(f"Creating Kafka source table for topic: {topic}")
        
        bootstrap_servers = self.config.get('kafka.bootstrap.servers')
        consumer_group = self.config.get('kafka.consumer.group', 'flink-python-consumer')
        start_offset = self.config.get('kafka.offset', 'earliest-offset')
        
        # Create Kafka table
        create_kafka_table = f"""
            CREATE TABLE {table_name}_source (
                event_id STRING,
                user_id STRING,
                event_type STRING,
                `timestamp` BIGINT,
                ride_id STRING,
                metadata ROW<
                    surge_multiplier DOUBLE,
                    estimated_wait_minutes INT,
                    fare_amount DOUBLE,
                    driver_rating DOUBLE
                >,
                proc_time AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic}',
                'properties.bootstrap.servers' = '{bootstrap_servers}',
                'properties.group.id' = '{consumer_group}',
                'scan.startup.mode' = '{start_offset}',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'AWS_MSK_IAM',
                'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
                'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
            )
        """
        
        self.table_env.execute_sql(create_kafka_table)
        logger.info(f"Kafka source table created: {table_name}_source")
    
    def create_iceberg_sink_table(self, namespace: str, table_name: str):
        """Create Iceberg sink table"""
        logger.info(f"Creating Iceberg sink table: {namespace}.{table_name}")
        
        warehouse = self.config.get('s3.warehouse')
        
        # Create Iceberg catalog
        create_catalog = f"""
            CREATE CATALOG iceberg_catalog WITH (
                'type' = 'iceberg',
                'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
                'warehouse' = '{warehouse}',
                'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
            )
        """
        
        try:
            self.table_env.execute_sql(create_catalog)
            logger.info("Iceberg catalog created")
        except Exception as e:
            logger.info(f"Catalog may already exist: {e}")
        
        # Create Iceberg table
        create_iceberg_table = f"""
            CREATE TABLE IF NOT EXISTS iceberg_catalog.{namespace}.{table_name} (
                event_id STRING,
                user_id STRING,
                event_type STRING,
                event_timestamp BIGINT,
                ride_id STRING,
                surge_multiplier DOUBLE,
                estimated_wait_minutes INT,
                fare_amount DOUBLE,
                driver_rating DOUBLE,
                event_hour STRING,
                PRIMARY KEY (event_id, event_hour) NOT ENFORCED
            ) PARTITIONED BY (event_hour)
            WITH (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy',
                'format-version' = '2',
                'write.upsert.enabled' = 'true'
            )
        """
        
        self.table_env.execute_sql(create_iceberg_table)
        logger.info(f"Iceberg sink table created: {namespace}.{table_name}")
    
    def process_topic(self, topic: str):
        """Process a single Kafka topic with transformation"""
        logger.info(f"Processing topic: {topic}")
        
        # Generate table names
        table_name = topic.replace('-', '_').replace('.', '_').lower()
        namespace = self.config.get('table.namespace', 'sink')
        
        # Create source and sink tables
        self.create_kafka_source_table(topic, table_name)
        self.create_iceberg_sink_table(namespace, table_name)
        
        # Transform and insert
        transform_insert = f"""
            INSERT INTO iceberg_catalog.{namespace}.{table_name}
            SELECT 
                event_id,
                user_id,
                event_type,
                `timestamp` as event_timestamp,
                ride_id,
                metadata.surge_multiplier,
                metadata.estimated_wait_minutes,
                metadata.fare_amount,
                metadata.driver_rating,
                DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000)), 'yyyy-MM-dd-HH') as event_hour
            FROM {table_name}_source
        """
        
        # Execute insert (this will run continuously)
        table_result = self.table_env.execute_sql(transform_insert)
        
        logger.info(f"Streaming insert started for topic: {topic}")
        
        # Note: In streaming mode, this will run until cancelled
        # For multiple topics, you'll need to handle this differently
        
    def run(self):
        """Main execution method"""
        logger.info("=== Starting PyFlink ETL with Native Iceberg ===")
        logger.info(f"Configuration: {json.dumps(self.config, indent=2)}")
        
        # Setup environment
        self.setup_environment()
        
        # Get topics
        topics_str = self.config.get('kafka.topics', 'user_events')
        topics = [t.strip() for t in topics_str.split(',')]
        
        logger.info(f"Processing {len(topics)} topics: {topics}")
        
        # Process first topic (streaming inserts block execution)
        # For multiple topics, consider creating separate pipelines
        if topics:
            self.process_topic(topics[0])
            
            # For additional topics, you'd need to create separate Flink jobs
            # or use a more complex pipeline structure
            if len(topics) > 1:
                logger.warning(f"Only processing first topic. {len(topics)-1} topics skipped.")
                logger.warning("For multiple topics, deploy separate Flink applications.")


def load_config() -> dict:
    """Load configuration from environment variables"""
    config = {
        'kafka.bootstrap.servers': os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS',
            'b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,'
            'b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,'
            'b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098'
        ),
        'kafka.topics': os.getenv('KAFKA_TOPICS', 'user_events'),
        'kafka.consumer.group': os.getenv('KAFKA_CONSUMER_GROUP', 'flink-python-consumer'),
        'kafka.offset': os.getenv('KAFKA_OFFSET', 'earliest-offset'),
        's3.warehouse': os.getenv(
            'S3_WAREHOUSE',
            'arn:aws:s3tables:ap-south-1:149815625933:bucket/testing-python-flink-table-bucket'
        ),
        'table.namespace': os.getenv('TABLE_NAMESPACE', 'sink'),
        'aws.region': os.getenv('AWS_REGION', 'ap-south-1'),
        'parallelism': os.getenv('PARALLELISM', '1'),
        'checkpoint.interval': os.getenv('CHECKPOINT_INTERVAL', '60000')
    }
    
    return config


def main():
    """Main entry point"""
    try:
        config = load_config()
        etl = FlinkIcebergETL(config)
        etl.run()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()