"""
Kafka Source Factory

Creates Flink Kafka sources with proper configuration.
Cloud-compatible implementation using Table API.
"""

import logging
from typing import Optional

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, TableDescriptor, Schema
from pyflink.table.types import DataTypes

from flink.common.config import FlinkConfig

logger = logging.getLogger(__name__)


class KafkaSourceFactory:
    """
    Factory for creating Kafka sources in Flink.
    
    Uses Table API for better cloud compatibility.
    """

    def __init__(self, config: FlinkConfig):
        """
        Initialize the factory with configuration.
        
        Args:
            config: Flink configuration
        """
        self.config = config

    def create_order_source_ddl(self, table_name: str = "orders_source") -> str:
        """
        Create DDL statement for order events Kafka source.
        
        Args:
            table_name: Name for the source table
            
        Returns:
            DDL statement string
        """
        from flink.common.config import is_aws_environment
        
        kafka_config = self.config.kafka
        
        # Base properties
        kafka_props = f"""
            'connector' = 'kafka',
            'topic' = '{kafka_config.topic}',
            'properties.bootstrap.servers' = '{kafka_config.bootstrap_servers}',
            'properties.group.id' = '{kafka_config.consumer_group}',
            'scan.startup.mode' = '{kafka_config.starting_offset}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        """
        
        # Add MSK IAM authentication for AWS
        if is_aws_environment():
            kafka_props += """,
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'AWS_MSK_IAM',
            'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
            'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
            """
        
        ddl = f"""
        CREATE TABLE {table_name} (
            event_id STRING,
            event_time TIMESTAMP(3),
            order_id STRING,
            user_id STRING,
            order_amount DECIMAL(10, 2),
            currency STRING,
            order_status STRING,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            {kafka_props}
        )
        """
        
        logger.info(f"Created Kafka source DDL for table: {table_name} (AWS: {is_aws_environment()})")
        return ddl

    def register_source(
        self,
        table_env: StreamTableEnvironment,
        table_name: str = "orders_source"
    ) -> None:
        """
        Register the Kafka source table in the table environment.
        
        Args:
            table_env: Flink table environment
            table_name: Name for the source table
        """
        ddl = self.create_order_source_ddl(table_name)
        table_env.execute_sql(ddl)
        logger.info(f"Registered Kafka source table: {table_name}")
