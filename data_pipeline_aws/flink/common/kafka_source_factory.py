"""
Kafka Source Factory

Dynamically creates Kafka source tables based on topic configuration.
"""

import logging
from pyflink.table import StreamTableEnvironment
from typing import Dict, Any

logger = logging.getLogger(__name__)


def is_aws_environment() -> bool:
    """Check if running on AWS Managed Flink"""
    import os
    return os.getenv("DEPLOYMENT_ENV") == "aws"


class KafkaSourceFactory:
    """Factory for creating Kafka source tables."""
    
    def __init__(self, kafka_config: Dict[str, Any], topic_config: Dict[str, Any]):
        """Initialize Kafka source factory.
        
        Args:
            kafka_config: Global Kafka configuration
            topic_config: Specific topic configuration
        """
        self.kafka_config = kafka_config
        self.topic_config = topic_config
        self.topic_name = topic_config['name']
    
    def create_source_table(self, table_env: StreamTableEnvironment) -> str:
        """Create Kafka source table in Flink.
        
        Args:
            table_env: Flink table environment
            
        Returns:
            Fully qualified source table name
        """
        source_table_name = f"kafka_source_{self.topic_name.replace('.', '_').replace('-', '_')}"
        
        # Build schema DDL
        schema_ddl = self._build_schema_ddl()
        
        # Build Kafka properties
        kafka_props = self._build_kafka_properties()
        
        # Create table DDL
        ddl = f"""
            CREATE TABLE {source_table_name} (
                {schema_ddl}
            ) WITH (
                {kafka_props}
            )
        """
        
        table_env.execute_sql(ddl)
        logger.info(f"✓ Created Kafka source: {source_table_name} for topic {self.topic_name}")
        
        return source_table_name
    
    def _build_schema_ddl(self) -> str:
        """Build schema DDL from topic configuration.
        
        Returns:
            Schema DDL string
        """
        source_schema = self.topic_config.get('source_schema', [])
        schema_lines = []
        
        for field in source_schema:
            field_name = field['name']
            field_type = field['type']
            schema_lines.append(f"{field_name} {field_type}")
        
        return ",\n                ".join(schema_lines)
    
    def _build_kafka_properties(self) -> str:
        """Build Kafka connector properties.
        
        Returns:
            Kafka properties string
        """
        bootstrap_servers = self.kafka_config['bootstrap_servers']
        group_id = self.topic_config.get('kafka_group_id', 'flink-default')
        scan_mode = self.topic_config.get('scan_mode', 'earliest-offset')
        format_type = self.topic_config.get('format', 'json')
        
        props = [
            "'connector' = 'kafka'",
            f"'topic' = '{self.topic_name}'",
            f"'properties.bootstrap.servers' = '{bootstrap_servers}'",
            f"'properties.group.id' = '{group_id}'",
            f"'scan.startup.mode' = '{scan_mode}'",
            f"'format' = '{format_type}'",
        ]
        
        # Add JSON-specific properties
        if format_type == 'json':
            props.extend([
                "'json.timestamp-format.standard' = 'ISO-8601'",
                "'json.fail-on-missing-field' = 'false'",
                "'json.ignore-parse-errors' = 'true'"
            ])
        
        # Add MSK IAM authentication for AWS
        if is_aws_environment():
            security = self.kafka_config.get('security', {})
            props.extend([
                f"'properties.security.protocol' = '{security.get('protocol', 'SASL_SSL')}'",
                f"'properties.sasl.mechanism' = '{security.get('sasl_mechanism', 'AWS_MSK_IAM')}'",
                f"'properties.sasl.jaas.config' = '{security.get('sasl_jaas_config', '')}'",
                f"'properties.sasl.client.callback.handler.class' = '{security.get('sasl_callback_handler', '')}'"
            ])
        
        return ",\n                ".join(props)
