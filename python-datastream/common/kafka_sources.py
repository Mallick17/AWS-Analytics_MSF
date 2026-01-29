# ==================================================================================
# KAFKA SOURCE CREATOR
# ==================================================================================
# Creates Kafka source tables dynamically based on topic configuration.
# ==================================================================================

from typing import Dict, Any
from pyflink.table import TableEnvironment


class KafkaSourceCreator:
    """Creates Kafka source tables for topics."""
    
    def __init__(self, table_env: TableEnvironment, kafka_config: Dict[str, Any]):
        """Initialize Kafka source creator.
        
        Args:
            table_env: Flink TableEnvironment
            kafka_config: Kafka connection configuration
        """
        self.table_env = table_env
        self.kafka_config = kafka_config
    
    def create_source(self, topic_name: str, topic_config: Dict[str, Any]) -> str:
        """Create a Kafka source table.
        
        Args:
            topic_name: Name of the Kafka topic
            topic_config: Topic configuration from YAML
            
        Returns:
            Name of the created Kafka source table
        """
        print(f"Creating Kafka source for topic: {topic_name}")
        
        # Generate table name (sanitize topic name for SQL)
        kafka_table_name = f"kafka_{topic_name.replace('-', '_')}"
        
        # Build schema DDL from source_schema
        schema_fields = []
        for field in topic_config['source_schema']:
            schema_fields.append(f"{field['name']} {field['type']}")
        schema_ddl = ",\n            ".join(schema_fields)
        
        # Switch to default catalog for Kafka sources
        self.table_env.use_catalog("default_catalog")
        self.table_env.use_database("default_database")
        
        # Build CREATE TABLE statement
        create_table_sql = f"""
            CREATE TABLE {kafka_table_name} (
                {schema_ddl}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic_name}',
                'properties.bootstrap.servers' = '{self.kafka_config["bootstrap_servers"]}',
                'properties.group.id' = '{topic_config["kafka_group_id"]}',
                'scan.startup.mode' = '{topic_config["scan_mode"]}',
                'format' = '{topic_config["format"]}',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = '{self.kafka_config["security"]["protocol"]}',
                'properties.sasl.mechanism' = '{self.kafka_config["security"]["sasl_mechanism"]}',
                'properties.sasl.jaas.config' = '{self.kafka_config["security"]["sasl_jaas_config"]}',
                'properties.sasl.client.callback.handler.class' = '{self.kafka_config["security"]["sasl_callback_handler"]}'
            )
        """
        
        self.table_env.execute_sql(create_table_sql)
        print(f"✓ Kafka source created: {kafka_table_name}")
        
        return kafka_table_name
