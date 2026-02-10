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
            
        Raises:
            ValueError: If required configuration is missing
            Exception: If table creation fails
        """
        print(f"    Creating Kafka source for topic: {topic_name}")
        
        # Generate table name (sanitize topic name for SQL)
        kafka_table_name = f"kafka_{topic_name.replace('-', '_')}"
        
        # Validate source_schema exists
        if 'source_schema' not in topic_config:
            raise ValueError(f"Missing 'source_schema' in topic config for {topic_name}")
        
        # Build schema DDL from source_schema
        source_schema = topic_config['source_schema']
        schema_cols = [f"`{field['name']}` {field['type']}" for field in source_schema]
        print(f"    Source schema: {len(source_schema)} fields")
        
        # Switch to default catalog for Kafka tables
        self.table_env.use_catalog("default_catalog")
        self.table_env.use_database("default_database")
        
        # Build CREATE TABLE statement
        kafka_ddl = f"""
            CREATE TABLE {kafka_table_name} (
                {', '.join(schema_cols)}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic_name}',
                'properties.bootstrap.servers' = '{self.kafka_config.get("bootstrap_servers")}',
                'properties.group.id' = '{topic_config.get("kafka_group_id", "flink-default")}',
                'scan.startup.mode' = '{topic_config.get("scan_mode", "latest-offset")}',
                'format' = '{topic_config.get("format", "json")}',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = '{self.kafka_config.get("security", {}).get("protocol", "SASL_SSL")}',
                'properties.sasl.mechanism' = '{self.kafka_config.get("security", {}).get("sasl_mechanism", "AWS_MSK_IAM")}',
                'properties.sasl.jaas.config' = '{self.kafka_config.get("security", {}).get("sasl_jaas_config", "software.amazon.msk.auth.iam.IAMLoginModule required;")}',
                'properties.sasl.client.callback.handler.class' = '{self.kafka_config.get("security", {}).get("sasl_callback_handler", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")}'
            )
        """
        
        # Execute DDL
        try:
            self.table_env.execute_sql(kafka_ddl)
            print(f"    ✓ Kafka source created: {kafka_table_name}")
        except Exception as e:
            error_msg = str(e).lower()
            # Table already exists is acceptable
            if "already exists" in error_msg or f"table {kafka_table_name} exists" in error_msg:
                print(f"    ✓ Kafka source already exists: {kafka_table_name}")
            else:
                print(f"    ✗ Failed to create Kafka source: {e}")
                raise
        
        return kafka_table_name