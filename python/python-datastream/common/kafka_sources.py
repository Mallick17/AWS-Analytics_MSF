from pyflink.table import TableEnvironment
from typing import Dict
from common.config import KafkaTopicConfig

class KafkaSourceFactory:
    def __init__(self, table_env: TableEnvironment, bootstrap_servers: str):
        self.table_env = table_env
        self.bootstrap_servers = bootstrap_servers
    
    def create_source(self, table_name: str, topic_config: KafkaTopicConfig):
        """Create Kafka source table from configuration"""
        
        # Switch to default catalog
        self.table_env.use_catalog("default_catalog")
        self.table_env.use_database("default_database")
        
        # Build schema DDL
        schema_ddl = ",\n            ".join([
            f"{col_name} {col_type}" 
            for col_name, col_type in topic_config.schema.items()
        ])
        
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {schema_ddl}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic_config.topic_name}',
                'properties.bootstrap.servers' = '{self.bootstrap_servers}',
                'properties.group.id' = '{topic_config.consumer_group}',
                'scan.startup.mode' = '{topic_config.startup_mode}',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'AWS_MSK_IAM',
                'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
                'properties.sasl.client.callback.handler.class' = 
                    'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
            )
        """
        
        self.table_env.execute_sql(ddl)
        print(f"✓ Created Kafka source: {table_name} <- {topic_config.topic_name}")