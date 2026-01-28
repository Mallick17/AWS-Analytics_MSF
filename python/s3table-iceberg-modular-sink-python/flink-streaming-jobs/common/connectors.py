from pyflink.table import TableEnvironment
from typing import Dict, List
from common.config import KafkaConfig

class KafkaSourceBuilder:
    def __init__(self, table_env: TableEnvironment, kafka_config: KafkaConfig):
        self.table_env = table_env
        self.kafka_config = kafka_config
    
    def create_json_source(
        self,
        table_name: str,
        topic: str,
        schema: Dict[str, str],
        consumer_group: str,
        startup_mode: str = "earliest-offset"
    ):
        """Create Kafka source with JSON format"""
        
        # Switch to default catalog for source tables
        self.table_env.use_catalog("default_catalog")
        self.table_env.use_database("default_database")
        
        # Build schema DDL
        schema_ddl = ",\n            ".join(
            [f"{col} {dtype}" for col, dtype in schema.items()]
        )
        
        ddl = f"""
            CREATE TABLE {table_name} (
                {schema_ddl}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic}',
                'properties.bootstrap.servers' = '{self.kafka_config.bootstrap_servers}',
                'properties.group.id' = '{consumer_group}',
                'scan.startup.mode' = '{startup_mode}',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = '{self.kafka_config.security_protocol}',
                'properties.sasl.mechanism' = '{self.kafka_config.sasl_mechanism}',
                'properties.sasl.jaas.config' = '{self.kafka_config.sasl_jaas_config}',
                'properties.sasl.client.callback.handler.class' = '{self.kafka_config.sasl_callback_handler}'
            )
        """
        
        self.table_env.execute_sql(ddl)
        print(f"✓ Kafka source created: {table_name} <- {topic}")

class IcebergSinkBuilder:
    def __init__(self, table_env: TableEnvironment, catalog_name: str, namespace: str):
        self.table_env = table_env
        self.catalog_name = catalog_name
        self.namespace = namespace
    
    def create_sink(
        self,
        table_name: str,
        schema: Dict[str, str],
        partition_by: List[str] = None
    ):
        """Create Iceberg sink table"""
        
        # Switch to Iceberg catalog
        self.table_env.use_catalog(self.catalog_name)
        self.table_env.use_database(self.namespace)
        
        schema_ddl = ",\n            ".join(
            [f"{col} {dtype}" for col, dtype in schema.items()]
        )
        
        partitioning = ""
        if partition_by:
            partitioning = f"PARTITIONED BY ({', '.join(partition_by)})"
        
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {schema_ddl}
            ) {partitioning}
            WITH (
                'format-version' = '2',
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """
        
        self.table_env.execute_sql(ddl)
        print(f"✓ Iceberg sink created: {table_name}")