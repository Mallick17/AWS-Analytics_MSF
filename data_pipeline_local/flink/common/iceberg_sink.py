"""
Iceberg Sink Factory

Creates Flink Iceberg sinks for writing to S3-backed Iceberg tables.
Cloud-compatible implementation using Hadoop catalog.
"""

import logging
from typing import Optional

from pyflink.table import StreamTableEnvironment

from flink.common.config import FlinkConfig

logger = logging.getLogger(__name__)


class IcebergSinkFactory:
    """
    Factory for creating Iceberg sinks in Flink.
    
    Uses Hadoop catalog for local development (no Glue dependency).
    Cloud deployment can switch to AWS Glue catalog via configuration.
    """

    def __init__(self, config: FlinkConfig):
        """
        Initialize the factory with configuration.
        
        Args:
            config: Flink configuration
        """
        self.config = config

    def create_catalog_ddl(self) -> str:
        """
        Create DDL statement for Iceberg catalog.
        
        Returns:
            DDL statement string
        """
        iceberg_config = self.config.iceberg
        s3_config = self.config.s3
        
        ddl = f"""
        CREATE CATALOG {iceberg_config.catalog_name} WITH (
            'type' = 'iceberg',
            'catalog-type' = 'hadoop',
            'warehouse' = '{iceberg_config.warehouse}',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = '{s3_config.endpoint_url}',
            's3.access-key-id' = '{s3_config.access_key}',
            's3.secret-access-key' = '{s3_config.secret_key}',
            's3.path-style-access' = 'true'
        )
        """
        
        logger.info(f"Created Iceberg catalog DDL: {iceberg_config.catalog_name}")
        return ddl

    def create_orders_table_ddl(self, table_name: str = "orders") -> str:
        """
        Create DDL statement for orders Iceberg table.
        
        Args:
            table_name: Name for the sink table
            
        Returns:
            DDL statement string
        """
        iceberg_config = self.config.iceberg
        full_table_name = f"{iceberg_config.catalog_name}.{iceberg_config.namespace}.{table_name}"
        
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            event_id STRING,
            event_time TIMESTAMP(3),
            order_id STRING,
            user_id STRING,
            order_amount DECIMAL(10, 2),
            currency STRING,
            order_status STRING,
            processed_at TIMESTAMP(3)
        ) PARTITIONED BY (days(event_time))
        """
        
        logger.info(f"Created Iceberg table DDL: {full_table_name}")
        return ddl

    def register_catalog(self, table_env: StreamTableEnvironment) -> None:
        """
        Register the Iceberg catalog in the table environment.
        
        Args:
            table_env: Flink table environment
        """
        ddl = self.create_catalog_ddl()
        table_env.execute_sql(ddl)
        
        # Create namespace if not exists
        iceberg_config = self.config.iceberg
        table_env.execute_sql(
            f"CREATE DATABASE IF NOT EXISTS {iceberg_config.catalog_name}.{iceberg_config.namespace}"
        )
        
        logger.info(f"Registered Iceberg catalog: {iceberg_config.catalog_name}")

    def register_orders_table(
        self,
        table_env: StreamTableEnvironment,
        table_name: str = "orders"
    ) -> str:
        """
        Register the orders Iceberg table in the table environment.
        
        Args:
            table_env: Flink table environment
            table_name: Name for the sink table
            
        Returns:
            Full table name (catalog.namespace.table)
        """
        ddl = self.create_orders_table_ddl(table_name)
        table_env.execute_sql(ddl)
        
        iceberg_config = self.config.iceberg
        full_table_name = f"{iceberg_config.catalog_name}.{iceberg_config.namespace}.{table_name}"
        
        logger.info(f"Registered Iceberg table: {full_table_name}")
        return full_table_name
