"""
Catalog Manager

Handles creation and management of Iceberg catalogs for both local and AWS environments.
"""

import os
import logging
from pyflink.table import StreamTableEnvironment
from typing import Tuple

logger = logging.getLogger(__name__)


def is_aws_environment() -> bool:
    """Check if running on AWS Managed Flink"""
    return os.getenv("DEPLOYMENT_ENV") == "aws"


class CatalogManager:
    """Manages Iceberg catalog creation and database setup."""
    
    def __init__(self, table_env: StreamTableEnvironment, iceberg_config: dict):
        """Initialize catalog manager.
        
        Args:
            table_env: Flink table environment
            iceberg_config: Iceberg configuration dictionary
        """
        self.table_env = table_env
        self.iceberg_config = iceberg_config
    
    def create_catalog(self) -> Tuple[str, str]:
        """Create appropriate Iceberg catalog based on environment.
        
        Returns:
            Tuple of (catalog_name, namespace)
        """
        catalog_name = self.iceberg_config['catalog_name']
        namespace = self.iceberg_config['namespace']
        
        if is_aws_environment():
            self._create_s3_tables_catalog(catalog_name)
        else:
            self._create_rest_catalog(catalog_name)
        
        # Create database/namespace
        self.table_env.execute_sql(
            f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{namespace}"
        )
        
        logger.info(f"✓ Created catalog: {catalog_name}.{namespace}")
        return catalog_name, namespace
    
    def _create_s3_tables_catalog(self, catalog_name: str):
        """Create AWS S3 Tables catalog.
        
        Args:
            catalog_name: Name for the catalog
        """
        warehouse = self.iceberg_config['warehouse']
        region = self.iceberg_config['region']
        
        ddl = f"""
            CREATE CATALOG {catalog_name} WITH (
                'type' = 'iceberg',
                'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
                'warehouse' = '{warehouse}',
                'region' = '{region}'
            )
        """
        
        self.table_env.execute_sql(ddl)
        logger.info(f"Created S3 Tables catalog: {catalog_name}")
    
    def _create_rest_catalog(self, catalog_name: str):
        """Create Iceberg REST catalog for local development.
        
        Args:
            catalog_name: Name for the catalog
        """
        rest_uri = self.iceberg_config['rest_uri']
        warehouse = self.iceberg_config['warehouse']
        
        ddl = f"""
            CREATE CATALOG {catalog_name} WITH (
                'type' = 'iceberg',
                'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
                'uri' = '{rest_uri}',
                'warehouse' = '{warehouse}'
            )
        """
        
        self.table_env.execute_sql(ddl)
        logger.info(f"Created REST catalog: {catalog_name}")
