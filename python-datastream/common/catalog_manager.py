# ==================================================================================
# CATALOG MANAGER
# ==================================================================================
# Manages Iceberg catalog creation and database setup.
# ==================================================================================

from typing import Any
from pyflink.table import TableEnvironment


class CatalogManager:
    """Manages Iceberg catalog and database operations."""
    
    def __init__(self, table_env: TableEnvironment, iceberg_config: dict):
        """Initialize catalog manager.
        
        Args:
            table_env: Flink TableEnvironment
            iceberg_config: Iceberg configuration dictionary
        """
        self.table_env = table_env
        self.iceberg_config = iceberg_config
        self.catalog_name = "s3_tables"
    
    def create_catalog(self):
        """Create and configure Iceberg catalog."""
        print("Creating Iceberg catalog...")
        
        # Create Iceberg catalog
        create_catalog_sql = f"""
            CREATE CATALOG {self.catalog_name} WITH (
                'type' = 'iceberg',
                'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
                'warehouse' = '{self.iceberg_config["warehouse"]}'
            )
        """
        self.table_env.execute_sql(create_catalog_sql)
        
        # Use catalog and create database
        self.table_env.use_catalog(self.catalog_name)
        
        create_db_sql = f"CREATE DATABASE IF NOT EXISTS {self.iceberg_config['namespace']}"
        self.table_env.execute_sql(create_db_sql)
        
        self.table_env.use_database(self.iceberg_config['namespace'])
        
        print(f"✓ Iceberg catalog '{self.catalog_name}' created")
        print(f"✓ Using database: {self.iceberg_config['namespace']}")
    
    def get_catalog_name(self) -> str:
        """Get the name of the Iceberg catalog.
        
        Returns:
            Catalog name
        """
        return self.catalog_name
    
    def get_namespace(self) -> str:
        """Get the namespace (database) name.
        
        Returns:
            Namespace name
        """
        return self.iceberg_config['namespace']
