from pyflink.table import TableEnvironment
from common.config import IcebergConfig

class CatalogManager:
    def __init__(self, table_env: TableEnvironment, config: IcebergConfig):
        self.table_env = table_env
        self.config = config
    
    def setup_iceberg_catalog(self):
        """Create and configure Iceberg catalog"""
        print(f"Setting up Iceberg catalog: {self.config.catalog_name}")
        
        self.table_env.execute_sql(f"""
            CREATE CATALOG {self.config.catalog_name} WITH (
                'type' = 'iceberg',
                'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
                'warehouse' = '{self.config.warehouse_arn}'
            )
        """)
        
        self.table_env.use_catalog(self.config.catalog_name)
        
        self.table_env.execute_sql(
            f"CREATE DATABASE IF NOT EXISTS {self.config.namespace}"
        )
        
        self.table_env.use_database(self.config.namespace)
        print(f"✓ Catalog ready: {self.config.catalog_name}.{self.config.namespace}")