from pyflink.table import TableEnvironment

class CatalogManager:
    def __init__(self, table_env: TableEnvironment, s3_warehouse: str, namespace: str):
        self.table_env = table_env
        self.s3_warehouse = s3_warehouse
        self.namespace = namespace
        self.catalog_name = "s3_tables"
    
    def setup(self) -> str:
        """Setup Iceberg catalog with S3 Tables implementation"""
        print(f"  Creating catalog: {self.catalog_name}")
        print(f"  Warehouse: {self.s3_warehouse}")
        print(f"  Namespace: {self.namespace}")
        
        # Create Iceberg catalog with S3 Tables implementation
        self.table_env.execute_sql(f"""
            CREATE CATALOG {self.catalog_name} WITH (
                'type' = 'iceberg',
                'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
                'warehouse' = '{self.s3_warehouse}'
            )
        """)
        
        # Switch to catalog and create database
        self.table_env.use_catalog(self.catalog_name)
        self.table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {self.namespace}")
        self.table_env.use_database(self.namespace)
        
        print(f"  ✓ Catalog setup complete")
        return self.catalog_name
