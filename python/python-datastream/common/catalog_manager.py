from pyflink.table import TableEnvironment

class CatalogManager:
    def __init__(self, table_env: TableEnvironment, warehouse_arn: str, namespace: str):
        self.table_env = table_env
        self.warehouse_arn = warehouse_arn
        self.namespace = namespace
        self.catalog_name = "s3_tables"
    
    def setup(self):
        """Initialize Iceberg catalog"""
        print(f"Setting up Iceberg catalog...")
        
        self.table_env.execute_sql(f"""
            CREATE CATALOG {self.catalog_name} WITH (
                'type' = 'iceberg',
                'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
                'warehouse' = '{self.warehouse_arn}'
            )
        """)
        
        self.table_env.use_catalog(self.catalog_name)
        self.table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {self.namespace}")
        self.table_env.use_database(self.namespace)
        
        print(f"✓ Catalog ready: {self.catalog_name}.{self.namespace}")
        
        return self.catalog_name