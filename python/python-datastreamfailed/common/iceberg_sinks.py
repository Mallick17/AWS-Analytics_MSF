from pyflink.table import TableEnvironment
from typing import Dict, List, Optional

class IcebergSinkFactory:
    def __init__(self, table_env: TableEnvironment, catalog_name: str, namespace: str):
        self.table_env = table_env
        self.catalog_name = catalog_name
        self.namespace = namespace
    
    def create_sink(
        self, 
        table_name: str, 
        schema: Dict[str, str],
        partition_by: Optional[List[str]] = None
    ):
        """Create Iceberg sink table"""
        
        # Switch to Iceberg catalog
        self.table_env.use_catalog(self.catalog_name)
        self.table_env.use_database(self.namespace)
        
        schema_ddl = ",\n            ".join([
            f"{col_name} {col_type}" 
            for col_name, col_type in schema.items()
        ])
        
        partition_clause = ""
        if partition_by:
            partition_clause = f"PARTITIONED BY ({', '.join(partition_by)})"
        
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {schema_ddl}
            ) {partition_clause}
            WITH (
                'format-version' = '2',
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """
        
        self.table_env.execute_sql(ddl)
        print(f"✓ Created Iceberg sink: {table_name}")