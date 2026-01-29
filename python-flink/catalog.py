# catalog.py
from utils import log_message

def create_iceberg_catalog(table_env, s3_warehouse, namespace):
    """Create and set up the Iceberg catalog."""
    log_message("Creating Iceberg catalog")
    table_env.execute_sql(f"""
        CREATE CATALOG s3_tables WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
            'warehouse' = '{s3_warehouse}'
        )
    """)
    table_env.use_catalog("s3_tables")
    table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {namespace}")
    table_env.use_database(namespace)
    log_message("Iceberg catalog ready")