# ==================================================================================
# ICEBERG SINK CREATOR
# ==================================================================================
# Creates Iceberg sink tables dynamically based on topic configuration.
# ==================================================================================

from typing import Dict, Any
from pyflink.table import TableEnvironment


class IcebergSinkCreator:
    """Creates Iceberg sink tables."""
    
    def __init__(self, table_env: TableEnvironment, catalog_manager, iceberg_config: Dict[str, Any]):
        """Initialize Iceberg sink creator.
        
        Args:
            table_env: Flink TableEnvironment
            catalog_manager: CatalogManager instance
            iceberg_config: Iceberg configuration dictionary
        """
        self.table_env = table_env
        self.catalog_manager = catalog_manager
        self.iceberg_config = iceberg_config
    
    def create_sink(self, topic_config: Dict[str, Any]) -> str:
        """Create an Iceberg sink table.
        
        Args:
            topic_config: Topic configuration from YAML
            
        Returns:
            Name of the created sink table
            
        Raises:
            ValueError: If required configuration is missing
            Exception: If table creation fails
        """
        # Validate sink configuration exists
        if 'sink' not in topic_config:
            raise ValueError("Missing 'sink' configuration in topic config")
        
        sink_config = topic_config['sink']
        
        if 'table_name' not in sink_config:
            raise ValueError("Missing 'table_name' in sink configuration")
        
        if 'schema' not in sink_config:
            raise ValueError("Missing 'schema' in sink configuration")
        
        sink_table_name = sink_config['table_name']
        print(f"    Creating Iceberg sink table: {sink_table_name}")
        
        # Build schema DDL from sink schema
        sink_schema = sink_config['schema']
        sink_cols = [f"`{field['name']}` {field['type']}" for field in sink_schema]
        print(f"    Sink schema: {len(sink_schema)} fields")
        
        # Switch to Iceberg catalog
        catalog_name = self.catalog_manager.get_catalog_name()
        namespace = self.catalog_manager.get_namespace()
        
        self.table_env.use_catalog(catalog_name)
        self.table_env.use_database(namespace)
        
        # Build CREATE TABLE statement
        sink_ddl = f"""
            CREATE TABLE {sink_table_name} (
                {', '.join(sink_cols)}
            ) WITH (
                'format-version' = '{self.iceberg_config.get("format_version", "2")}',
                'write.format.default' = '{self.iceberg_config.get("write_format", "parquet")}',
                'write.parquet.compression-codec' = '{self.iceberg_config.get("compression_codec", "snappy")}'
            )
        """
        
        # Execute DDL
        try:
            self.table_env.execute_sql(sink_ddl)
            print(f"    ✓ Sink table created: {sink_table_name}")
        except Exception as e:
            error_msg = str(e).lower()
            # Table already exists is acceptable
            if "already exists" in error_msg or f"table {sink_table_name} exists" in error_msg:
                print(f"    ✓ Sink table already exists: {sink_table_name}")
            else:
                print(f"    ✗ Failed to create Iceberg sink: {e}")
                raise
        
        return sink_table_name