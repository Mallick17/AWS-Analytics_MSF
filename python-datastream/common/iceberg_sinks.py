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
            iceberg_config: Iceberg configuration
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
        """
        sink_table_name = topic_config['sink']['table_name']
        print(f"Creating Iceberg sink table: {sink_table_name}")
        
        # Build schema DDL from sink schema
        schema_fields = []
        for field in topic_config['sink']['schema']:
            schema_fields.append(f"{field['name']} {field['type']}")
        schema_ddl = ",\n            ".join(schema_fields)
        
        # Switch to Iceberg catalog
        self.table_env.use_catalog(self.catalog_manager.get_catalog_name())
        self.table_env.use_database(self.catalog_manager.get_namespace())
        
        # Build CREATE TABLE statement
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {sink_table_name} (
                {schema_ddl}
            ) WITH (
                'format-version' = '{self.iceberg_config["format_version"]}',
                'write.format.default' = '{self.iceberg_config["write_format"]}',
                'write.parquet.compression-codec' = '{self.iceberg_config["compression_codec"]}'
            )
        """
        
        self.table_env.execute_sql(create_table_sql)
        print(f"✓ Iceberg sink created: {sink_table_name}")
        
        return sink_table_name
