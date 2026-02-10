"""
Iceberg Sink Factory

Dynamically creates Iceberg sink tables based on topic configuration.
"""

import logging
from pyflink.table import StreamTableEnvironment
from typing import Dict, Any

logger = logging.getLogger(__name__)


class IcebergSinkFactory:
    """Factory for creating Iceberg sink tables."""
    
    def __init__(self, catalog_name: str, namespace: str, topic_config: Dict[str, Any]):
        """Initialize Iceberg sink factory.
        
        Args:
            catalog_name: Iceberg catalog name
            namespace: Database/namespace name
            topic_config: Topic configuration
        """
        self.catalog_name = catalog_name
        self.namespace = namespace
        self.topic_config = topic_config
        self.sink_config = topic_config.get('sink', {})
    
    def create_sink_table(self, table_env: StreamTableEnvironment) -> str:
        """Create Iceberg sink table in Flink.
        
        Args:
            table_env: Flink table environment
            
        Returns:
            Fully qualified sink table name
        """
        table_name = self.sink_config.get('table_name', 'unknown_table')
        full_table_name = f"{self.catalog_name}.{self.namespace}.{table_name}"
        
        # Build schema DDL
        schema_ddl = self._build_schema_ddl()
        
        # Create table DDL
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {schema_ddl}
            )
        """
        
        table_env.execute_sql(ddl)
        logger.info(f"✓ Created Iceberg sink: {full_table_name}")
        
        return full_table_name
    
    def _build_schema_ddl(self) -> str:
        """Build schema DDL from sink configuration.
        
        Returns:
            Schema DDL string
        """
        sink_schema = self.sink_config.get('schema', [])
        schema_lines = []
        
        for field in sink_schema:
            field_name = field['name']
            field_type = field['type']
            schema_lines.append(f"{field_name} {field_type}")
        
        return ",\n                ".join(schema_lines)
