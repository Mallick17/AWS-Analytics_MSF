from pyflink.table import TableEnvironment
from typing import Dict, List, Optional
from abc import ABC, abstractmethod

class BaseTransformer(ABC):
    def __init__(self, table_env: TableEnvironment):
        self.table_env = table_env
    
    @abstractmethod
    def get_sink_schema(self) -> Dict[str, str]:
        """Return the schema for the sink table"""
        pass
    
    @abstractmethod
    def get_partition_by(self) -> Optional[List[str]]:
        """Return partition columns (or None)"""
        pass
    
    @abstractmethod
    def get_transformation_sql(self, source_table: str) -> str:
        """Return the SELECT query for transformation"""
        pass
    
    def get_insert_sql(
        self, 
        source_table: str, 
        sink_table: str,
        catalog_name: str,
        namespace: str
    ) -> str:
        """Generate the full INSERT INTO statement"""
        # Get the transformation SELECT
        select_sql = self.get_transformation_sql(source_table)
        
        # Build full INSERT statement
        insert_sql = f"""
            INSERT INTO {catalog_name}.{namespace}.{sink_table}
            {select_sql}
        """
        
        return insert_sql
