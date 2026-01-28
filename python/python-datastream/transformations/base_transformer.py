from abc import ABC, abstractmethod
from pyflink.table import TableEnvironment
from typing import Dict

class BaseTransformer(ABC):
    def __init__(self, table_env: TableEnvironment):
        self.table_env = table_env
    
    @abstractmethod
    def get_sink_schema(self) -> Dict[str, str]:
        """Define output schema"""
        pass
    
    @abstractmethod
    def get_transformation_sql(self, source_table: str, sink_table: str) -> str:
        """Define transformation logic"""
        pass
    
    def get_partition_by(self):
        """Optional: Define partitioning strategy"""
        return None
    
    def execute(self, source_table: str, sink_table: str):
        """Execute the transformation"""
        sql = self.get_transformation_sql(source_table, sink_table)
        print(f"Executing: {self.__class__.__name__}")
        return self.table_env.execute_sql(sql)