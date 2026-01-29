# ==================================================================================
# BASE TRANSFORMER
# ==================================================================================
# Abstract base class for all transformations.
# All custom transformers should inherit from this class.
# ==================================================================================

from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseTransformer(ABC):
    """Abstract base class for data transformations.
    
    All transformer classes must inherit from this class and implement
    the get_transformation_sql() method.
    """
    
    def __init__(self, topic_config: Dict[str, Any]):
        """Initialize transformer.
        
        Args:
            topic_config: Configuration for the topic being processed
        """
        self.topic_config = topic_config
    
    @abstractmethod
    def get_transformation_sql(self, source_table: str) -> str:
        """Generate the SQL transformation query.
        
        Args:
            source_table: Fully qualified name of the source Kafka table
            
        Returns:
            SQL SELECT statement for the transformation
        """
        pass
    
    def get_description(self) -> str:
        """Get a description of this transformation.
        
        Returns:
            Description string
        """
        return self.__class__.__name__
