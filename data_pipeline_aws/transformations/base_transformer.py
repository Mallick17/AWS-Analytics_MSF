# ==================================================================================
# BASE TRANSFORMER
# ==================================================================================
# Base class for all transformations providing common functionality.
# All transformers should inherit from this class.
# ==================================================================================

from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseTransformer(ABC):
    """Base class for all transformations.
    
    Provides common functionality for all transformers including:
    - Configuration validation
    - Topic name access
    - Abstract methods for transformation logic
    """
    
    def __init__(self, topic_config: Dict[str, Any]):
        """Initialize transformer with topic configuration.
        
        Args:
            topic_config: Configuration dictionary for the topic
        """
        self.topic_config = topic_config
        self.topic_name = topic_config.get('name', 'unknown')
        self.sink_config = topic_config.get('sink', {})
    
    @abstractmethod
    def get_transformation_sql(self, source_table: str) -> str:
        """Generate transformation SQL for this topic.
        
        Args:
            source_table: Fully qualified Kafka source table name
            
        Returns:
            SQL SELECT statement with transformation logic
        """
        pass
    
    @abstractmethod
    def get_description(self) -> str:
        """Get human-readable description of this transformation.
        
        Returns:
            Description string
        """
        pass
    
    def validate_config(self) -> bool:
        """Validate that topic configuration has required fields.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        required_fields = ['source_schema', 'sink']
        return all(field in self.topic_config for field in required_fields)
    
    def get_sink_table_name(self) -> str:
        """Get the sink table name from configuration.
        
        Returns:
            Sink table name
        """
        return self.sink_config.get('table_name', 'unknown_table')
