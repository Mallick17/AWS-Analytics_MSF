# ==================================================================================
# USER EVENTS PROCESSED TRANSFORMER
# ==================================================================================
# Transforms and processes user events from Kafka to Iceberg format.
# Adds computed fields like is_mobile based on device_type.
# ==================================================================================

from transformations.base_transformer import BaseTransformer
from typing import Dict, Any


class UserEventsProcessedTransformer(BaseTransformer):
    """Transformer for user events with processing.
    
    This transformer:
    - Converts timestamp_ms (BIGINT) to event_time (TIMESTAMP)
    - Adds ingestion_time timestamp
    - Computes is_mobile field based on device_type
    - Passes through all other fields
    """
    
    def __init__(self, topic_config: Dict[str, Any]):
        """Initialize transformer.
        
        Args:
            topic_config: Configuration for user-events topic
        """
        super().__init__(topic_config)
    
    def get_transformation_sql(self, source_table: str) -> str:
        """Generate transformation SQL for user events.
        
        Args:
            source_table: Fully qualified Kafka source table name
            
        Returns:
            SQL SELECT statement with processing logic
        """
        sql = f"""
            SELECT
                event_type,
                user_id,
                page_url,
                device_type,
                session_id,
                CASE 
                    WHEN LOWER(device_type) IN ('mobile', 'tablet', 'smartphone') THEN TRUE
                    ELSE FALSE
                END AS is_mobile,
                TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS event_time,
                CURRENT_TIMESTAMP AS ingestion_time
            FROM {source_table}
        """
        return sql
    
    def get_description(self) -> str:
        """Get description of this transformer.
        
        Returns:
            Description string
        """
        return "Processed user events transformation with device detection and timestamp conversion"
