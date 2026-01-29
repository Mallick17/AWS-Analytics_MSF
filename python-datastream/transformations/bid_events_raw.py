# ==================================================================================
# BID EVENTS RAW TRANSFORMER
# ==================================================================================
# Transforms raw bid events from Kafka to Iceberg format.
# Performs timestamp conversion from milliseconds to TIMESTAMP type.
# ==================================================================================

from transformations.base_transformer import BaseTransformer
from typing import Dict, Any


class BidEventsRawTransformer(BaseTransformer):
    """Transformer for raw bid events.
    
    This transformer:
    - Converts timestamp_ms (BIGINT) to event_time (TIMESTAMP)
    - Adds ingestion_time timestamp
    - Passes through all other fields
    """
    
    def __init__(self, topic_config: Dict[str, Any]):
        """Initialize transformer.
        
        Args:
            topic_config: Configuration for bid-events topic
        """
        super().__init__(topic_config)
    
    def get_transformation_sql(self, source_table: str) -> str:
        """Generate transformation SQL for bid events.
        
        Args:
            source_table: Fully qualified Kafka source table name
            
        Returns:
            SQL SELECT statement
        """
        sql = f"""
            SELECT
                event_name,
                user_id,
                city_id,
                platform,
                session_id,
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
        return "Raw bid events transformation with timestamp conversion"
