"""
User Events Enriched Transformer
Transforms user activity events with enrichment and computed fields.
"""

from transformations.base_transformer import BaseTransformer
from typing import Dict, Any


class UserEventsEnrichedTransformer(BaseTransformer):
    """
    Transformer for user activity events.
    
    Transformations:
    - Converts timestamp_ms (BIGINT) to event_time (TIMESTAMP)
    - Adds ingestion_time timestamp
    - Computes is_mobile (BOOLEAN) from device_type
    - Passes through all other fields
    """
    
    def __init__(self, topic_config: Dict[str, Any]):
        """Initialize with topic configuration."""
        super().__init__(topic_config)
    
    def get_transformation_sql(self, source_table: str) -> str:
        """
        Generate transformation SQL for user events.
        
        This transformation enriches user events by:
        1. Converting timestamp to proper TIMESTAMP type
        2. Adding computed field 'is_mobile' based on device_type
        3. Adding ingestion timestamp
        
        Args:
            source_table: Fully qualified Kafka source table name
            
        Returns:
            SELECT statement with enrichment logic
        """
        sql = f"""
            SELECT
                `event_type`,
                `user_id`,
                `page_url`,
                `device_type`,
                `session_id`,
                CASE 
                    WHEN LOWER(`device_type`) IN ('ios', 'android', 'mobile') THEN TRUE
                    ELSE FALSE
                END AS `is_mobile`,
                TO_TIMESTAMP_LTZ(`timestamp_ms`, 3) AS `event_time`,
                CURRENT_TIMESTAMP AS `ingestion_time`
            FROM {source_table}
        """
        return sql
    
    def get_description(self) -> str:
        return "Enriches user events with computed is_mobile field and timestamp conversion"