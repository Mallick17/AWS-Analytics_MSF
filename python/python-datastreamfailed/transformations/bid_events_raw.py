from transformations.base_transformer import BaseTransformer
from typing import Dict, List, Optional

class BidEventsRawTransformer(BaseTransformer):
    def get_sink_schema(self) -> Dict[str, str]:
        return {
            "event_name": "STRING",
            "user_id": "BIGINT",
            "city_id": "INT",
            "platform": "STRING",
            "session_id": "STRING",
            "event_time": "TIMESTAMP(3)",
            "ingestion_time": "TIMESTAMP(3)"
        }
    
    def get_partition_by(self) -> Optional[List[str]]:
        return None  # No partitioning for raw events
    
    def get_transformation_sql(self, source_table: str) -> str:
        return f"""
            SELECT
                event_name,
                user_id,
                city_id,
                platform,
                session_id,
                TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS event_time,
                CURRENT_TIMESTAMP AS ingestion_time
            FROM default_catalog.default_database.{source_table}
        """
