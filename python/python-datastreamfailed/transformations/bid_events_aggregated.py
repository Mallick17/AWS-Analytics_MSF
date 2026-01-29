from transformations.base_transformer import BaseTransformer
from typing import Dict, List, Optional

class BidEventsAggregatedTransformer(BaseTransformer):
    def get_sink_schema(self) -> Dict[str, str]:
        return {
            "window_start": "TIMESTAMP(3)",
            "window_end": "TIMESTAMP(3)",
            "platform": "STRING",
            "event_count": "BIGINT",
            "unique_users": "BIGINT",
            "unique_sessions": "BIGINT"
        }
    
    def get_partition_by(self) -> Optional[List[str]]:
        return ["platform"]  # Partition by platform
    
    def get_transformation_sql(self, source_table: str) -> str:
        return f"""
            SELECT
                TUMBLE_START(TO_TIMESTAMP_LTZ(timestamp_ms, 3), INTERVAL '5' MINUTE) AS window_start,
                TUMBLE_END(TO_TIMESTAMP_LTZ(timestamp_ms, 3), INTERVAL '5' MINUTE) AS window_end,
                platform,
                COUNT(*) AS event_count,
                COUNT(DISTINCT user_id) AS unique_users,
                COUNT(DISTINCT session_id) AS unique_sessions
            FROM default_catalog.default_database.{source_table}
            GROUP BY
                platform,
                TUMBLE(TO_TIMESTAMP_LTZ(timestamp_ms, 3), INTERVAL '5' MINUTE)
        """
