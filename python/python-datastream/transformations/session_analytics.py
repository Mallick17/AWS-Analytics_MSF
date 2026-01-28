from transformations.base_transformer import BaseTransformer
from typing import Dict

class SessionAnalyticsTransformer(BaseTransformer):
    def get_sink_schema(self) -> Dict[str, str]:
        return {
            "session_id": "STRING",
            "user_id": "BIGINT",
            "session_start": "TIMESTAMP(3)",
            "total_bids": "BIGINT",
            "total_events": "BIGINT",
            "platforms_used": "STRING",
            "cities_accessed": "STRING"
        }
    
    def get_partition_by(self):
        return ["DATE(session_start)"]
    
    def get_transformation_sql(self, source_table: str, sink_table: str) -> str:
        # This is a simplified version - you might need temporal joins
        return f"""
            INSERT INTO {sink_table}
            SELECT
                session_id,
                user_id,
                MIN(TO_TIMESTAMP_LTZ(timestamp_ms, 3)) AS session_start,
                COUNT(*) AS total_bids,
                0 AS total_events,
                LISTAGG(DISTINCT platform, ',') AS platforms_used,
                LISTAGG(DISTINCT CAST(city_id AS STRING), ',') AS cities_accessed
            FROM default_catalog.default_database.{source_table}
            GROUP BY session_id, user_id
        """