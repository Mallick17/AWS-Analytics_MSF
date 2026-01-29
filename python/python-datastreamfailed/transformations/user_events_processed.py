from transformations.base_transformer import BaseTransformer
from typing import Dict

class UserEventsProcessedTransformer(BaseTransformer):
    def get_sink_schema(self) -> Dict[str, str]:
        return {
            "event_type": "STRING",
            "user_id": "BIGINT",
            "action": "STRING",
            "event_time": "TIMESTAMP(3)",
            "event_date": "DATE",
            "event_hour": "INT",
            "metadata": "STRING",
            "ingestion_time": "TIMESTAMP(3)"
        }
    
    def get_partition_by(self):
        return ["event_date"]
    
    def get_transformation_sql(self, source_table: str, sink_table: str) -> str:
        return f"""
            INSERT INTO {sink_table}
            SELECT
                event_type,
                user_id,
                action,
                TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS event_time,
                DATE(TO_TIMESTAMP_LTZ(timestamp_ms, 3)) AS event_date,
                HOUR(TO_TIMESTAMP_LTZ(timestamp_ms, 3)) AS event_hour,
                metadata,
                CURRENT_TIMESTAMP AS ingestion_time
            FROM default_catalog.default_database.{source_table}
        """