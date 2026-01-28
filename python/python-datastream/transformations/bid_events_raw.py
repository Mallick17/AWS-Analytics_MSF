from transformations.base_transformer import BaseTransformer
from typing import Dict

class BidEventsRawTransformer(BaseTransformer):
    def get_sink_schema(self) -> Dict[str, str]:
        return {
            "event_name": "STRING",
            "user_id": "BIGINT",
            "city_id": "INT",
            "platform": "STRING",
            "session_id": "STRING",
            "bid_amount": "DOUBLE",
            "event_time": "TIMESTAMP(3)",
            "ingestion_time": "TIMESTAMP(3)"
        }
    
    def get_partition_by(self):
        return ["DATE(event_time)"]
    
    def get_transformation_sql(self, source_table: str, sink_table: str) -> str:
        return f"""
            INSERT INTO {sink_table}
            SELECT
                event_name,
                user_id,
                city_id,
                platform,
                session_id,
                bid_amount,
                TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS event_time,
                CURRENT_TIMESTAMP AS ingestion_time
            FROM default_catalog.default_database.{source_table}
        """