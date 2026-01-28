from transformations.base_transformer import BaseTransformer
from typing import Dict

class BidEventsAggregatedTransformer(BaseTransformer):
    def get_sink_schema(self) -> Dict[str, str]:
        return {
            "window_start": "TIMESTAMP(3)",
            "window_end": "TIMESTAMP(3)",
            "city_id": "INT",
            "platform": "STRING",
            "total_bids": "BIGINT",
            "unique_users": "BIGINT",
            "avg_bid_amount": "DOUBLE",
            "max_bid_amount": "DOUBLE",
            "min_bid_amount": "DOUBLE"
        }
    
    def get_partition_by(self):
        return ["DATE(window_start)"]
    
    def get_transformation_sql(self, source_table: str, sink_table: str) -> str:
        return f"""
            INSERT INTO {sink_table}
            SELECT
                TUMBLE_START(TO_TIMESTAMP_LTZ(timestamp_ms, 3), INTERVAL '1' HOUR) AS window_start,
                TUMBLE_END(TO_TIMESTAMP_LTZ(timestamp_ms, 3), INTERVAL '1' HOUR) AS window_end,
                city_id,
                platform,
                COUNT(*) AS total_bids,
                COUNT(DISTINCT user_id) AS unique_users,
                AVG(bid_amount) AS avg_bid_amount,
                MAX(bid_amount) AS max_bid_amount,
                MIN(bid_amount) AS min_bid_amount
            FROM default_catalog.default_database.{source_table}
            GROUP BY
                TUMBLE(TO_TIMESTAMP_LTZ(timestamp_ms, 3), INTERVAL '1' HOUR),
                city_id,
                platform
        """