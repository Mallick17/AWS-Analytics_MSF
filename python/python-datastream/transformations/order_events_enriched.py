from transformations.base_transformer import BaseTransformer
from typing import Dict

class OrderEventsEnrichedTransformer(BaseTransformer):
    def get_sink_schema(self) -> Dict[str, str]:
        return {
            "order_id": "STRING",
            "user_id": "BIGINT",
            "product_id": "STRING",
            "quantity": "INT",
            "price": "DOUBLE",
            "total_amount": "DOUBLE",
            "order_time": "TIMESTAMP(3)",
            "order_date": "DATE",
            "price_category": "STRING",
            "ingestion_time": "TIMESTAMP(3)"
        }
    
    def get_partition_by(self):
        return ["order_date"]
    
    def get_transformation_sql(self, source_table: str, sink_table: str) -> str:
        return f"""
            INSERT INTO {sink_table}
            SELECT
                order_id,
                user_id,
                product_id,
                quantity,
                price,
                quantity * price AS total_amount,
                TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS order_time,
                DATE(TO_TIMESTAMP_LTZ(timestamp_ms, 3)) AS order_date,
                CASE
                    WHEN price < 100 THEN 'LOW'
                    WHEN price >= 100 AND price < 500 THEN 'MEDIUM'
                    ELSE 'HIGH'
                END AS price_category,
                CURRENT_TIMESTAMP AS ingestion_time
            FROM default_catalog.default_database.{source_table}
        """