# ==================================================================================
# USER EVENTS PROCESSED TRANSFORMER (TEMPLATE)
# ==================================================================================
# 🔖 MARKER: Create this file when adding user-activity topic
# ==================================================================================
# This is a template showing how to create a new transformer.
# Uncomment and modify when you're ready to add the user-activity topic.
# ==================================================================================

# from transformations.base_transformer import BaseTransformer
# from typing import Dict, Any


# class UserEventsProcessedTransformer(BaseTransformer):
#     """Transformer for user activity events.
#     
#     This transformer:
#     - Converts timestamp_ms to activity_time
#     - Adds ingestion_time
#     - Can add enrichment logic here
#     """
#     
#     def __init__(self, topic_config: Dict[str, Any]):
#         super().__init__(topic_config)
#     
#     def get_transformation_sql(self, source_table: str) -> str:
#         """Generate transformation SQL for user activity events.
#         
#         Args:
#             source_table: Fully qualified Kafka source table name
#             
#         Returns:
#             SQL SELECT statement
#         """
#         sql = f"""
#             SELECT
#                 user_id,
#                 activity_type,
#                 page_url,
#                 TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS activity_time,
#                 CURRENT_TIMESTAMP AS ingestion_time
#             FROM {source_table}
#         """
#         return sql
#     
#     def get_description(self) -> str:
#         return "User activity events processing with timestamp conversion"
