# transformations.py
# This file can hold transformation logic, e.g., the SQL query template for INSERTs.
# For now, it's simple, but you can extend it for more complex transformations.

INSERT_QUERY_TEMPLATE = """
    INSERT INTO {table_name}
    SELECT
        event_name,
        user_id,
        city_id,
        platform,
        session_id,
        TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS event_time,
        CURRENT_TIMESTAMP AS ingestion_time
    FROM default_catalog.default_database.kafka_events
    WHERE event_name = '{event_type}'
"""