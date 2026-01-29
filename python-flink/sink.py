# sink.py
from utils import log_message
from transformations import INSERT_QUERY_TEMPLATE

def create_iceberg_sinks(table_env, event_types):
    """Create separate Iceberg sink tables for each event type."""
    table_env.use_catalog("s3_tables")
    # Assuming namespace is already set from create_iceberg_catalog
    
    for event_type in event_types:
        table_name = f"{event_type}_events"
        log_message(f"Creating Iceberg sink table: {table_name}")
        table_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_name STRING,
                user_id BIGINT,
                city_id INT,
                platform STRING,
                session_id STRING,
                event_time TIMESTAMP(3),
                ingestion_time TIMESTAMP(3)
            ) PARTITIONED BY (days(event_time))
            WITH (
                'format-version' = '2',
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        log_message(f"✓ Iceberg sink {table_name} created")

def submit_insert_jobs(table_env, event_types):
    """Submit streaming INSERT statements for each event type using transformation template."""
    log_message("Submitting streaming INSERTs with JSON parsing and filtering")
    
    for event_type in event_types:
        table_name = f"{event_type}_events"
        insert_query = INSERT_QUERY_TEMPLATE.format(
            table_name=table_name,
            event_type=event_type
        )
        table_env.execute_sql(insert_query)
        log_message(f"✓ INSERT submitted for {event_type} into {table_name}")
    
    log_message("✓ All jobs submitted successfully (JSON format with split sinks)")