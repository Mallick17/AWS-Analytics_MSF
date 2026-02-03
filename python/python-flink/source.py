# source.py (UPDATED: dedent for SQL)
import textwrap
from utils import log_message

def create_kafka_source(table_env, msk_bootstrap_servers, kafka_topic):
    """Create the Kafka source table."""
    table_env.use_catalog("default_catalog")
    table_env.use_database("default_database")
    log_message("Creating Kafka source with JSON format")
    
    sql = textwrap.dedent(f"""
        CREATE TABLE kafka_events (
            event_name STRING,
            user_id BIGINT,
            city_id INT,
            platform STRING,
            session_id STRING,
            timestamp_ms BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{kafka_topic}',
            'properties.bootstrap.servers' = '{msk_bootstrap_servers}',
            'properties.group.id' = 'flink-s3tables-json-v1',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'AWS_MSK_IAM',
            'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
            'properties.sasl.client.callback.handler.class' = 
                'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
        )
    """)
    table_env.execute_sql(sql)
    log_message("✓ Kafka source created")