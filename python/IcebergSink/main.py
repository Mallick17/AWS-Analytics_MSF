"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
PyFlink equivalent of Java StreamingJob - MSK → S3 Tables (Iceberg)
"""
import os
import json
import pyflink
from pyflink.table import EnvironmentSettings, TableEnvironment

#######################################
# 1. Execution environment setup
#######################################

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Checkpointing (matches Java)
table_env.get_config().get_configuration().set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "60000")  # 1 min
table_env.get_config().get_configuration().set_string("execution.checkpointing.min-pause", "30000")
table_env.get_config().get_configuration().set_string("execution.checkpointing.timeout", "600000")
table_env.get_config().get_configuration().set_string("execution.checkpointing.max-concurrent-checkpoints", "1")

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"
is_local = True if os.environ.get("IS_LOCAL") else False

if is_local:
    APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars", f"file:///{CURRENT_DIR}/target/pyflink-dependencies.jar"
    )
    print("PyFlink home:", os.path.dirname(os.path.abspath(pyflink.__file__)))
    print("Logging directory:", os.path.dirname(os.path.abspath(pyflink.__file__)) + '/log')

def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            return json.loads(file.read())
    print(f'Config file "{APPLICATION_PROPERTIES_FILE_PATH}" not found')
    return []

def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]
    return {}

def main():
    props = get_application_properties()
    
    # Kafka config (from your Java app)
    kafka_props = property_map(props, "KafkaSource0") or {}
    bootstrap_servers = kafka_props.get("bootstrap.servers", 
        "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
        "b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
        "b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098")
    topics_str = kafka_props.get("topics", "user_events")
    topics = [t.strip() for t in topics_str.split(",")]
    consumer_group = kafka_props.get("group.id", "flink-s3-tables-tableapi")
    
    # S3 Tables config (from your Java app)
    s3_props = property_map(props, "IcebergTable0") or {}
    s3_warehouse = s3_props.get("warehouse.path", 
        "arn:aws:s3tables:ap-south-1:149815625933:bucket/flink-transform-sink")
    namespace = s3_props.get("database.name", "sink")
    aws_region = s3_props.get("aws.region", "ap-south-1")
    
    print("=== Configuration ===")
    print(f"Kafka Bootstrap: {bootstrap_servers}")
    print(f"Topics: {topics}")
    print(f"Consumer Group: {consumer_group}")
    print(f"S3 Warehouse: {s3_warehouse}")
    print(f"Namespace: {namespace}")
    print("====================")

    #################################################
    # 1. Create S3 Tables Catalog (matches Java)
    #################################################
    
    s3_catalog_name = "s3_tables_catalog"
    table_env.execute_sql(f"""
        CREATE CATALOG {s3_catalog_name} WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
            'warehouse' = '{s3_warehouse}',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            'aws.region' = '{aws_region}'
        )
    """)
    table_env.execute_sql(f"USE CATALOG {s3_catalog_name}")
    table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS `{namespace}`")
    table_env.execute_sql(f"USE `{namespace}`")

    #################################################
    # 2. Process each Kafka topic (matches Java loop)
    #################################################
    
    for topic in topics:
        topic = topic.strip()
        table_name = topic.replace("-", "_").replace(".", "_").lower()
        
        print(f"Processing topic: {topic} -> table: {namespace}.{table_name}")
        
        # Kafka source table (matches Java KafkaSource.builder())
        table_env.execute_sql(f"""
            CREATE TABLE kafka_{table_name} (
                event_id STRING,
                user_id STRING,
                event_type STRING,
                event_timestamp BIGINT,
                ride_id STRING,
                surge_multiplier DOUBLE,
                estimated_wait_minutes INT,
                fare_amount DOUBLE,
                driver_rating DOUBLE,
                metadata_json STRING,
                WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECONDS
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic}',
                'properties.bootstrap.servers' = '{bootstrap_servers}',
                'properties.group.id' = '{consumer_group}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'AWS_MSK_IAM',
                'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
                'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
            )
        """)
        
        # Transform table (matches Java JSON parsing + metadata extraction)
        table_env.execute_sql(f"""
            CREATE TABLE transformed_{table_name} AS
            SELECT 
                JSON_VALUE(metadata_json, '$.surge_multiplier') as surge_multiplier,
                JSON_VALUE(metadata_json, '$.estimated_wait_minutes') as estimated_wait_minutes,
                JSON_VALUE(metadata_json, '$.fare_amount') as fare_amount,
                JSON_VALUE(metadata_json, '$.driver_rating') as driver_rating,
                event_id,
                user_id,
                event_type,
                event_timestamp,
                ride_id,
                DATE_FORMAT(FROM_UNIXTIME(event_timestamp / 1000), 'yyyy-MM-dd-HH') as event_hour
            FROM kafka_{table_name}
        """)
        
        # Iceberg sink table (matches Java table creation)
        table_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                event_id STRING,
                user_id STRING,
                event_type STRING,
                event_timestamp BIGINT,
                ride_id STRING,
                surge_multiplier DOUBLE,
                estimated_wait_minutes INT,
                fare_amount DOUBLE,
                driver_rating DOUBLE,
                event_hour STRING
            ) PARTITIONED BY (event_hour)
            WITH (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy',
                'format-version' = '2',
                'write.upsert.enabled' = 'true'
            )
        """)
        
        # Insert with upsert (matches Java FlinkSink.upsert(true))
        table_env.execute_sql(f"""
            INSERT INTO `{table_name}`
            SELECT 
                event_id, user_id, event_type, event_timestamp, ride_id,
                CAST(surge_multiplier AS DOUBLE), 
                CAST(estimated_wait_minutes AS INT),
                CAST(fare_amount AS DOUBLE),
                CAST(driver_rating AS DOUBLE),
                event_hour
            FROM transformed_{table_name}
        """)
        
        print(f"✅ Pipeline active: {topic} → {table_name}")

    if is_local:
        print("🚀 Local mode - Press Ctrl+C to stop")
        table_env.execute_sql("SELECT 1").wait()  # Keep running

if __name__ == "__main__":
    main()