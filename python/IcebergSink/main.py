"""
PyFlink MSK → S3 Tables (Iceberg) - Converted from Java StreamingJob
"""
import os
import json
import pyflink
from pyflink.table import EnvironmentSettings, TableEnvironment

#######################################
# 1. Execution environment
#######################################

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Checkpointing (matches Java)
table_env.get_config().get_configuration().set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "60000")
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
    
    # Kafka config
    kafka_props = property_map(props, "KafkaSource0") or {}
    bootstrap_servers = kafka_props.get("bootstrap.servers", 
        "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
        "b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
        "b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098")
    topics_str = kafka_props.get("topics", "user_events")
    topics = [t.strip() for t in topics_str.split(",")]
    consumer_group = kafka_props.get("group.id", "flink-s3-tables-tableapi")
    
    # S3 Tables config
    s3_props = property_map(props, "IcebergTable0") or {}
    s3_warehouse = s3_props.get("warehouse.path", 
        "arn:aws:s3tables:ap-south-1:149815625933:bucket/testing-python-flink-table-bucket")
    namespace = s3_props.get("database.name", "sink")
    aws_region = s3_props.get("aws.region", "ap-south-1")
    
    print("=== Configuration ===")
    print(f"Kafka Bootstrap: {bootstrap_servers}")
    print(f"Topics: {topics}")
    print(f"S3 Warehouse: {s3_warehouse}")
    print(f"Namespace: {namespace}")
    print("====================")

    #################################################
    # FIXED: S3 Tables Catalog (exact Java equivalent)
    #################################################
    
    s3_catalog_name = "s3_tables"
    table_env.execute_sql(f"""
        CREATE CATALOG {s3_catalog_name} WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
            'warehouse' = '{s3_warehouse}',
            'aws.region' = '{aws_region}'
        )
    """)
    table_env.execute_sql(f"USE CATALOG {s3_catalog_name}")
    table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS `{namespace}`")
    table_env.execute_sql(f"USE `{namespace}`")
    print(f"✅ S3 Tables catalog '{s3_catalog_name}' created")

    #################################################
    # Process Kafka topics (matches Java loop)
    #################################################
    
    for topic in topics:
        topic = topic.strip()
        table_name = topic.replace("-", "_").replace(".", "_").lower()
        
        print(f"Processing topic: {topic} → table: {table_name}")
        
        # Kafka source (MSK IAM auth)
        table_env.execute_sql(f"""
            CREATE TABLE kafka_{table_name} (
                raw_json STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic}',
                'properties.bootstrap.servers' = '{bootstrap_servers}',
                'properties.group.id' = '{consumer_group}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'raw',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'AWS_MSK_IAM',
                'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
                'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
            )
        """)
        
        # Transform (matches Java JSON parsing)
        table_env.execute_sql(f"""
            CREATE TABLE transformed_{table_name} AS
            SELECT 
                JSON_VALUE(raw_json, '$.event_id') as event_id,
                JSON_VALUE(raw_json, '$.user_id') as user_id,
                JSON_VALUE(raw_json, '$.event_type') as event_type,
                CAST(JSON_VALUE(raw_json, '$.timestamp') AS BIGINT) as event_timestamp,
                JSON_VALUE(raw_json, '$.ride_id') as ride_id,
                CAST(JSON_VALUE(raw_json, '$.metadata.surge_multiplier') AS DOUBLE) as surge_multiplier,
                CAST(JSON_VALUE(raw_json, '$.metadata.estimated_wait_minutes') AS INT) as estimated_wait_minutes,
                CAST(JSON_VALUE(raw_json, '$.metadata.fare_amount') AS DOUBLE) as fare_amount,
                CAST(JSON_VALUE(raw_json, '$.metadata.driver_rating') AS DOUBLE) as driver_rating,
                DATE_FORMAT(FROM_UNIXTIME(CAST(JSON_VALUE(raw_json, '$.timestamp') AS BIGINT) / 1000), 'yyyy-MM-dd-HH') as event_hour
            FROM kafka_{table_name}
        """)
        
        # Iceberg sink (matches Java schema)
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
        
        # Insert stream
        table_result = table_env.execute_sql(f"""
            INSERT INTO `{table_name}`
            SELECT * FROM transformed_{table_name}
        """)
        
        print(f"✅ Pipeline: {topic} → {table_name}")

    print("🚀 All pipelines active - Press Ctrl+C to stop")
    if is_local:
        table_env.execute_sql("SELECT 1").wait()

if __name__ == "__main__":
    main()