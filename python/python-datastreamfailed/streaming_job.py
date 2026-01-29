import sys
import os
import time
import yaml

sys.stdout = sys.stderr  # CloudWatch logs

print("=" * 80)
print("STARTING DYNAMIC MULTI-TOPIC PYFLINK JOB")
print("=" * 80)

try:
    from pyflink.table import EnvironmentSettings, TableEnvironment
    from pyflink.java_gateway import get_gateway
    print("✓ PyFlink imported")

    # JAR injection (EXACTLY like your working file)
    gateway = get_gateway()
    jvm = gateway.jvm
    jar_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib", "pyflink-dependencies.jar")
    
    if not os.path.exists(jar_file):
        print(f"WARNING: JAR not found at {jar_file}, continuing...")
    else:
        jar_url = jvm.java.net.URL(f"file://{jar_file}")
        jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)
        print(f"✓ JAR injected: {jar_file}")

    # Environment setup (EXACTLY like your working file)
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    
    config = table_env.get_config().get_configuration()
    config.set_string("table.exec.resource.default-parallelism", "1")
    config.set_string("execution.checkpointing.interval", "60s")
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    print("✓ Flink config applied")

    # DYNAMIC CONFIG FROM ENVIRONMENT (no file imports!)
    TOPICS_CONFIG = os.getenv("TOPICS_CONFIG", """
[
  {"name": "bid-events", "table": "kafka_bid_events", "sink": "bid_events_raw", "group": "flink-bid-v1"},
  {"name": "user-events", "table": "kafka_user_events", "sink": "user_events_raw", "group": "flink-user-v1"}
]
    """).strip()
    
    MSK_SERVERS = os.getenv("MSK_BOOTSTRAP_SERVERS", "b-1.rttestinganalyticsmsk.mbenee.c4.kafka.ap-south-1.amazonaws.com:9098,b-2.rttestinganalyticsmsk.mbenee.c4.kafka.ap-south-1.amazonaws.com:9098")
    S3_WAREHOUSE = os.getenv("S3_WAREHOUSE", "arn:aws:s3tables:ap-south-1:508351649560:bucket/rt-testing-cdc-bucket")
    NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "analytics")

    topics = yaml.safe_load(TOPICS_CONFIG)
    print(f"✓ Loaded {len(topics)} topics from config")

    # Iceberg catalog (EXACTLY like working file)
    print("Setting up S3 Tables catalog...")
    table_env.execute_sql(f"""
        CREATE CATALOG s3_tables WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
            'warehouse' = '{S3_WAREHOUSE}'
        )
    """)
    table_env.use_catalog("s3_tables")
    table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    table_env.use_database(NAMESPACE)
    print("✓ S3 Tables catalog ready")

    # Create Kafka sources DYNAMICALLY
    print(f"\nCreating {len(topics)} Kafka sources...")
    table_env.use_catalog("default_catalog")
    table_env.use_database("default_database")
    
    for topic in topics:
        table_name = topic["table"]
        topic_name = topic["name"]
        group_id = topic.get("group", "flink-default")
        
        print(f"  Creating: {table_name} <- {topic_name}")
        table_env.execute_sql(f"""
            CREATE TABLE {table_name} (
                event_name STRING, user_id BIGINT, city_id INT, 
                platform STRING, session_id STRING, timestamp_ms BIGINT
            ) WITH (
                'connector' = 'kafka', 'topic' = '{topic_name}',
                'properties.bootstrap.servers' = '{MSK_SERVERS}',
                'properties.group.id' = '{group_id}',
                'scan.startup.mode' = 'earliest-offset', 'format' = 'json',
                'json.fail-on-missing-field' = 'false', 'json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'AWS_MSK_IAM',
                'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
                'properties.sasl.client.callback.handler.class' = 
                    'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
            )
        """)
        print(f"  ✓ Created: {table_name}")

    # Create sinks + streaming jobs DYNAMICALLY
    table_env.use_catalog("s3_tables")
    table_env.use_database(NAMESPACE)
    
    for topic in topics:
        source_table = topic["table"]
        sink_table = topic["sink"]
        
        print(f"\nCreating sink + job: {sink_table}")
        table_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS {sink_table} (
                event_name STRING, user_id BIGINT, city_id INT,
                platform STRING, session_id STRING,
                event_time TIMESTAMP(3), ingestion_time TIMESTAMP(3)
            ) WITH (
                'format-version' = '2',
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        
        table_env.execute_sql(f"""
            INSERT INTO {sink_table}
            SELECT event_name, user_id, city_id, platform, session_id,
                   TO_TIMESTAMP_LTZ(timestamp_ms, 3), CURRENT_TIMESTAMP
            FROM default_catalog.default_database.{source_table}
        """)
        print(f"✓ Streaming job: {source_table} -> {sink_table}")

    print(f"\n{'='*80}")
    print(f"✓ ALL {len(topics)} STREAMING JOBS RUNNING SUCCESSFULLY!")
    print(f"{'='*80}")

    while True:
        time.sleep(60)
        print(f"[{time.strftime('%H:%M:%S')}] {len(topics)} jobs active...")

except Exception as e:
    print("=" * 60)
    print("CRASH DETAILS:")
    print("=" * 60)
    import traceback
    traceback.print_exc()
    print(f"ERROR: {e}")
    print("=" * 60)
    raise
