#!/usr/bin/env python3
# ==================================================================================
# MODULAR PYFLINK - BULLETPROOF LOGGING VERSION
# ==================================================================================
# Logs EVERY SINGLE STEP - Finds silent Python failures
# ==================================================================================

import sys
import os
import yaml
import time

# FORCE ALL OUTPUT TO CLOUDWATCH (CRITICAL FOR SILENT FAILURES)
sys.stdout = sys.stderr
sys.stderr = sys.stderr

print("=" * 100)
print("🚀 STARTING - BULLETPROOF LOGGING MODE")
print("📅 " + time.strftime("%Y-%m-%d %H:%M:%S"))
print("=" * 100)

# ================================================================================
# PHASE 1: ENVIRONMENT CHECKS (Log everything)
# ================================================================================
print("\n🔍 PHASE 1: CHECKING ENVIRONMENT")
print(f"✅ Python running in: {os.getcwd()}")
print(f"✅ Python version: {sys.version}")

# Check critical files
required_files = [
    "config/topics.yaml",
    "lib/pyflink-dependencies.jar"
]

print("\n📁 CHECKING REQUIRED FILES:")
for file_path in required_files:
    full_path = os.path.join(os.getcwd(), file_path)
    exists = os.path.exists(full_path)
    size = os.path.getsize(full_path) if exists else 0
    print(f"   {file_path}: {'✅' if exists else '❌'} ({size} bytes)")
    
    if not exists and file_path == "config/topics.yaml":
        print("💥 FATAL: topics.yaml missing - cannot continue")
        sys.exit(1)

print("✅ PHASE 1 COMPLETE")

# ================================================================================
# PHASE 2: PYFLINK IMPORTS (Log each import)
# ================================================================================
print("\n🔍 PHASE 2: PYFLINK IMPORTS")
try:
    print("   Importing pyflink.table...")
    from pyflink.table import EnvironmentSettings, TableEnvironment
    print("✅ pyflink.table OK")
    
    print("   Importing pyflink.java_gateway...")
    from pyflink.java_gateway import get_gateway
    print("✅ pyflink.java_gateway OK")
    
except ImportError as e:
    print(f"💥 PYFLINK IMPORT FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("✅ PHASE 2 COMPLETE")

# ================================================================================
# PHASE 3: JAR INJECTION (Verbose)
# ================================================================================
print("\n🔍 PHASE 3: JAR INJECTION")
try:
    gateway = get_gateway()
    jvm = gateway.jvm
    print("✅ JVM gateway OK")
    
    jar_path = os.path.join(os.getcwd(), "lib", "pyflink-dependencies.jar")
    print(f"🔍 JAR path: {jar_path}")
    
    if os.path.exists(jar_path):
        jar_url = jvm.java.net.URL(f"file://{jar_path}")
        jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)
        print("✅ JAR injected successfully")
    else:
        print("⚠️  JAR missing - using embedded dependencies")
        
except Exception as e:
    print(f"⚠️  JAR injection error (non-fatal): {e}")

print("✅ PHASE 3 COMPLETE")

# ================================================================================
# PHASE 4: LOAD YOUR topics.yaml (Verbose)
# ================================================================================
print("\n🔍 PHASE 4: LOADING topics.yaml")
try:
    topics_path = os.path.join(os.getcwd(), "config", "topics.yaml")
    print(f"🔍 Loading: {topics_path}")
    
    with open(topics_path, 'r') as f:
        topics_config = yaml.safe_load(f)
    
    print("✅ YAML parsed successfully")
    
    # Log global config
    kafka_global = topics_config['kafka']
    print(f"✅ Kafka servers: {kafka_global['bootstrap_servers'][:50]}...")
    
    # Log enabled topics
    enabled_topics = [name for name, config in topics_config['topics'].items() 
                     if config.get('enabled', False)]
    print(f"✅ Enabled topics: {enabled_topics}")
    
except Exception as e:
    print(f"💥 YAML LOAD FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("✅ PHASE 4 COMPLETE")

# ================================================================================
# PHASE 5: FLINK ENVIRONMENT (Verbose)
# ================================================================================
print("\n🔍 PHASE 5: FLINK TABLE ENVIRONMENT")
try:
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    print("✅ TableEnvironment created")
    
    config = table_env.get_config().get_configuration()
    config.set_string("table.exec.resource.default-parallelism", "1")
    config.set_string("execution.checkpointing.interval", "60s")
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    print("✅ Flink config applied")
    
except Exception as e:
    print(f"💥 FLINK ENV FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("✅ PHASE 5 COMPLETE")

# ================================================================================
# PHASE 6: S3 TABLES CATALOG (Your exact ARN)
# ================================================================================
print("\n🔍 PHASE 6: S3 TABLES CATALOG")
s3tables_arn = os.getenv("S3_WAREHOUSE", "arn:aws:s3tables:ap-south-1:508351649560:bucket/rt-testing-cdc-bucket")
namespace = os.getenv("ICEBERG_NAMESPACE", "analytics")

print(f"🔗 S3 Tables ARN: {s3tables_arn}")
print(f"📁 Namespace: {namespace}")

try:
    table_env.execute_sql(f"""
        CREATE CATALOG IF NOT EXISTS s3_tables WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
            'warehouse' = '{s3tables_arn}'
        )
    """)
    print("✅ Catalog created")
    
    table_env.use_catalog("s3_tables")
    table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {namespace}")
    table_env.use_database(namespace)
    print("✅ S3 Tables ready")
    
except Exception as e:
    print(f"💥 S3 TABLES FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("✅ PHASE 6 COMPLETE")

# ================================================================================
# PHASE 7: PROCESS YOUR ENABLED TOPIC (bid-events)
# ================================================================================
print("\n🔍 PHASE 7: PROCESSING TOPICS")
pipelines_created = 0

for topic_name, topic_config in topics_config['topics'].items():
    if not topic_config.get('enabled', False):
        print(f"⏭️  Skipping: {topic_name}")
        continue
        
    print(f"\n🚀 PROCESSING TOPIC: {topic_name}")
    pipelines_created += 1
    
    try:
        # Kafka source table name
        kafka_table = f"kafka_{topic_name.replace('-', '_')}"
        
        # Build source DDL
        source_schema = topic_config['source_schema']
        schema_cols = [f'"{field["name"]}" {field["type"]}' for field in source_schema]
        print(f"🔍 Source schema: {len(source_schema)} fields")
        
        # Create Kafka source
        table_env.use_catalog("default_catalog")
        table_env.use_database("default_database")
        
        kafka_ddl = f"""
            CREATE TABLE {kafka_table} (
                {', '.join(schema_cols)}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic_name}',
                'properties.bootstrap.servers' = '{kafka_global["bootstrap_servers"]}',
                'properties.group.id' = '{topic_config["kafka_group_id"]}',
                'scan.startup.mode' = '{topic_config["scan_mode"]}',
                'format' = '{topic_config["format"]}',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = '{kafka_global["security"]["protocol"]}',
                'properties.sasl.mechanism' = '{kafka_global["security"]["sasl_mechanism"]}',
                'properties.sasl.jaas.config' = '{kafka_global["security"]["sasl_jaas_config"]}',
                'properties.sasl.client.callback.handler.class' = '{kafka_global["security"]["sasl_callback_handler"]}'
            )
        """
        
        print("🔍 Creating Kafka source...")
        table_env.execute_sql(kafka_ddl)
        print(f"✅ Kafka source: {kafka_table}")
        
        # Create sink table
        table_env.use_catalog("s3_tables")
        table_env.use_database(namespace)
        sink_table = topic_config['sink']['table_name']
        sink_schema = topic_config['sink']['schema']
        sink_cols = [f'"{field["name"]}" {field["type"]}' for field in sink_schema]
        
        sink_ddl = f"""
            CREATE TABLE IF NOT EXISTS {sink_table} (
                {', '.join(sink_cols)}
            ) WITH (
                'format-version' = '2',
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """
        print("🔍 Creating sink table...")
        table_env.execute_sql(sink_ddl)
        print(f"✅ Sink table: {sink_table}")
        
        # Generate INSERT (timestamp conversion)
        source_fields = [f'"{field["name"]}"' for field in source_schema[:-1]]
        timestamp_field = source_schema
