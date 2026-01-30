#!/usr/bin/env python3
# ==================================================================================
# MODULAR PYFLINK STREAMING JOB - COMPLETE WORKING VERSION
# ==================================================================================

import sys
import os
import yaml
import time

# FORCE ALL OUTPUT TO CLOUDWATCH
sys.stdout = sys.stderr

print("=" * 100)
print("STARTING - MODULAR PYFLINK STREAMING JOB")
print("Time: " + time.strftime("%Y-%m-%d %H:%M:%S"))
print("=" * 100)

# ================================================================================
# CRITICAL: Get the script's directory, not the current working directory
# ================================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
print(f"\nScript Directory: {SCRIPT_DIR}")
print(f"Current Working Directory: {os.getcwd()}")

# Add script directory to Python path so modules can be imported
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
    print(f"Added {SCRIPT_DIR} to sys.path")

print("-" * 100)

# ================================================================================
# List files from SCRIPT_DIR (where your files actually are)
# ================================================================================
print("\nDirectory Structure and Files (from SCRIPT_DIR):")
print("-" * 100)

for root, dirs, files in os.walk(SCRIPT_DIR):
    level = root.replace(SCRIPT_DIR, '').count(os.sep)
    indent = ' ' * 2 * level
    print(f'{indent}{os.path.basename(root)}/')
    
    sub_indent = ' ' * 2 * (level + 1)
    for file in files:
        file_path = os.path.join(root, file)
        try:
            file_size = os.path.getsize(file_path)
            print(f'{sub_indent}{file} ({file_size} bytes)')
        except:
            print(f'{sub_indent}{file}')

print("=" * 100)

# ================================================================================
# PHASE 1: ENVIRONMENT CHECKS - USING SCRIPT_DIR
# ================================================================================
print("\n[PHASE 1] CHECKING ENVIRONMENT")
print(f"  Python running in: {os.getcwd()}")
print(f"  Script located in: {SCRIPT_DIR}")
print(f"  Python version: {sys.version}")

# Use SCRIPT_DIR for all file paths
required_files = [
    ("config/topics.yaml", os.path.join(SCRIPT_DIR, "config", "topics.yaml")),
    ("lib/pyflink-dependencies.jar", os.path.join(SCRIPT_DIR, "lib", "pyflink-dependencies.jar"))
]

print("\n  CHECKING REQUIRED FILES:")
all_files_exist = True

for display_name, file_path in required_files:
    exists = os.path.exists(file_path)
    size = os.path.getsize(file_path) if exists else 0
    print(f"    {display_name}: {'OK' if exists else 'MISSING'} ({size} bytes)")
    print(f"      Path: {file_path}")
    
    if not exists:
        all_files_exist = False
        if display_name == "config/topics.yaml":
            print("      ERROR: topics.yaml missing - cannot continue")
        elif display_name == "lib/pyflink-dependencies.jar":
            print("      ERROR: pyflink-dependencies.jar missing - cannot inject dependencies")

if not all_files_exist:
    print("\nFATAL: Required files missing!")
    sys.exit(1)

print("[PHASE 1] COMPLETE")

# ================================================================================
# PHASE 2: PYFLINK IMPORTS
# ================================================================================
print("\n[PHASE 2] PYFLINK IMPORTS")
try:
    print("  Importing pyflink.table...")
    from pyflink.table import EnvironmentSettings, TableEnvironment
    print("  pyflink.table OK")
    
    print("  Importing pyflink.java_gateway...")
    from pyflink.java_gateway import get_gateway
    print("  pyflink.java_gateway OK")
    
except ImportError as e:
    print(f"FATAL: PYFLINK IMPORT FAILED: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

print("[PHASE 2] COMPLETE")

# ================================================================================
# PHASE 3: JAR INJECTION - USING SCRIPT_DIR
# ================================================================================
print("\n[PHASE 3] JAR INJECTION")
try:
    gateway = get_gateway()
    jvm = gateway.jvm
    print("  JVM gateway OK")
    
    jar_path = os.path.join(SCRIPT_DIR, "lib", "pyflink-dependencies.jar")
    print(f"  JAR path: {jar_path}")
    
    if os.path.exists(jar_path):
        jar_url = jvm.java.net.URL(f"file://{jar_path}")
        jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)
        print("  JAR injected successfully")
    else:
        print("FATAL: Dependency JAR missing - cannot continue")
        sys.exit(1)
        
except Exception as e:
    print(f"FATAL: JAR INJECTION FAILED: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

print("[PHASE 3] COMPLETE")

# ================================================================================
# PHASE 4: LOAD topics.yaml - USING SCRIPT_DIR
# ================================================================================
print("\n[PHASE 4] LOADING topics.yaml")
try:
    topics_path = os.path.join(SCRIPT_DIR, "config", "topics.yaml")
    print(f"  Loading: {topics_path}")
    
    with open(topics_path, 'r') as f:
        topics_config = yaml.safe_load(f)
    
    print("  YAML parsed successfully")
    
    kafka_global = topics_config.get('kafka', {})
    print(f"  Kafka servers: {kafka_global.get('bootstrap_servers', 'MISSING')[:50]}...")
    
    enabled_topics = [name for name, config in topics_config.get('topics', {}).items() 
                      if config.get('enabled', False)]
    print(f"  Enabled topics: {enabled_topics}")
    
    if not enabled_topics:
        print("WARNING: No enabled topics found!")
    
except Exception as e:
    print(f"FATAL: YAML LOAD FAILED: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

print("[PHASE 4] COMPLETE")

# ================================================================================
# PHASE 5: FLINK TABLE ENVIRONMENT
# ================================================================================
print("\n[PHASE 5] FLINK TABLE ENVIRONMENT")
try:
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    print("  TableEnvironment created")
    
    config = table_env.get_config().get_configuration()
    config.set_string("table.exec.resource.default-parallelism", "1")
    config.set_string("execution.checkpointing.interval", "60s")
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    print("  Flink config applied")
    
except Exception as e:
    print(f"FATAL: FLINK ENV FAILED: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

print("[PHASE 5] COMPLETE")

# ================================================================================
# PHASE 6: S3 TABLES CATALOG
# ================================================================================
print("\n[PHASE 6] S3 TABLES CATALOG")
s3tables_arn = os.getenv("S3_WAREHOUSE", "arn:aws:s3tables:ap-south-1:508351649560:bucket/rt-testing-cdc-bucket")
namespace = os.getenv("ICEBERG_NAMESPACE", "analytics")

print(f"  S3 Tables ARN: {s3tables_arn}")
print(f"  Namespace: {namespace}")

# Step 1: Create Catalog (without IF NOT EXISTS)
try:
    print("  Creating S3 Tables catalog...")
    table_env.execute_sql(f"""
        CREATE CATALOG s3_tables WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
            'warehouse' = '{s3tables_arn}'
        )
    """)
    print("  Catalog created successfully")
except Exception as e:
    error_msg = str(e).lower()
    if "already exists" in error_msg or "catalog s3_tables exists" in error_msg:
        print("  Catalog already exists (OK)")
    else:
        print(f"  Warning during catalog creation: {e}")
        # Continue anyway - catalog might exist

# Step 2: Use Catalog
try:
    table_env.use_catalog("s3_tables")
    print("  Switched to s3_tables catalog")
except Exception as e:
    print(f"FATAL: Cannot use catalog s3_tables: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

# Step 3: Create Database (without IF NOT EXISTS)
try:
    print(f"  Creating database {namespace}...")
    table_env.execute_sql(f"CREATE DATABASE {namespace}")
    print(f"  Database {namespace} created successfully")
except Exception as e:
    error_msg = str(e).lower()
    if "already exists" in error_msg or f"database {namespace} exists" in error_msg:
        print(f"  Database {namespace} already exists (OK)")
    else:
        print(f"  Warning during database creation: {e}")
        # Continue anyway - database might exist

# Step 4: Use Database
try:
    table_env.use_database(namespace)
    print(f"  Switched to database {namespace}")
    print("  S3 Tables catalog ready")
except Exception as e:
    print(f"FATAL: Cannot use database {namespace}: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

print("[PHASE 6] COMPLETE")

# ================================================================================
# PHASE 7: PROCESS ENABLED TOPICS - CREATE PIPELINES
# ================================================================================
print("\n[PHASE 7] PROCESSING TOPICS")
pipelines_created = 0
table_results = []

kafka_global = topics_config.get('kafka', {})

for topic_name, topic_config in topics_config.get('topics', {}).items():
    if not topic_config.get('enabled', False):
        print(f"  Skipping disabled topic: {topic_name}")
        continue
        
    print(f"\n  PROCESSING TOPIC: {topic_name}")
    
    try:
        # Step 1: Create Kafka source table
        kafka_table = f"kafka_{topic_name.replace('-', '_')}"
        
        source_schema = topic_config.get('source_schema', [])
        schema_cols = [f"`{field['name']}` {field['type']}" for field in source_schema]
        print(f"    Source schema: {len(source_schema)} fields")
        
        # Switch to default catalog for Kafka table
        table_env.use_catalog("default_catalog")
        table_env.use_database("default_database")
        
        kafka_ddl = f"""
            CREATE TABLE {kafka_table} (
                {', '.join(schema_cols)}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic_name}',
                'properties.bootstrap.servers' = '{kafka_global.get("bootstrap_servers", "MISSING")}',
                'properties.group.id' = '{topic_config.get("kafka_group_id", "flink-default")}',
                'scan.startup.mode' = '{topic_config.get("scan_mode", "latest-offset")}',
                'format' = '{topic_config.get("format", "json")}',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = '{kafka_global.get("security", {}).get("protocol", "SASL_SSL")}',
                'properties.sasl.mechanism' = '{kafka_global.get("security", {}).get("sasl_mechanism", "AWS_MSK_IAM")}',
                'properties.sasl.jaas.config' = '{kafka_global.get("security", {}).get("sasl_jaas_config", "software.amazon.msk.auth.iam.IAMLoginModule required;")}',
                'properties.sasl.client.callback.handler.class' = '{kafka_global.get("security", {}).get("sasl_callback_handler", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")}'
            )
        """
        
        print("    Creating Kafka source table...")
        try:
            table_env.execute_sql(kafka_ddl)
            print(f"    Kafka source created: {kafka_table}")
        except Exception as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg or f"table {kafka_table} exists" in error_msg:
                print(f"    Kafka source already exists: {kafka_table}")
            else:
                raise
        
        # Step 2: Create Iceberg sink table
        table_env.use_catalog("s3_tables")
        table_env.use_database(namespace)
        
        sink_table = topic_config.get('sink', {}).get('table_name', f"{topic_name}_sink")
        sink_schema = topic_config.get('sink', {}).get('schema', [])
        sink_cols = [f"`{field['name']}` {field['type']}" for field in sink_schema]
        
        sink_ddl = f"""
            CREATE TABLE {sink_table} (
                {', '.join(sink_cols)}
            ) WITH (
                'format-version' = '2',
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """
        
        print("    Creating Iceberg sink table...")
        try:
            table_env.execute_sql(sink_ddl)
            print(f"    Sink table created: {sink_table}")
        except Exception as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg or f"table {sink_table} exists" in error_msg:
                print(f"    Sink table already exists: {sink_table}")
            else:
                raise
        
        # Step 3: Build INSERT statement with field mapping
        source_field_names = [field["name"] for field in source_schema]
        
        select_parts = []
        for sink_field in sink_schema:
            sink_name = sink_field["name"]
            sink_type = sink_field["type"]
            
            # Special handling for timestamp conversion
            if sink_name == "event_time" and "timestamp_ms" in source_field_names:
                select_parts.append(f"TO_TIMESTAMP_LTZ(`timestamp_ms`, 3) AS `event_time`")
            # Special handling for ingestion timestamp
            elif sink_name == "ingestion_time":
                select_parts.append(f"CURRENT_TIMESTAMP AS `ingestion_time`")
            # Direct field mapping
            elif sink_name in source_field_names:
                select_parts.append(f"`{sink_name}`")
            # Field not in source - use NULL
            else:
                print(f"    WARNING: Sink field '{sink_name}' not in source, using NULL")
                select_parts.append(f"CAST(NULL AS {sink_type}) AS `{sink_name}`")
        
        select_clause = ",\n                ".join(select_parts)
        
        insert_sql = f"""
            INSERT INTO `s3_tables`.`{namespace}`.`{sink_table}`
            SELECT
                {select_clause}
            FROM `default_catalog`.`default_database`.`{kafka_table}`
        """
        
        print("    INSERT SQL Statement:")
        for line in insert_sql.strip().split('\n'):
            print(f"      {line.strip()}")
        
        # Step 4: Execute the streaming pipeline
        print(f"    Executing streaming pipeline for topic: {topic_name}...")
        result = table_env.execute_sql(insert_sql)
        table_results.append((topic_name, result))
        
        pipelines_created += 1
        print(f"    ✓ Pipeline STARTED for: {topic_name}")
        
    except Exception as e:
        print(f"    ✗ ERROR processing topic {topic_name}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        print(f"    Continuing to next topic...")
        continue  # Continue processing other topics

print(f"\n[PHASE 7] COMPLETE - {pipelines_created} pipeline(s) created")

# ================================================================================
# PHASE 8: JOB STATUS AND WAITING
# ================================================================================
print("\n[PHASE 8] JOB STATUS")

if pipelines_created == 0:
    print("  ✗ WARNING: No pipelines were created!")
    print("  Check topic configurations:")
    print("    1. Ensure at least one topic has 'enabled: true'")
    print("    2. Verify source and sink schemas are correct")
    print("    3. Check logs above for errors")
    sys.exit(1)

# Success - pipelines are running
print(f"  ✓ SUCCESS: {pipelines_created} streaming pipeline(s) started")
print("\n  Active Pipelines:")
for topic_name, result in table_results:
    print(f"    • {topic_name} → RUNNING")

print("\n  Job Lifecycle:")
print("    • Pipelines will process data continuously")
print("    • AWS Managed Flink handles failure recovery")
print("    • Data flows: Kafka → Flink → S3 Tables (Iceberg)")

# Wait for the first pipeline to complete (it never will - streaming job)
if table_results:
    print("\n  Entering streaming mode - job will run indefinitely...")
    print("  To stop: Stop the Kinesis Data Analytics application")
    print("-" * 100)
    
    try:
        # Wait on first result - this blocks forever for streaming jobs
        first_topic, first_result = table_results[0]
        print(f"  Monitoring pipeline: {first_topic}")
        first_result.wait()
    except KeyboardInterrupt:
        print("\n  Job interrupted by user")
    except Exception as e:
        print(f"\n  Pipeline execution ended: {e}")
        import traceback
        traceback.print_exc(file=sys.stderr)

print("\n" + "=" * 100)
print("MODULAR PYFLINK JOB EXECUTION COMPLETE")
print("Time: " + time.strftime("%Y-%m-%d %H:%M:%S"))
print("=" * 100)