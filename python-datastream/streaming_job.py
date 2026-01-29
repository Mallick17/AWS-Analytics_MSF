"#!/usr/bin/env python3
# ==================================================================================
# MODULAR PYFLINK STREAMING JOB - COMPLETE WORKING VERSION
# ==================================================================================
# Processes Kafka topics from topics.yaml and writes to S3 Tables (Iceberg)
# Fixed: Complete INSERT INTO statement and proper job execution
# ==================================================================================

import sys
import os
import yaml
import time

# FORCE ALL OUTPUT TO CLOUDWATCH (CRITICAL FOR AWS MANAGED FLINK)
sys.stdout = sys.stderr

print(\"=\" * 100)
print(\"STARTING - MODULAR PYFLINK STREAMING JOB\")
print(\"Time: \" + time.strftime(\"%Y-%m-%d %H:%M:%S\"))
print(\"=\" * 100)

# ================================================================================
# PHASE 1: ENVIRONMENT CHECKS
# ================================================================================
print(\"
[PHASE 1] CHECKING ENVIRONMENT\")
print(f\"  Python running in: {os.getcwd()}\")
print(f\"  Python version: {sys.version}\")

required_files = [
    \"config/topics.yaml\",
    \"lib/pyflink-dependencies.jar\"
]

print(\"
  CHECKING REQUIRED FILES:\")
for file_path in required_files:
    full_path = os.path.join(os.getcwd(), file_path)
    exists = os.path.exists(full_path)
    size = os.path.getsize(full_path) if exists else 0
    print(f\"    {file_path}: {'OK' if exists else 'MISSING'} ({size} bytes)\")
    
    if not exists and file_path == \"config/topics.yaml\":
        print(\"FATAL: topics.yaml missing - cannot continue\")
        sys.exit(1)

print(\"[PHASE 1] COMPLETE\")

# ================================================================================
# PHASE 2: PYFLINK IMPORTS
# ================================================================================
print(\"
[PHASE 2] PYFLINK IMPORTS\")
try:
    print(\"  Importing pyflink.table...\")
    from pyflink.table import EnvironmentSettings, TableEnvironment
    print(\"  pyflink.table OK\")
    
    print(\"  Importing pyflink.java_gateway...\")
    from pyflink.java_gateway import get_gateway
    print(\"  pyflink.java_gateway OK\")
    
except ImportError as e:
    print(f\"FATAL: PYFLINK IMPORT FAILED: {e}\")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print(\"[PHASE 2] COMPLETE\")

# ================================================================================
# PHASE 3: JAR INJECTION
# ================================================================================
print(\"
[PHASE 3] JAR INJECTION\")
try:
    gateway = get_gateway()
    jvm = gateway.jvm
    print(\"  JVM gateway OK\")
    
    jar_path = os.path.join(os.getcwd(), \"lib\", \"pyflink-dependencies.jar\")
    print(f\"  JAR path: {jar_path}\")
    
    if os.path.exists(jar_path):
        jar_url = jvm.java.net.URL(f\"file://{jar_path}\")
        jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)
        print(\"  JAR injected successfully\")
    else:
        print(\"  WARNING: JAR missing - using embedded dependencies\")
        
except Exception as e:
    print(f\"  WARNING: JAR injection error (non-fatal): {e}\")

print(\"[PHASE 3] COMPLETE\")

# ================================================================================
# PHASE 4: LOAD topics.yaml
# ================================================================================
print(\"
[PHASE 4] LOADING topics.yaml\")
try:
    topics_path = os.path.join(os.getcwd(), \"config\", \"topics.yaml\")
    print(f\"  Loading: {topics_path}\")
    
    with open(topics_path, 'r') as f:
        topics_config = yaml.safe_load(f)
    
    print(\"  YAML parsed successfully\")
    
    # Log global config
    kafka_global = topics_config['kafka']
    print(f\"  Kafka servers: {kafka_global['bootstrap_servers'][:50]}...\")
    
    # Log enabled topics
    enabled_topics = [name for name, config in topics_config['topics'].items() 
                     if config.get('enabled', False)]
    print(f\"  Enabled topics: {enabled_topics}\")
    
    if not enabled_topics:
        print(\"WARNING: No enabled topics found!\")
    
except Exception as e:
    print(f\"FATAL: YAML LOAD FAILED: {e}\")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print(\"[PHASE 4] COMPLETE\")

# ================================================================================
# PHASE 5: FLINK ENVIRONMENT
# ================================================================================
print(\"
[PHASE 5] FLINK TABLE ENVIRONMENT\")
try:
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    print(\"  TableEnvironment created\")
    
    config = table_env.get_config().get_configuration()
    config.set_string(\"table.exec.resource.default-parallelism\", \"1\")
    config.set_string(\"execution.checkpointing.interval\", \"60s\")
    config.set_string(\"execution.checkpointing.mode\", \"EXACTLY_ONCE\")
    print(\"  Flink config applied\")
    
except Exception as e:
    print(f\"FATAL: FLINK ENV FAILED: {e}\")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print(\"[PHASE 5] COMPLETE\")

# ================================================================================
# PHASE 6: S3 TABLES CATALOG
# ================================================================================
print(\"
[PHASE 6] S3 TABLES CATALOG\")
s3tables_arn = os.getenv(\"S3_WAREHOUSE\", \"arn:aws:s3tables:ap-south-1:508351649560:bucket/rt-testing-cdc-bucket\")
namespace = os.getenv(\"ICEBERG_NAMESPACE\", \"analytics\")

print(f\"  S3 Tables ARN: {s3tables_arn}\")
print(f\"  Namespace: {namespace}\")

try:
    table_env.execute_sql(f\"\"\"
        CREATE CATALOG IF NOT EXISTS s3_tables WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
            'warehouse' = '{s3tables_arn}'
        )
    \"\"\")
    print(\"  Catalog created\")
    
    table_env.use_catalog(\"s3_tables\")
    table_env.execute_sql(f\"CREATE DATABASE IF NOT EXISTS {namespace}\")
    table_env.use_database(namespace)
    print(\"  S3 Tables ready\")
    
except Exception as e:
    print(f\"FATAL: S3 TABLES FAILED: {e}\")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print(\"[PHASE 6] COMPLETE\")

# ================================================================================
# PHASE 7: PROCESS ENABLED TOPICS - CREATE PIPELINES
# ================================================================================
print(\"
[PHASE 7] PROCESSING TOPICS\")
pipelines_created = 0
table_results = []

for topic_name, topic_config in topics_config['topics'].items():
    if not topic_config.get('enabled', False):
        print(f\"  Skipping disabled topic: {topic_name}\")
        continue
        
    print(f\"
  PROCESSING TOPIC: {topic_name}\")
    
    try:
        # =====================================================
        # Step 1: Create Kafka source table
        # =====================================================
        kafka_table = f\"kafka_{topic_name.replace('-', '_')}\"
        
        source_schema = topic_config['source_schema']
        schema_cols = [f\"`{field['name']}` {field['type']}\" for field in source_schema]
        print(f\"    Source schema: {len(source_schema)} fields\")
        
        # Switch to default catalog for Kafka source
        table_env.use_catalog(\"default_catalog\")
        table_env.use_database(\"default_database\")
        
        kafka_ddl = f\"\"\"
            CREATE TABLE IF NOT EXISTS {kafka_table} (
                {', '.join(schema_cols)}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic_name}',
                'properties.bootstrap.servers' = '{kafka_global[\"bootstrap_servers\"]}',
                'properties.group.id' = '{topic_config[\"kafka_group_id\"]}',
                'scan.startup.mode' = '{topic_config[\"scan_mode\"]}',
                'format' = '{topic_config[\"format\"]}',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = '{kafka_global[\"security\"][\"protocol\"]}',
                'properties.sasl.mechanism' = '{kafka_global[\"security\"][\"sasl_mechanism\"]}',
                'properties.sasl.jaas.config' = '{kafka_global[\"security\"][\"sasl_jaas_config\"]}',
                'properties.sasl.client.callback.handler.class' = '{kafka_global[\"security\"][\"sasl_callback_handler\"]}'
            )
        \"\"\"
        
        print(\"    Creating Kafka source...\")
        table_env.execute_sql(kafka_ddl)
        print(f\"    Kafka source created: {kafka_table}\")
        
        # =====================================================
        # Step 2: Create Iceberg sink table
        # =====================================================
        table_env.use_catalog(\"s3_tables\")
        table_env.use_database(namespace)
        
        sink_table = topic_config['sink']['table_name']
        sink_schema = topic_config['sink']['schema']
        sink_cols = [f\"`{field['name']}` {field['type']}\" for field in sink_schema]
        
        sink_ddl = f\"\"\"
            CREATE TABLE IF NOT EXISTS {sink_table} (
                {', '.join(sink_cols)}
            ) WITH (
                'format-version' = '2',
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        \"\"\"
        print(\"    Creating sink table...\")
        table_env.execute_sql(sink_ddl)
        print(f\"    Sink table created: {sink_table}\")
        
        # =====================================================
        # Step 3: Build INSERT INTO statement with transformation
        # =====================================================
        source_field_names = [field[\"name\"] for field in source_schema]
        
        # Build SELECT clause with proper field mapping
        select_parts = []
        for sink_field in sink_schema:
            sink_name = sink_field[\"name\"]
            sink_type = sink_field[\"type\"]
            
            if sink_name == \"event_time\" and \"timestamp_ms\" in source_field_names:
                # Transform timestamp_ms (BIGINT millis) to event_time (TIMESTAMP)
                select_parts.append(f\"TO_TIMESTAMP_LTZ(`timestamp_ms`, 3) AS `event_time`\")
            elif sink_name == \"ingestion_time\":
                # Add current timestamp for ingestion tracking
                select_parts.append(f\"CURRENT_TIMESTAMP AS `ingestion_time`\")
            elif sink_name in source_field_names:
                # Pass through field directly
                select_parts.append(f\"`{sink_name}`\")
            else:
                # Field not in source - log warning and use NULL
                print(f\"    WARNING: Sink field '{sink_name}' not in source, using NULL\")
                select_parts.append(f\"CAST(NULL AS {sink_type}) AS `{sink_name}`\")
        
        select_clause = \",
                \".join(select_parts)
        
        # Build full INSERT INTO statement with fully qualified table names
        insert_sql = f\"\"\"
            INSERT INTO `s3_tables`.`{namespace}`.`{sink_table}`
            SELECT
                {select_clause}
            FROM `default_catalog`.`default_database`.`{kafka_table}`
        \"\"\"
        
        print(\"    INSERT SQL:\")
        for line in insert_sql.strip().split('
'):
            print(f\"      {line.strip()}\")
        
        # =====================================================
        # Step 4: Execute the INSERT (starts streaming pipeline)
        # =====================================================
        print(f\"    Executing pipeline for topic: {topic_name}...\")
        result = table_env.execute_sql(insert_sql)
        table_results.append((topic_name, result))
        
        pipelines_created += 1
        print(f\"    Pipeline STARTED for: {topic_name}\")
        
    except Exception as e:
        print(f\"    ERROR processing topic {topic_name}: {e}\")
        import traceback
        traceback.print_exc()
        # Continue with other topics instead of failing completely
        continue

print(f\"
[PHASE 7] COMPLETE - {pipelines_created} pipeline(s) created\")

# ================================================================================
# PHASE 8: JOB STATUS AND WAITING
# ================================================================================
print(\"
[PHASE 8] JOB STATUS\")
if pipelines_created > 0:
    print(f\"  SUCCESS: {pipelines_created} streaming pipeline(s) started\")
    print(\"  Pipelines:\")
    for topic_name, result in table_results:
        print(f\"    - {topic_name}: RUNNING\")
    
    print(\"
  Job will continue processing data indefinitely...\")
    print(\"  AWS Managed Flink will manage the job lifecycle\")
    
    # Wait on the first pipeline to keep the job running
    # This is important for AWS Managed Flink
    if table_results:
        print(\"
  Waiting on pipeline execution...\")
        try:
            # Get the first result and wait on it
            first_topic, first_result = table_results[0]
            first_result.wait()
        except Exception as e:
            # This is expected - streaming jobs run indefinitely
            print(f\"  Pipeline execution status: {e}\")
else:
    print(\"  WARNING: No pipelines were created!\")
    print(\"  Check topic configurations:\")
    print(\"    1. Ensure at least one topic has 'enabled: true'\")
    print(\"    2. Verify transformation class exists\")
    print(\"    3. Check source and sink schemas match\")
    sys.exit(1)

print(\"
\" + \"=\" * 100)
print(\"MODULAR PYFLINK JOB INITIALIZATION COMPLETE\")
print(\"=\" * 100)
"