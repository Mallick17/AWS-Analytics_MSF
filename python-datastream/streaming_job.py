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
# PHASE 4: LOAD CONFIGURATION
# ================================================================================
print("\n[PHASE 4] LOADING CONFIGURATION")

try:
    from common.config import Config, FLINK_CONFIG, ICEBERG_CONFIG
    print("  ✓ Config module imported")
    
    # Initialize configuration loader
    config_dir = os.path.join(SCRIPT_DIR, "config")
    print(f"  Config directory: {config_dir}")
    
    config = Config(config_dir=config_dir)
    print("  ✓ Configuration loaded successfully")
    
    # Get Kafka configuration
    kafka_config = config.get_kafka_config()
    print(f"  ✓ Kafka bootstrap servers: {kafka_config.get('bootstrap_servers', 'MISSING')[:50]}...")
    
    # Get enabled topics
    enabled_topics = config.get_enabled_topics()
    print(f"  ✓ Enabled topics: {enabled_topics}")
    
    if not enabled_topics:
        print("  WARNING: No enabled topics found!")
    
except Exception as e:
    print(f"FATAL: CONFIGURATION LOAD FAILED: {e}", file=sys.stderr)
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
    print("  ✓ TableEnvironment created")
    
    # Apply Flink configuration
    flink_config = table_env.get_config().get_configuration()
    flink_config.set_string("table.exec.resource.default-parallelism", FLINK_CONFIG.get("parallelism", "1"))
    flink_config.set_string("execution.checkpointing.interval", FLINK_CONFIG.get("checkpointing_interval", "60s"))
    flink_config.set_string("execution.checkpointing.mode", FLINK_CONFIG.get("checkpointing_mode", "EXACTLY_ONCE"))
    print("  ✓ Flink configuration applied:")
    print(f"    - Parallelism: {FLINK_CONFIG.get('parallelism')}")
    print(f"    - Checkpointing interval: {FLINK_CONFIG.get('checkpointing_interval')}")
    print(f"    - Checkpointing mode: {FLINK_CONFIG.get('checkpointing_mode')}")
    
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

try:
    from common.catalog_manager import CatalogManager
    print("  ✓ CatalogManager imported")
except ImportError as e:
    print(f"FATAL: Cannot import CatalogManager: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

# Use ICEBERG_CONFIG from config.py (with environment variable overrides)
iceberg_config = {
    "warehouse": os.getenv("S3_WAREHOUSE", ICEBERG_CONFIG["warehouse"]),
    "region": os.getenv("AWS_REGION", ICEBERG_CONFIG["region"]),
    "namespace": os.getenv("ICEBERG_NAMESPACE", ICEBERG_CONFIG["namespace"])
}

print(f"  Iceberg Configuration:")
print(f"    Warehouse: {iceberg_config['warehouse']}")
print(f"    Region: {iceberg_config['region']}")
print(f"    Namespace: {iceberg_config['namespace']}")

# Create and initialize catalog using CatalogManager
try:
    catalog_manager = CatalogManager(table_env, iceberg_config)
    catalog_manager.create_catalog()
except Exception as e:
    print(f"FATAL: S3 TABLES CATALOG SETUP FAILED: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

print("[PHASE 6] COMPLETE")

# ================================================================================
# PHASE 7: PROCESS ENABLED TOPICS - CREATE PIPELINES
# ================================================================================
print("\n[PHASE 7] PROCESSING TOPICS")

# Import modular components
try:
    from common.kafka_sources import KafkaSourceCreator
    from common.iceberg_sinks import IcebergSinkCreator
    from common.utils import load_and_instantiate_transformer, validate_topic_config
    print("  ✓ Modular components imported")
except ImportError as e:
    print(f"FATAL: Cannot import modular components: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

# Load transformations registry
try:
    transformations_registry = config.get_transformations_config()
    print(f"  ✓ Loaded {len(transformations_registry)} transformation(s):")
    for trans_name in transformations_registry:
        trans_config = transformations_registry[trans_name]
        print(f"    • {trans_name}")
        print(f"        Class: {trans_config.get('class', 'MISSING')}")
        print(f"        Module: {trans_config.get('module', 'MISSING')}")
except Exception as e:
    print(f"FATAL: Cannot load transformations registry: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

# Get catalog and namespace from catalog_manager
catalog_name = catalog_manager.get_catalog_name()
namespace = catalog_manager.get_namespace()

# Get Kafka global config from Config object
kafka_global = config.get_kafka_config()

# Initialize modular creators (reusable across all topics)
print("\n  Initializing modular components:")
kafka_creator = KafkaSourceCreator(table_env, kafka_global)
print("    ✓ KafkaSourceCreator initialized")

iceberg_creator = IcebergSinkCreator(table_env, catalog_manager, ICEBERG_CONFIG)
print("    ✓ IcebergSinkCreator initialized")

# Track pipelines
pipelines_created = 0
table_results = []

# Process each enabled topic
for topic_name in enabled_topics:
    print(f"\n  {'=' * 80}")
    print(f"  PROCESSING TOPIC: {topic_name}")
    print(f"  {'=' * 80}")
    
    try:
        # Get topic-specific configuration
        topic_config = config.get_topic_config(topic_name)
        
        # Validate topic configuration has all required fields
        validate_topic_config(topic_config, topic_name)
        
        # ========================================================================
        # STEP 1: Create Kafka Source Table (using KafkaSourceCreator)
        # ========================================================================
        print("\n  [STEP 1] Creating Kafka Source")
        kafka_table = kafka_creator.create_source(topic_name, topic_config)
        
        # ========================================================================
        # STEP 2: Create Iceberg Sink Table (using IcebergSinkCreator)
        # ========================================================================
        print("\n  [STEP 2] Creating Iceberg Sink")
        sink_table = iceberg_creator.create_sink(topic_config)
        
        # ========================================================================
        # STEP 3: Load and Execute Transformer (using modular transformer classes)
        # ========================================================================
        print("\n  [STEP 3] Loading Transformation")
        
        # Get transformation name from topic config
        transformation_name = topic_config.get('transformation')
        if not transformation_name:
            raise ValueError(
                f"No transformation specified for topic '{topic_name}'. "
                f"Add 'transformation: <name>' to topic config in topics.yaml"
            )
        
        # Load and instantiate transformer using utils
        transformer = load_and_instantiate_transformer(
            transformation_name,
            transformations_registry,
            topic_config
        )
        
        # Get transformation SQL from transformer
        # Provide fully qualified source table name
        source_table_fqn = f"`default_catalog`.`default_database`.`{kafka_table}`"
        
        print(f"    Generating transformation SQL...")
        transformation_sql = transformer.get_transformation_sql(source_table_fqn)
        print(f"    ✓ Transformation SQL generated")
        
        # Optional: Print SQL preview for debugging (first 10 lines)
        sql_lines = transformation_sql.strip().split('\n')
        if len(sql_lines) <= 10:
            print(f"    SQL Preview:")
            for line in sql_lines:
                print(f"      {line.strip()}")
        else:
            print(f"    SQL Preview (first 10 lines):")
            for line in sql_lines[:10]:
                print(f"      {line.strip()}")
            print(f"      ... ({len(sql_lines) - 10} more lines)")
        
        # ========================================================================
        # STEP 4: Build and Execute INSERT Statement
        # ========================================================================
        print("\n  [STEP 4] Executing Pipeline")
        
        # Build INSERT statement using transformer's SQL
        insert_sql = f"""
            INSERT INTO `{catalog_name}`.`{namespace}`.`{sink_table}`
            {transformation_sql}
        """
        
        print(f"    INSERT INTO: {catalog_name}.{namespace}.{sink_table}")
        print(f"    FROM: {kafka_table}")
        print(f"    Executing streaming pipeline...")
        
        # Execute the streaming pipeline
        result = table_env.execute_sql(insert_sql)
        table_results.append((topic_name, result))
        
        pipelines_created += 1
        print(f"\n    ✓ ✓ ✓ Pipeline STARTED for: {topic_name} ✓ ✓ ✓")
        
    except ValueError as e:
        # Configuration or validation errors
        print(f"\n    ✗ CONFIGURATION ERROR for topic {topic_name}:", file=sys.stderr)
        print(f"    {e}", file=sys.stderr)
        print(f"    Skipping this topic and continuing...\n")
        continue
        
    except ImportError as e:
        # Transformer loading errors
        print(f"\n    ✗ TRANSFORMER LOAD ERROR for topic {topic_name}:", file=sys.stderr)
        print(f"    {e}", file=sys.stderr)
        print(f"    Skipping this topic and continuing...\n")
        continue
        
    except Exception as e:
        # Any other errors
        print(f"\n    ✗ ERROR processing topic {topic_name}:", file=sys.stderr)
        print(f"    {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        print(f"    Skipping this topic and continuing...\n")
        continue

print(f"\n{'=' * 100}")
print(f"[PHASE 7] COMPLETE - {pipelines_created} pipeline(s) created")
print(f"{'=' * 100}")

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