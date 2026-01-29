# ==================================================================================
# MODULAR PYFLINK STREAMING JOB
# ==================================================================================
# Configuration-driven Flink streaming job with modular architecture.
# All configurations are loaded from YAML files in the config/ directory.
# Transformations are implemented as separate classes in transformations/ package.
# ==================================================================================

import sys
import os
import time

# Send logs to CloudWatch
sys.stdout = sys.stderr

print(\"=\" * 80)
print(\"STARTING MODULAR PYFLINK STREAMING JOB\")
print(\"=\" * 80)

try:
    from pyflink.table import EnvironmentSettings, TableEnvironment
    from pyflink.java_gateway import get_gateway
    
    # Import our modular components
    from common.config import Config, FLINK_CONFIG, ICEBERG_CONFIG
    from common.catalog_manager import CatalogManager
    from common.kafka_sources import KafkaSourceCreator
    from common.iceberg_sinks import IcebergSinkCreator
    from common.utils import load_transformer_class
    
    print(\"✓ PyFlink imports successful\")
    print(\"✓ Modular components loaded\")

    # ============================================================================
    # CLASSLOADER WORKAROUND FOR DEPENDENCIES
    # ============================================================================
    gateway = get_gateway()
    jvm = gateway.jvm
    base_dir = os.path.dirname(os.path.abspath(__file__))
    jar_file = os.path.join(base_dir, \"lib\", \"pyflink-dependencies.jar\")

    if not os.path.exists(jar_file):
        raise RuntimeError(f\"Dependency JAR not found: {jar_file}\")

    jar_url = jvm.java.net.URL(f\"file://{jar_file}\")
    jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)
    print(f\"✓ Injected dependency JAR: {jar_file}\")

    # ============================================================================
    # LOAD CONFIGURATION
    # ============================================================================
    print(\"
\" + \"=\" * 80)
    print(\"LOADING CONFIGURATION\")
    print(\"=\" * 80)
    
    config = Config(config_dir=\"config\")
    kafka_config = config.get_kafka_config()
    print(f\"✓ Kafka bootstrap servers: {kafka_config['bootstrap_servers'][:50]}...\")

    # ============================================================================
    # TABLE ENVIRONMENT SETUP
    # ============================================================================
    print(\"
\" + \"=\" * 80)
    print(\"INITIALIZING FLINK TABLE ENVIRONMENT\")
    print(\"=\" * 80)
    
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    print(\"✓ TableEnvironment created\")

    # Apply Flink runtime configuration
    flink_config = table_env.get_config().get_configuration()
    flink_config.set_string(\"table.exec.resource.default-parallelism\", FLINK_CONFIG[\"parallelism\"])
    flink_config.set_string(\"execution.checkpointing.interval\", FLINK_CONFIG[\"checkpointing_interval\"])
    flink_config.set_string(\"execution.checkpointing.timeout\", FLINK_CONFIG[\"checkpointing_timeout\"])
    flink_config.set_string(\"execution.checkpointing.mode\", FLINK_CONFIG[\"checkpointing_mode\"])
    flink_config.set_string(\"taskmanager.log.level\", FLINK_CONFIG[\"log_level\"])
    print(\"✓ Flink runtime configuration applied\")

    # ============================================================================
    # ICEBERG CATALOG SETUP
    # ============================================================================
    print(\"
\" + \"=\" * 80)
    print(\"SETTING UP ICEBERG CATALOG\")
    print(\"=\" * 80)
    
    catalog_manager = CatalogManager(table_env, ICEBERG_CONFIG)
    catalog_manager.create_catalog()

    # ============================================================================
    # INITIALIZE COMPONENT CREATORS
    # ============================================================================
    kafka_source_creator = KafkaSourceCreator(table_env, kafka_config)
    iceberg_sink_creator = IcebergSinkCreator(table_env, catalog_manager, ICEBERG_CONFIG)
    
    print(\"
✓ Component creators initialized\")

    # ============================================================================
    # PROCESS ENABLED TOPICS
    # ============================================================================
    print(\"
\" + \"=\" * 80)
    print(\"PROCESSING ENABLED TOPICS\")
    print(\"=\" * 80)
    
    enabled_topics = config.get_enabled_topics()
    
    if not enabled_topics:
        print(\"WARNING: No enabled topics found. Please enable topics in config/topics.yaml\")
        print(\"Job will start but no data will be processed.\")
    
    for topic_name in enabled_topics:
        print(\"
\" + \"-\" * 80)
        print(f\"Setting up pipeline for: {topic_name}\")
        print(\"-\" * 80)
        
        # Get topic configuration
        topic_config = config.get_topic_config(topic_name)
        
        # Step 1: Create Kafka source
        kafka_table_name = kafka_source_creator.create_source(topic_name, topic_config)
        
        # Step 2: Create Iceberg sink
        sink_table_name = iceberg_sink_creator.create_sink(topic_config)
        
        # Step 3: Load and instantiate transformer
        transformation_name = topic_config['transformation']
        transformation_config = config.get_transformation_config(transformation_name)
        
        print(f\"Loading transformation: {transformation_name}\")
        transformer_class = load_transformer_class(
            transformation_config['module'],
            transformation_config['class']
        )
        transformer = transformer_class(topic_config)
        print(f\"✓ Transformer loaded: {transformer.get_description()}\")
        
        # Step 4: Generate transformation SQL
        source_table_ref = f\"default_catalog.default_database.{kafka_table_name}\"
        transformation_sql = transformer.get_transformation_sql(source_table_ref)
        
        # Step 5: Create and submit streaming INSERT
        insert_sql = f\"\"\"
            INSERT INTO {catalog_manager.get_catalog_name()}.{catalog_manager.get_namespace()}.{sink_table_name}
            {transformation_sql}
        \"\"\"
        
        print(f\"Submitting streaming INSERT for {topic_name}\")
        table_env.execute_sql(insert_sql)
        print(f\"✓ Pipeline submitted successfully for {topic_name}\")

    # ============================================================================
    # JOB SUMMARY
    # ============================================================================
    print(\"
\" + \"=\" * 80)
    print(\"JOB STARTUP COMPLETE\")
    print(\"=\" * 80)
    print(f\"Total pipelines created: {len(enabled_topics)}\")
    print(f\"Active topics: {', '.join(enabled_topics)}\")
    print(\"Job is now running in streaming mode...\")
    print(\"=\" * 80)

    # Keep alive - streaming job runs indefinitely
    while True:
        time.sleep(60)

except Exception:
    print(\"
\" + \"=\" * 80)
    print(\"FATAL ERROR\")
    print(\"=\" * 80)
    import traceback
    traceback.print_exc()
    print(\"=\" * 80)
    raise
"