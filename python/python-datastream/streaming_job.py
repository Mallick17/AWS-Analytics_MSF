import sys
import time
import importlib

# Redirect logs
sys.stdout = sys.stderr

print("=" * 80)
print("STARTING MODULAR PYFLINK STREAMING JOB")
print("=" * 80)

try:
    from pyflink.table import EnvironmentSettings, TableEnvironment
    
    from common.config import ConfigLoader
    from common.utils import inject_dependencies, create_table_environment
    from common.catalog_manager import CatalogManager
    from common.kafka_sources import KafkaSourceFactory
    from common.iceberg_sinks import IcebergSinkFactory
    
    print("✓ Imports successful")
    
    # Step 1: Load configurations
    config_loader = ConfigLoader()
    kafka_topics = config_loader.get_kafka_topics()
    transformations = config_loader.get_transformations()
    bootstrap_servers = config_loader.get_msk_bootstrap_servers()
    s3_warehouse = config_loader.get_s3_warehouse()
    namespace = config_loader.get_iceberg_namespace()
    
    print(f"✓ Loaded {len(kafka_topics)} topics, {len(transformations)} transformations")
    
    # Step 2: Setup environment
    inject_dependencies()
    table_env = create_table_environment()
    
    # Step 3: Setup Iceberg catalog
    catalog_mgr = CatalogManager(table_env, s3_warehouse, namespace)
    catalog_name = catalog_mgr.setup()
    
    # Step 4: Create Kafka sources for all topics
    kafka_factory = KafkaSourceFactory(table_env, bootstrap_servers)
    created_sources = set()
    
    for topic_key, topic_config in kafka_topics.items():
        source_table = f"kafka_{topic_key}"
        kafka_factory.create_source(source_table, topic_config)
        created_sources.add(source_table)
    
    # Step 5: Create Iceberg sinks and execute transformations
    iceberg_factory = IcebergSinkFactory(table_env, catalog_name, namespace)
    
    job_handles = []
    
    for tf_config in transformations:
        print(f"\n{'=' * 60}")
        print(f"Processing: {tf_config.name}")
        print(f"Description: {tf_config.description}")
        print(f"{'=' * 60}")
        
        # Dynamically load transformer class
        module_name = f"transformations.{tf_config.name}"
        module = importlib.import_module(module_name)
        transformer_class = getattr(module, tf_config.transformer_class)
        transformer = transformer_class(table_env)
        
        # Create sink table
        sink_schema = transformer.get_sink_schema()
        partition_by = transformer.get_partition_by()
        
        iceberg_factory.create_sink(
            table_name=tf_config.sink_table,
            schema=sink_schema,
            partition_by=partition_by
        )
        
        # Execute transformation (non-blocking)
        job_handle = transformer.execute(
            source_table=tf_config.source_table,
            sink_table=tf_config.sink_table
        )
        
        job_handles.append({
            'name': tf_config.name,
            'handle': job_handle
        })
        
        print(f"✓ Started: {tf_config.name} -> {tf_config.sink_table}")
    
    print("\n" + "=" * 80)
    print(f"✓ ALL {len(transformations)} TRANSFORMATIONS SUBMITTED SUCCESSFULLY")
    print("=" * 80)
    
    # Keep alive
    print("\nJob is running... (press Ctrl+C to stop)")
    while True:
        time.sleep(60)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Jobs running: {len(job_handles)}")

except KeyboardInterrupt:
    print("\n\nShutting down gracefully...")
    sys.exit(0)

except Exception as e:
    print("\n" + "=" * 80)
    print("FATAL ERROR")
    print("=" * 80)
    import traceback
    traceback.print_exc()
    print("=" * 80)
    raise
