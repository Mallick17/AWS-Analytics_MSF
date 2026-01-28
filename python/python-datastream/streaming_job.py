import sys
import time
import traceback

# CRITICAL: Redirect stdout to stderr for CloudWatch logs
sys.stdout = sys.stderr

def main():
    print("=" * 80)
    print("STARTING MODULAR PYFLINK STREAMING JOB")
    print("=" * 80)
    print(f"Python version: {sys.version}")
    print(f"Working directory: {os.getcwd()}")
    print(f"Python path: {sys.path}")
    print("=" * 80)

    try:
        # Test imports one by one to find the failing import
        print("Testing imports...")
        
        print("  Importing pyflink...")
        from pyflink.table import EnvironmentSettings, TableEnvironment
        print("  ✓ pyflink imported")
        
        print("  Importing common.config...")
        from common.config import ConfigLoader
        print("  ✓ common.config imported")
        
        print("  Importing common.utils...")
        from common.utils import inject_dependencies, create_table_environment
        print("  ✓ common.utils imported")
        
        print("  Importing common.catalog_manager...")
        from common.catalog_manager import CatalogManager
        print("  ✓ common.catalog_manager imported")
        
        print("  Importing common.kafka_sources...")
        from common.kafka_sources import KafkaSourceFactory
        print("  ✓ common.kafka_sources imported")
        
        print("  Importing common.iceberg_sinks...")
        from common.iceberg_sinks import IcebergSinkFactory
        print("  ✓ common.iceberg_sinks imported")
        
        print("\n✓ All imports successful\n")
        
        # Step 1: Load configurations
        print("Loading configurations...")
        config_loader = ConfigLoader()
        kafka_topics = config_loader.get_kafka_topics()
        transformations = config_loader.get_transformations()
        bootstrap_servers = config_loader.get_msk_bootstrap_servers()
        s3_warehouse = config_loader.get_s3_warehouse()
        namespace = config_loader.get_iceberg_namespace()
        
        print(f"✓ Loaded {len(kafka_topics)} topics, {len(transformations)} transformations")
        
        # Step 2: Setup environment
        print("\nSetting up Flink environment...")
        inject_dependencies()
        table_env = create_table_environment()
        
        # Step 3: Setup Iceberg catalog
        print("\nSetting up Iceberg catalog...")
        catalog_mgr = CatalogManager(table_env, s3_warehouse, namespace)
        catalog_name = catalog_mgr.setup()
        
        # Step 4: Create Kafka sources
        print("\nCreating Kafka sources...")
        kafka_factory = KafkaSourceFactory(table_env, bootstrap_servers)
        
        for topic_key, topic_config in kafka_topics.items():
            source_table = f"kafka_{topic_key}"
            kafka_factory.create_source(source_table, topic_config)
        
        # Step 5: Create sinks and execute transformations
        print("\nCreating Iceberg sinks and starting transformations...")
        iceberg_factory = IcebergSinkFactory(table_env, catalog_name, namespace)
        
        job_handles = []
        
        for tf_config in transformations:
            print(f"\n{'=' * 60}")
            print(f"Processing: {tf_config.name}")
            print(f"Description: {tf_config.description}")
            print(f"{'=' * 60}")
            
            # Dynamically load transformer class
            module_name = f"transformations.{tf_config.name}"
            print(f"  Importing module: {module_name}")
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
            
            # Execute transformation
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
        print(f"✓ ALL {len(transformations)} TRANSFORMATIONS RUNNING")
        print("=" * 80)
        
        # Keep alive
        print("\nJob is running... monitoring...")
        while True:
            time.sleep(60)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Jobs: {len(job_handles)}")

    except ImportError as e:
        print("\n" + "=" * 80)
        print("IMPORT ERROR - Missing module or dependency")
        print("=" * 80)
        print(f"Error: {e}")
        traceback.print_exc()
        print("=" * 80)
        sys.exit(1)
        
    except FileNotFoundError as e:
        print("\n" + "=" * 80)
        print("FILE NOT FOUND ERROR")
        print("=" * 80)
        print(f"Error: {e}")
        traceback.print_exc()
        print("=" * 80)
        sys.exit(1)
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("FATAL ERROR")
        print("=" * 80)
        print(f"Error type: {type(e).__name__}")
        print(f"Error: {e}")
        traceback.print_exc()
        print("=" * 80)
        sys.exit(1)

if __name__ == "__main__":
    import os
    import importlib
    main()