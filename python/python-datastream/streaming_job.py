import sys
import os
import time
import importlib
import traceback

# CRITICAL: Redirect stdout to stderr for CloudWatch logs
sys.stdout = sys.stderr

print("=" * 80)
print("STARTING MODULAR PYFLINK STREAMING JOB")
print("=" * 80)

def main():
    try:
        # Step 1: Import PyFlink first
        from pyflink.table import EnvironmentSettings, TableEnvironment
        from pyflink.java_gateway import get_gateway
        print("✓ PyFlink imported")
        
        # Step 2: Inject dependencies BEFORE other imports
        print("Injecting JAR dependencies...")
        gateway = get_gateway()
        jvm = gateway.jvm
        base_dir = os.path.dirname(os.path.abspath(__file__))
        jar_file = os.path.join(base_dir, "lib", "pyflink-dependencies.jar")
        
        if not os.path.exists(jar_file):
            raise RuntimeError(f"Dependency JAR not found: {jar_file}")
        
        jar_url = jvm.java.net.URL(f"file://{jar_file}")
        jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)
        print(f"✓ Injected dependency JAR: {jar_file}")
        
        # Step 3: Now import application modules
        print("Importing application modules...")
        from common.config import ConfigLoader
        from common.catalog_manager import CatalogManager
        from common.kafka_sources import KafkaSourceFactory
        from common.iceberg_sinks import IcebergSinkFactory
        print("✓ All application modules imported")
        
        # Step 4: Load configurations
        print("\nLoading configurations...")
        config_loader = ConfigLoader()
        kafka_topics = config_loader.get_kafka_topics()
        transformations = config_loader.get_transformations()
        bootstrap_servers = config_loader.get_msk_bootstrap_servers()
        s3_warehouse = config_loader.get_s3_warehouse()
        namespace = config_loader.get_iceberg_namespace()
        print(f"✓ Loaded {len(kafka_topics)} topics, {len(transformations)} transformations")
        
        # Step 5: Setup environment
        print("\nSetting up Flink environment...")
        env_settings = EnvironmentSettings.in_streaming_mode()
        table_env = TableEnvironment.create(env_settings)
        
        # Runtime configuration
        config = table_env.get_config().get_configuration()
        config.set_string("table.exec.resource.default-parallelism", "1")
        config.set_string("execution.checkpointing.interval", "60s")
        config.set_string("execution.checkpointing.timeout", "2min")
        config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
        config.set_string("taskmanager.log.level", "DEBUG")
        print("✓ Pipeline configuration applied")
        
        # Step 6: Setup Iceberg catalog
        print("\nSetting up Iceberg catalog...")
        catalog_mgr = CatalogManager(table_env, s3_warehouse, namespace)
        catalog_name = catalog_mgr.setup()
        print(f"✓ Iceberg catalog '{catalog_name}' ready")
        
        # Step 7: Create Kafka sources
        print("\nCreating Kafka sources...")
        kafka_factory = KafkaSourceFactory(table_env, bootstrap_servers)
        for topic_key, topic_config in kafka_topics.items():
            source_table = f"kafka_{topic_key}"
            kafka_factory.create_source(source_table, topic_config)
        print(f"✓ Created {len(kafka_topics)} Kafka sources")
        
        # Step 8: Create sinks
        print("\nCreating Iceberg sinks...")
        iceberg_factory = IcebergSinkFactory(table_env, catalog_name, namespace)
        
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
            print(f"  ✓ Created sink: {tf_config.sink_table}")
        
        # Step 9: Execute all transformations as streaming INSERTs
        print(f"\n{'=' * 80}")
        print("SUBMITTING STREAMING JOBS")
        print("=" * 80)
        
        for tf_config in transformations:
            print(f"\nSubmitting: {tf_config.name}")
            
            # Dynamically load transformer class
            module_name = f"transformations.{tf_config.name}"
            module = importlib.import_module(module_name)
            transformer_class = getattr(module, tf_config.transformer_class)
            transformer = transformer_class(table_env)
            
            # Get the INSERT SQL
            insert_sql = transformer.get_insert_sql(
                source_table=tf_config.source_table,
                sink_table=tf_config.sink_table,
                catalog_name=catalog_name,
                namespace=namespace
            )
            
            print(f"  Executing SQL:\n{insert_sql}")
            
            # Submit the streaming job
            table_env.execute_sql(insert_sql)
            
            print(f"  ✓ Submitted: {tf_config.name}")
        
        print(f"\n{'=' * 80}")
        print(f"✓ ALL {len(transformations)} TRANSFORMATIONS RUNNING")
        print("=" * 80)
        
        # Keep alive
        print("\nJob is running... monitoring...")
        while True:
            time.sleep(60)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] All jobs running...")
    
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
    main()
