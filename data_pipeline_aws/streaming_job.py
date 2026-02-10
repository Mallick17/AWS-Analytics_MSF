#!/usr/bin/env python3
"""
Main Flink Job Orchestrator

Dynamically creates pipelines for all enabled Kafka topics.
Supports both local and AWS deployment with automatic environment detection.
"""

import os
import sys
import logging
import importlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment detection
IS_AWS = os.getenv("DEPLOYMENT_ENV") == "aws"

print("=" * 80)
print("FLINK TRANSFORMATION PIPELINE")
print("=" * 80)
print(f"Environment: {'AWS' if IS_AWS else 'LOCAL'}")
print(f"Python version: {sys.version}")
print(f"Working directory: {os.getcwd()}")

# JAR injection for AWS
if IS_AWS:
    print("\n[AWS MODE] Injecting JAR dependencies...")
    try:
        from pyflink.java_gateway import get_gateway
        
        SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
        jar_path = os.path.join(SCRIPT_DIR, "lib", "pyflink-dependencies.jar")
        
        if not os.path.exists(jar_path):
            logger.error(f"JAR not found at {jar_path}")
            sys.exit(1)
        
        gateway = get_gateway()
        jar_url = gateway.jvm.java.net.URL(f"file://{jar_path}")
        gateway.jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)
        
        print(f"✓ JAR injected: {jar_path}")
    except Exception as e:
        logger.error(f"Failed to inject JAR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

# Add modules to Python path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
for module_dir in ['flink', 'transformations', 'config']:
    module_path = os.path.join(SCRIPT_DIR, module_dir)
    if os.path.exists(module_path) and module_path not in sys.path:
        sys.path.insert(0, module_path)
        print(f"✓ Added to path: {module_path}")

# Import Flink modules
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from flink.common.config import Config
from flink.common.catalog_manager import CatalogManager
from flink.common.kafka_source_factory import KafkaSourceFactory
from flink.common.iceberg_sink_factory import IcebergSinkFactory


def main():
    """Main orchestrator function."""
    
    print("\n" + "=" * 80)
    print("LOADING CONFIGURATION")
    print("=" * 80)
    
    # Load configuration
    config = Config()
    
    # Get enabled topics
    enabled_topics = config.get_enabled_topics()
    print(f"\nEnabled topics ({len(enabled_topics)}):")
    for topic in enabled_topics:
        print(f"  - {topic}")
    
    if not enabled_topics:
        logger.warning("No enabled topics found in configuration!")
        return
    
    print("\n" + "=" * 80)
    print("CREATING FLINK ENVIRONMENT")
    print("=" * 80)
    
    # Create Flink environment
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(settings)
    
    # Set parallelism
    parallelism = int(os.getenv("FLINK_PARALLELISM", "1"))
    table_env.get_config().set("parallelism.default", str(parallelism))
    print(f"✓ Parallelism: {parallelism}")
    
    print("\n" + "=" * 80)
    print("CREATING ICEBERG CATALOG")
    print("=" * 80)
    
    # Create Iceberg catalog
    iceberg_config = config.get_iceberg_config()
    catalog_mgr = CatalogManager(table_env, iceberg_config)
    catalog_name, namespace = catalog_mgr.create_catalog()
    
    print(f"✓ Catalog: {catalog_name}")
    print(f"✓ Namespace: {namespace}")
    
    # Get Kafka configuration
    kafka_config = config.get_kafka_config()
    
    print("\n" + "=" * 80)
    print("CREATING PIPELINES")
    print("=" * 80)
    
    # Create pipelines for each enabled topic
    pipelines_created = 0
    
    for topic_name in enabled_topics:
        print(f"\n{'─' * 80}")
        print(f"Processing: {topic_name}")
        print(f"{'─' * 80}")
        
        try:
            # Get topic configuration
            topic_config = config.get_topic_config(topic_name)
            transformation_name = topic_config.get('transformation')
            
            if not transformation_name:
                logger.warning(f"No transformation specified for topic: {topic_name}")
                continue
            
            print(f"  Transformation: {transformation_name}")
            
            # Get transformation configuration
            trans_config = config.get_transformation_config(transformation_name)
            
            if not trans_config:
                logger.warning(f"Transformation not found: {transformation_name}")
                continue
            
            module_name = trans_config['module']
            class_name = trans_config['class']
            description = trans_config.get('description', 'No description')
            
            print(f"  Description: {description}")
            print(f"  Module: {module_name}")
            print(f"  Class: {class_name}")
            
            # Import and instantiate transformer
            print(f"  Loading transformer...")
            module = importlib.import_module(module_name)
            transformer_class = getattr(module, class_name)
            transformer = transformer_class(topic_config)
            
            # Validate configuration
            if not transformer.validate_config():
                logger.error(f"Invalid configuration for topic: {topic_name}")
                continue
            
            # Create Kafka source
            print(f"  Creating Kafka source...")
            kafka_factory = KafkaSourceFactory(kafka_config, topic_config)
            source_table = kafka_factory.create_source_table(table_env)
            
            # Create Iceberg sink
            print(f"  Creating Iceberg sink...")
            iceberg_factory = IcebergSinkFactory(catalog_name, namespace, topic_config)
            sink_table = iceberg_factory.create_sink_table(table_env)
            
            # Get transformation SQL
            print(f"  Generating transformation SQL...")
            transform_sql = transformer.get_transformation_sql(source_table)
            
            # Create and execute pipeline
            print(f"  Executing pipeline...")
            pipeline_sql = f"""
                INSERT INTO {sink_table}
                {transform_sql}
            """
            
            # Execute asynchronously
            table_env.execute_sql(pipeline_sql)
            
            print(f"  ✓ Pipeline created: {topic_name} → {sink_table}")
            pipelines_created += 1
            
        except Exception as e:
            logger.error(f"Failed to create pipeline for {topic_name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print("\n" + "=" * 80)
    print("PIPELINE SUMMARY")
    print("=" * 80)
    print(f"Total topics: {len(enabled_topics)}")
    print(f"Pipelines created: {pipelines_created}")
    print(f"Failed: {len(enabled_topics) - pipelines_created}")
    
    if pipelines_created > 0:
        print("\n✓ All pipelines created successfully!")
        print("Flink jobs are now running...")
    else:
        print("\n✗ No pipelines were created!")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
