# streaming_job.py (main entry point)
import time
from config import MSK_BOOTSTRAP_SERVERS, KAFKA_TOPIC, S3_WAREHOUSE, NAMESPACE, EVENT_TYPES
from utils import log_message, inject_dependency_jar
from environment import create_table_environment
from catalog import create_iceberg_catalog
from source import create_kafka_source
from sink import create_iceberg_sinks, submit_insert_jobs

def main():
    log_message("STARTING PYFLINK MSK -> S3 TABLES JOB (JSON FORMAT - SPLIT SINKS)", level="START")
    
    try:
        inject_dependency_jar()
        table_env = create_table_environment()
        create_iceberg_catalog(table_env, S3_WAREHOUSE, NAMESPACE)
        create_kafka_source(table_env, MSK_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
        create_iceberg_sinks(table_env, EVENT_TYPES)
        submit_insert_jobs(table_env, EVENT_TYPES)
        
        # Keep alive
        while True:
            time.sleep(60)
    
    except Exception:
        log_message("FATAL ERROR", level="ERROR")
        import traceback
        traceback.print_exc()
        log_message("", level="ERROR")
        raise

if __name__ == "__main__":
    main()