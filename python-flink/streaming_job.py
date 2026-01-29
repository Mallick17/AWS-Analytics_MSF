# streaming_job.py (UPDATED: removed inject_dependency_jar call, enhanced except)
import time
import sys
from config import MSK_BOOTSTRAP_SERVERS, KAFKA_TOPIC, S3_WAREHOUSE, NAMESPACE, EVENT_TYPES
from utils import log_message  # Removed inject_dependency_jar
from environment import create_table_environment
from catalog import create_iceberg_catalog
from source import create_kafka_source
from sink import create_iceberg_sinks, submit_insert_jobs

def main():
    log_message("STARTING PYFLINK MSK -> S3 TABLES JOB (JSON FORMAT - SPLIT SINKS)", level="START")
    
    try:
        # inject_dependency_jar()  # COMMENTED OUT - Managed Flink handles classpath
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
        print("PYTHON TRACEBACK:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        log_message("", level="ERROR")
        raise

if __name__ == "__main__":
    main()