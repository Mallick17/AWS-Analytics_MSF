# environment.py (UPDATED: added pipeline.jars config + textwrap import for future)
import textwrap
from utils import import_pyflink, log_message

def create_table_environment():
    """Create and configure the TableEnvironment."""
    EnvironmentSettings, TableEnvironment, get_gateway = import_pyflink()
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    log_message("TableEnvironment created")
    
    config = table_env.get_config().get_configuration()
    config.set_string("table.exec.resource.default-parallelism", "1")
    config.set_string("execution.checkpointing.interval", "60s")
    config.set_string("execution.checkpointing.timeout", "2min")
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    config.set_string("taskmanager.log.level", "DEBUG")
    # Rely on Managed Flink classpath (no manual injection)
    config.set_string("pipeline.jars", "file://./lib/pyflink-dependencies.jar")
    log_message("Pipeline configuration applied")
    
    return table_env