import os
from pyflink.java_gateway import get_gateway

def inject_dependencies(jar_name: str = "pyflink-dependencies.jar"):
    """Inject JAR dependencies into classloader"""
    gateway = get_gateway()
    jvm = gateway.jvm
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    jar_file = os.path.join(base_dir, "lib", jar_name)
    
    if not os.path.exists(jar_file):
        raise RuntimeError(f"Dependency JAR not found: {jar_file}")
    
    jar_url = jvm.java.net.URL(f"file://{jar_file}")
    jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)
    print(f"✓ Injected dependency JAR: {jar_file}")

def setup_table_environment(flink_config):
    """Create and configure TableEnvironment"""
    from pyflink.table import EnvironmentSettings, TableEnvironment
    
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    
    config = table_env.get_config().get_configuration()
    config.set_string("table.exec.resource.default-parallelism", str(flink_config.parallelism))
    config.set_string("execution.checkpointing.interval", flink_config.checkpoint_interval)
    config.set_string("execution.checkpointing.timeout", flink_config.checkpoint_timeout)
    config.set_string("execution.checkpointing.mode", flink_config.checkpoint_mode)
    
    print("✓ TableEnvironment configured")
    return table_env