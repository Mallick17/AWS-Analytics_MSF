import os
from pyflink.java_gateway import get_gateway
from pyflink.table import EnvironmentSettings, TableEnvironment

def inject_dependencies():
    """Inject JAR dependencies"""
    gateway = get_gateway()
    jvm = gateway.jvm
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    jar_file = os.path.join(base_dir, "lib", "pyflink-dependencies.jar")
    
    if not os.path.exists(jar_file):
        raise RuntimeError(f"Dependency JAR not found: {jar_file}")
    
    jar_url = jvm.java.net.URL(f"file://{jar_file}")
    jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)
    print(f"✓ Injected JAR: {jar_file}")

def create_table_environment():
    """Create and configure Flink TableEnvironment"""
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    
    config = table_env.get_config().get_configuration()
    config.set_string("table.exec.resource.default-parallelism", "1")
    config.set_string("execution.checkpointing.interval", "60s")
    config.set_string("execution.checkpointing.timeout", "2min")
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    
    print("✓ TableEnvironment created")
    return table_env