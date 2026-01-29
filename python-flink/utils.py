# utils.py
import sys
import os
import time

# Send logs to CloudWatch
sys.stdout = sys.stderr

def log_message(message, level="INFO"):
    """Utility function for logging messages with dividers for major sections."""
    if level == "START" or level == "END" or level == "ERROR":
        print("=" * 60)
        print(message)
        print("=" * 60)
    else:
        print(message)

def import_pyflink():
    """Handle PyFlink imports and log success."""
    try:
        from pyflink.table import EnvironmentSettings, TableEnvironment
        from pyflink.java_gateway import get_gateway
        log_message("PyFlink imports successful")
        return EnvironmentSettings, TableEnvironment, get_gateway
    except Exception as e:
        raise ImportError(f"Failed to import PyFlink modules: {e}")

def inject_dependency_jar():
    """Inject the dependency JAR into the JVM classloader."""
    EnvironmentSettings, TableEnvironment, get_gateway = import_pyflink()
    gateway = get_gateway()
    jvm = gateway.jvm
    base_dir = os.path.dirname(os.path.abspath(__file__))
    jar_file = os.path.join(base_dir, "lib", "pyflink-dependencies.jar")
    
    if not os.path.exists(jar_file):
        raise RuntimeError(f"Dependency JAR not found: {jar_file}")
    
    jar_url = jvm.java.net.URL(f"file://{jar_file}")
    jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)
    log_message(f"Injected dependency JAR: {jar_file}")