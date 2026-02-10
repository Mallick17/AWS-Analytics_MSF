# ==================================================================================
# CONFIGURATION LOADER
# ==================================================================================
# Loads YAML configurations for topics, transformations, and global settings.
# ==================================================================================

import os
import yaml
from typing import Dict, Any, List

# Load environment variables (from .env if present)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional, env vars can be set manually

# ==================================================================================
# GLOBAL FLINK CONFIGURATION
# ==================================================================================
FLINK_CONFIG = {
    "parallelism": "2",  # Changed from 1 to 2 to support multiple concurrent jobs
    "checkpointing_interval": "60s",
    "checkpointing_mode": "EXACTLY_ONCE"
}


# ==================================================================================
# GLOBAL ICEBERG CONFIGURATION
# ==================================================================================
ICEBERG_CONFIG = {
    "warehouse": "arn:aws:s3tables:ap-south-1:508351649560:bucket/rt-testing-cdc-bucket",
    "region": "ap-south-1",
    "namespace": "analytics",
    "format_version": "2",
    "write_format": "parquet",
    "compression_codec": "snappy"
}


# ==================================================================================
# CONFIG CLASS
# ==================================================================================
class Config:
    """Configuration loader for topics and transformations."""
    
    def __init__(self, config_dir: str = "config"):
        """Initialize configuration loader.
        
        Args:
            config_dir: Directory containing YAML configuration files
        """
        self.config_dir = config_dir
        self._topics_data = None
        self._transformations_data = None
        
        # Load configurations
        self._load_topics()
        self._load_transformations()
    
    def _load_topics(self):
        """Load topics configuration from topics.yaml"""
        topics_file = os.path.join(self.config_dir, "topics.yaml")
        
        if not os.path.exists(topics_file):
            raise FileNotFoundError(f"Topics config not found: {topics_file}")
        
        with open(topics_file, 'r') as f:
            self._topics_data = yaml.safe_load(f)
        
        if not self._topics_data:
            raise ValueError("Topics config file is empty")
    
    def _load_transformations(self):
        """Load transformations configuration from transformations.yaml"""
        trans_file = os.path.join(self.config_dir, "transformations.yaml")
        
        if not os.path.exists(trans_file):
            raise FileNotFoundError(f"Transformations config not found: {trans_file}")
        
        with open(trans_file, 'r') as f:
            self._transformations_data = yaml.safe_load(f)
        
        if not self._transformations_data:
            raise ValueError("Transformations config file is empty")
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka connection configuration with environment overrides."""
        kafka_config = self._topics_data.get('kafka', {}).copy()
        
        # Check if running locally
        if self.is_local_env():
            print("  Overriding Kafka config for local environment")
            # Override with local defaults or env vars
            BootstrapServers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092") # Default for Docker
            kafka_config["bootstrap_servers"] = BootstrapServers
            
            # Remove IAM auth for local
            kafka_config["security"] = {
                "protocol": "PLAINTEXT"
            }
            
        return kafka_config
    
    def get_topic_config(self, topic_name: str) -> Dict[str, Any]:
        """Get configuration for a specific topic.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Topic configuration dictionary
            
        Raises:
            ValueError: If topic not found
        """
        topics = self._topics_data.get('topics', {})
        
        if topic_name not in topics:
            available = list(topics.keys())
            raise ValueError(
                f"Topic '{topic_name}' not found in config. "
                f"Available topics: {available}"
            )
        
        return topics[topic_name]
    
    def get_enabled_topics(self) -> List[str]:
        """Get list of enabled topic names.
        
        Returns:
            List of topic names where enabled=true
        """
        topics = self._topics_data.get('topics', {})
        enabled = [
            name for name, config in topics.items()
            if config.get('enabled', False)
        ]
        return enabled
    
    def get_all_topics(self) -> Dict[str, Any]:
        """Get all topics configuration.
        
        Returns:
            Dictionary of all topics
        """
        return self._topics_data.get('topics', {})
    
    def get_transformations_config(self) -> Dict[str, Any]:
        """Get transformations registry.
        
        Returns:
            Dictionary mapping transformation names to their class/module info
        """
        transformations = self._transformations_data.get('transformations', {})
        
        if not transformations:
            raise ValueError(
                "No transformations defined in transformations.yaml. "
                "At least one transformation must be configured."
            )
        
        return transformations
    
    def get_transformation_config(self, transformation_name: str) -> Dict[str, Any]:
        """Get configuration for a specific transformation.
        
        Args:
            transformation_name: Name of the transformation
            
        Returns:
            Transformation configuration dictionary
            
        Raises:
            ValueError: If transformation not found
        """
        transformations = self.get_transformations_config()
        
        if transformation_name not in transformations:
            available = list(transformations.keys())
            raise ValueError(
                f"Transformation '{transformation_name}' not found. "
                f"Available transformations: {available}"
            )
        
        return transformations[transformation_name]

    def get_iceberg_config(self) -> Dict[str, Any]:
        """Get Iceberg configuration with environment overrides."""
        # Start with default config
        config = ICEBERG_CONFIG.copy()
        
        # Override with environment variables
        warehouse = os.getenv("S3_WAREHOUSE")
        if warehouse:
            config["warehouse"] = warehouse
            
        region = os.getenv("AWS_REGION")
        if region:
            config["region"] = region
            
        namespace = os.getenv("ICEBERG_NAMESPACE")
        if namespace:
            config["namespace"] = namespace
            
        return config

    def is_local_env(self) -> bool:
        """Check if running in local environment."""
        return os.getenv("FLINK_ENV") == "local"