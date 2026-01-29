# ==================================================================================
# CONFIGURATION LOADER
# ==================================================================================
# Loads and manages YAML configuration files for topics and transformations.
# ==================================================================================

import yaml
import os
from typing import Dict, List, Any


class Config:
    """Configuration manager for loading YAML configs."""
    
    def __init__(self, config_dir: str = "config"):
        """Initialize configuration loader.
        
        Args:
            config_dir: Directory containing YAML config files
        """
        self.config_dir = config_dir
        self._topics_config = None
        self._transformations_config = None
        self._load_configs()
    
    def _load_configs(self):
        """Load all YAML configuration files."""
        topics_path = os.path.join(self.config_dir, "topics.yaml")
        transformations_path = os.path.join(self.config_dir, "transformations.yaml")
        
        print(f"Loading configuration from: {self.config_dir}")
        
        # Load topics configuration
        with open(topics_path, 'r') as f:
            self._topics_config = yaml.safe_load(f)
        print(f"✓ Loaded topics configuration: {len(self._topics_config['topics'])} topics defined")
        
        # Load transformations configuration
        with open(transformations_path, 'r') as f:
            self._transformations_config = yaml.safe_load(f)
        print(f"✓ Loaded transformations configuration: {len(self._transformations_config['transformations'])} transformations defined")
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka connection configuration.
        
        Returns:
            Dictionary with Kafka configuration
        """
        return self._topics_config['kafka']
    
    def get_enabled_topics(self) -> List[str]:
        """Get list of enabled topic names.
        
        Returns:
            List of enabled topic names
        """
        topics = self._topics_config['topics']
        enabled = [name for name, config in topics.items() if config.get('enabled', False)]
        print(f"Found {len(enabled)} enabled topic(s): {enabled}")
        return enabled
    
    def get_topic_config(self, topic_name: str) -> Dict[str, Any]:
        """Get configuration for a specific topic.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Topic configuration dictionary
        """
        topics = self._topics_config['topics']
        if topic_name not in topics:
            raise ValueError(f"Topic '{topic_name}' not found in configuration")
        return topics[topic_name]
    
    def get_transformation_config(self, transformation_name: str) -> Dict[str, Any]:
        """Get configuration for a specific transformation.
        
        Args:
            transformation_name: Name of the transformation
            
        Returns:
            Transformation configuration dictionary
        """
        transformations = self._transformations_config['transformations']
        if transformation_name not in transformations:
            raise ValueError(f"Transformation '{transformation_name}' not found in configuration")
        return transformations[transformation_name]
    
    def get_all_topics(self) -> Dict[str, Any]:
        """Get all topic configurations.
        
        Returns:
            Dictionary of all topics
        """
        return self._topics_config['topics']


# Flink runtime configuration
FLINK_CONFIG = {
    "parallelism": "1",
    "checkpointing_interval": "60s",
    "checkpointing_timeout": "2min",
    "checkpointing_mode": "EXACTLY_ONCE",
    "log_level": "DEBUG"
}

# Iceberg catalog configuration
ICEBERG_CONFIG = {
    "warehouse": "arn:aws:s3tables:ap-south-1:149815625933:bucket/python-saren",
    "namespace": "sink",
    "format_version": "2",
    "write_format": "parquet",
    "compression_codec": "snappy"
}