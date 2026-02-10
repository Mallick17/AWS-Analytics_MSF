"""
Flink Configuration Manager

Enhanced configuration loader that supports:
- YAML-based topic and transformation configuration
- Environment-driven settings for cloud portability
- Dynamic Kafka and Iceberg configuration
"""

import os
import yaml
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

load_dotenv()


def is_aws_environment() -> bool:
    """Check if running on AWS Managed Flink"""
    return os.getenv("DEPLOYMENT_ENV") == "aws"


class Config:
    """Enhanced configuration loader for multi-topic processing."""
    
    def __init__(self, config_dir: str = "config"):
        """Initialize configuration loader.
        
        Args:
            config_dir: Directory containing YAML configuration files
        """
        self.config_dir = config_dir
        self._topics_config = None
        self._transformations_config = None
    
    @property
    def topics_config(self) -> Dict[str, Any]:
        """Load topics configuration (lazy loading)."""
        if self._topics_config is None:
            topics_file = os.path.join(self.config_dir, 'topics.yaml')
            with open(topics_file, 'r') as f:
                self._topics_config = yaml.safe_load(f)
        return self._topics_config
    
    @property
    def transformations_config(self) -> Dict[str, Any]:
        """Load transformations configuration (lazy loading)."""
        if self._transformations_config is None:
            trans_file = os.path.join(self.config_dir, 'transformations.yaml')
            with open(trans_file, 'r') as f:
                self._transformations_config = yaml.safe_load(f)
        return self._transformations_config
    
    def get_enabled_topics(self) -> List[str]:
        """Get list of enabled topic names.
        
        Returns:
            List of topic names that are enabled
        """
        topics = self.topics_config.get('topics', {})
        return [
            name for name, config in topics.items()
            if config.get('enabled', False)
        ]
    
    def get_topic_config(self, topic_name: str) -> Dict[str, Any]:
        """Get configuration for a specific topic.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Topic configuration dictionary
        """
        topic_config = self.topics_config.get('topics', {}).get(topic_name, {})
        # Add topic name to config for convenience
        topic_config['name'] = topic_name
        return topic_config
    
    def get_transformation_config(self, transformation_name: str) -> Dict[str, Any]:
        """Get transformation configuration by name.
        
        Args:
            transformation_name: Name of the transformation
            
        Returns:
            Transformation configuration dictionary
        """
        return self.transformations_config.get('transformations', {}).get(transformation_name, {})
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka configuration with environment overrides.
        
        Returns:
            Kafka configuration dictionary
        """
        kafka_config = self.topics_config.get('kafka', {}).copy()
        
        # Override with environment variables if present
        if os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
            kafka_config['bootstrap_servers'] = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        
        # AWS MSK authentication
        if is_aws_environment():
            kafka_config['security'] = {
                'protocol': 'SASL_SSL',
                'sasl_mechanism': 'AWS_MSK_IAM',
                'sasl_jaas_config': 'software.amazon.msk.auth.iam.IAMLoginModule required;',
                'sasl_callback_handler': 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
            }
        
        return kafka_config
    
    def get_iceberg_config(self) -> Dict[str, Any]:
        """Get Iceberg configuration based on environment.
        
        Returns:
            Iceberg configuration dictionary
        """
        if is_aws_environment():
            return {
                'catalog_name': 's3_tables',
                'warehouse': os.getenv('AWS_S3_TABLES_ARN'),
                'region': os.getenv('AWS_REGION', 'ap-south-1'),
                'namespace': os.getenv('ICEBERG_NAMESPACE', 'analytics')
            }
        else:
            return {
                'catalog_name': 'iceberg_catalog',
                'warehouse': os.getenv('ICEBERG_WAREHOUSE', 's3://warehouse'),
                'rest_uri': os.getenv('ICEBERG_REST_URI', 'http://iceberg-rest:8181'),
                'namespace': os.getenv('ICEBERG_NAMESPACE', 'analytics')
            }


@dataclass
class FlinkRuntimeConfig:
    """Flink runtime configuration"""
    parallelism: int = field(
        default_factory=lambda: int(os.getenv("FLINK_PARALLELISM", "1"))
    )
    checkpoint_interval: int = field(
        default_factory=lambda: int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "10000"))
    )
    checkpoint_dir: str = field(
        default_factory=lambda: os.getenv("FLINK_CHECKPOINT_DIR", "file:///opt/flink/checkpoints")
    )
