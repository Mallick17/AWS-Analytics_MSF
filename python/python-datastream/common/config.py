import os
import yaml
from typing import Dict, List, Any
from dataclasses import dataclass

@dataclass
class KafkaTopicConfig:
    topic_name: str
    consumer_group: str
    startup_mode: str
    schema: Dict[str, str]

@dataclass
class TransformationConfig:
    name: str
    enabled: bool
    source_topic: str
    source_table: str
    sink_table: str
    transformer_class: str
    description: str
    additional_sources: List[str] = None

class ConfigLoader:
    def __init__(self, config_dir: str = None):
        if config_dir is None:
            self.config_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), 
                'config'
            )
        else:
            self.config_dir = config_dir
        
        self.topics_config = self._load_yaml('topics.yaml')
        self.transformations_config = self._load_yaml('transformations.yaml')
    
    def _load_yaml(self, filename: str) -> Dict:
        filepath = os.path.join(self.config_dir, filename)
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)
    
    def get_kafka_topics(self) -> Dict[str, KafkaTopicConfig]:
        """Get all Kafka topic configurations"""
        topics = {}
        for topic_key, topic_data in self.topics_config['kafka_topics'].items():
            topics[topic_key] = KafkaTopicConfig(
                topic_name=topic_data['topic_name'],
                consumer_group=topic_data['consumer_group'],
                startup_mode=topic_data['startup_mode'],
                schema=topic_data['schema']
            )
        return topics
    
    def get_transformations(self) -> List[TransformationConfig]:
        """Get all enabled transformations"""
        transformations = []
        for tf in self.transformations_config['transformations']:
            if tf.get('enabled', True):
                transformations.append(TransformationConfig(
                    name=tf['name'],
                    enabled=tf['enabled'],
                    source_topic=tf['source_topic'],
                    source_table=tf['source_table'],
                    sink_table=tf['sink_table'],
                    transformer_class=tf['transformer_class'],
                    description=tf['description'],
                    additional_sources=tf.get('additional_sources', [])
                ))
        return transformations
    
    @staticmethod
    def get_msk_bootstrap_servers() -> str:
        return os.getenv(
            "MSK_BOOTSTRAP_SERVERS",
            "b-2.rttestinganalyticsmsk.mbenee.c4.kafka.ap-south-1.amazonaws.com:9098,"
            "b-1.rttestinganalyticsmsk.mbenee.c4.kafka.ap-south-1.amazonaws.com:9098"
        )
    
    @staticmethod
    def get_s3_warehouse() -> str:
        return os.getenv(
            "S3_WAREHOUSE",
            "arn:aws:s3tables:ap-south-1:508351649560:bucket/rt-testing-cdc-bucket"
        )
    
    @staticmethod
    def get_iceberg_namespace() -> str:
        return os.getenv("ICEBERG_NAMESPACE", "analytics")
