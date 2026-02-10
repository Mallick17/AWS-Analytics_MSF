"""
Kafka Client Manager
Handles connections to local and MSK Kafka clusters
"""

import json
import os
from typing import List, Dict, Any
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


class KafkaClientManager:
    """Manages Kafka producer and admin client connections"""
    
    def __init__(self, is_local: bool = True):
        self.is_local = is_local
        self._producer = None
        self._admin = None
    
    def get_bootstrap_servers(self) -> List[str]:
        """Get bootstrap servers based on mode"""
        if self.is_local:
            servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            return servers.split(',') if isinstance(servers, str) else [servers]
        else:
            # MSK servers
            return [
                'b-1.rttestinganalyticsmsk.mbenee.c4.kafka.ap-south-1.amazonaws.com:9098',
                'b-2.rttestinganalyticsmsk.mbenee.c4.kafka.ap-south-1.amazonaws.com:9098'
            ]
    
    def get_producer(self) -> KafkaProducer:
        """Get or create Kafka producer"""
        if self._producer is None:
            bootstrap_servers = self.get_bootstrap_servers()
            
            if self.is_local:
                print(f"📍 Connecting to LOCAL Kafka: {bootstrap_servers}")
                self._producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
            else:
                print(f"☁️  Connecting to AWS MSK: {bootstrap_servers[0]}...")
                # Import MSK auth only when needed
                from kafka.sasl.oauth import AbstractTokenProvider
                from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
                
                class MSKTokenProvider(AbstractTokenProvider):
                    def token(self):
                        token, _ = MSKAuthTokenProvider.generate_auth_token(
                            os.getenv('AWS_REGION', 'ap-south-1')
                        )
                        return token
                
                self._producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    security_protocol='SASL_SSL',
                    sasl_mechanism='OAUTHBEARER',
                    sasl_oauth_token_provider=MSKTokenProvider(),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
        
        return self._producer
    
    def get_admin_client(self) -> KafkaAdminClient:
        """Get or create Kafka admin client"""
        if self._admin is None:
            bootstrap_servers = self.get_bootstrap_servers()
            
            if self.is_local:
                self._admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            else:
                # Import MSK auth only when needed
                from kafka.sasl.oauth import AbstractTokenProvider
                from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
                
                class MSKTokenProvider(AbstractTokenProvider):
                    def token(self):
                        token, _ = MSKAuthTokenProvider.generate_auth_token(
                            os.getenv('AWS_REGION', 'ap-south-1')
                        )
                        return token
                
                self._admin = KafkaAdminClient(
                    bootstrap_servers=bootstrap_servers,
                    security_protocol='SASL_SSL',
                    sasl_mechanism='OAUTHBEARER',
                    sasl_oauth_token_provider=MSKTokenProvider()
                )
        
        return self._admin
    
    def create_topic_if_not_exists(self, topic_name: str):
        """Create Kafka topic if it doesn't exist"""
        try:
            admin = self.get_admin_client()
            replication = 1 if self.is_local else 2
            topic = NewTopic(
                name=topic_name, 
                num_partitions=3, 
                replication_factor=replication
            )
            admin.create_topics([topic])
            print(f"✓ Topic '{topic_name}' created")
        except Exception as e:
            error_str = str(e).lower()
            if "already exists" in error_str or "topicexistsexception" in error_str:
                print(f"✓ Topic '{topic_name}' already exists")
            else:
                print(f"⚠ Topic creation error: {e}")
    
    def close(self):
        """Close producer and admin clients"""
        if self._producer:
            self._producer.flush()
            self._producer.close()
            self._producer = None
        
        if self._admin:
            self._admin.close()
            self._admin = None
