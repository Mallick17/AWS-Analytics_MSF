import os
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class KafkaConfig:
    bootstrap_servers: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "AWS_MSK_IAM"
    sasl_jaas_config: str = "software.amazon.msk.auth.iam.IAMLoginModule required;"
    sasl_callback_handler: str = "software.amazon.msk.auth.iam.IAMClientCallbackHandler"

@dataclass
class IcebergConfig:
    warehouse_arn: str
    namespace: str
    catalog_name: str = "s3_tables"

@dataclass
class FlinkConfig:
    parallelism: int = 1
    checkpoint_interval: str = "60s"
    checkpoint_timeout: str = "2min"
    checkpoint_mode: str = "EXACTLY_ONCE"

class ConfigManager:
    @staticmethod
    def get_msk_config() -> KafkaConfig:
        return KafkaConfig(
            bootstrap_servers=os.getenv(
                "MSK_BOOTSTRAP_SERVERS",
                "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
                "b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
                "b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098"
            )
        )
    
    @staticmethod
    def get_iceberg_config() -> IcebergConfig:
        return IcebergConfig(
            warehouse_arn=os.getenv(
                "S3_WAREHOUSE",
                "arn:aws:s3tables:ap-south-1:149815625933:bucket/python-saren"
            ),
            namespace=os.getenv("ICEBERG_NAMESPACE", "sink")
        )
    
    @staticmethod
    def get_flink_config() -> FlinkConfig:
        return FlinkConfig(
            parallelism=int(os.getenv("FLINK_PARALLELISM", "1")),
            checkpoint_interval=os.getenv("CHECKPOINT_INTERVAL", "60s")
        )