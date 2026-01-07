class KafkaSourceFactory:
    """
    Factory for registering Kafka source tables in Flink.
    """

    def __init__(self, config):
        self.config = config

    def register_source(self, table_env, table_name: str) -> None:
        """
        Register a Kafka source table.

        Args:
            table_env: Flink TableEnvironment
            table_name: Name of the source table
        """
        table_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_id STRING,
            event_time TIMESTAMP(3),
            order_id STRING,
            user_id STRING,
            order_amount DOUBLE,
            currency STRING,
            order_status STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{self.config.kafka_topic}',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'order-ingest-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
        """)
