class FlinkConfig:
    """
    Central configuration object for Flink jobs.
    Loaded from environment (extendable later).
    """

    def __init__(self, kafka_topic: str = "orders"):
        self.kafka_topic = kafka_topic

    @classmethod
    def from_env(cls):
        """
        Load configuration from environment variables.
        Defaults are safe for local Docker execution.
        """
        return cls()

    def to_dict(self) -> dict:
        return {
            "kafka_topic": self.kafka_topic
        }
