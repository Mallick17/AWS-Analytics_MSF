import logging
import sys
from typing import Optional

from common.config import FlinkConfig
from common.job_base import FlinkJobBase
from common.kafka_source import KafkaSourceFactory
from common.iceberg_sink import IcebergSinkFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OrderIngestJob(FlinkJobBase):
    def __init__(self, config: Optional[FlinkConfig] = None):
        super().__init__("order_ingest_job", config)
        self.kafka_source = KafkaSourceFactory(self.config)
        self.iceberg_sink = IcebergSinkFactory(self.config)
        self._sink_table = None

    def define_pipeline(self):
        self.kafka_source.register_source(self.table_env, "orders_source")
        self.iceberg_sink.register_catalog(self.table_env)
        self._sink_table = self.iceberg_sink.register_orders_table(
            self.table_env, "orders"
        )

    def get_insert_statement(self) -> str:
        return f"""
        INSERT INTO {self._sink_table}
        SELECT
            event_id,
            event_time,
            order_id,
            user_id,
            order_amount,
            currency,
            order_status,
            CURRENT_TIMESTAMP
        FROM orders_source
        """


def main():
    try:
        config = FlinkConfig.from_env()
        job = OrderIngestJob(config)
        job.run()
    except Exception as e:
        logger.error(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
