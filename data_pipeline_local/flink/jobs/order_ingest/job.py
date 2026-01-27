#!/usr/bin/env python3
"""
Order Ingest Job

Flink job that:
1. Consumes order creation events from Kafka
2. Validates and deserializes events
3. Adds processed_at timestamp
4. Writes to Iceberg table in S3

This is a reference ingestion pipeline with minimal transformation.
"""

import logging
import sys
from typing import Optional

from flink.common.config import FlinkConfig
from flink.common.job_base import FlinkJobBase
from flink.common.kafka_source import KafkaSourceFactory
from flink.common.iceberg_sink import IcebergSinkFactory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class OrderIngestJob(FlinkJobBase):
    """
    Order ingestion job.
    
    Consumes order creation events from Kafka and writes to Iceberg.
    Adds minimal transformation (processed_at timestamp).
    """

    def __init__(self, config: Optional[FlinkConfig] = None):
        """
        Initialize the order ingest job.
        
        Args:
            config: Flink configuration (defaults to env-based config)
        """
        super().__init__(job_name="order_ingest_job", config=config)
        self.kafka_source = KafkaSourceFactory(self.config)
        self.iceberg_sink = IcebergSinkFactory(self.config)
        self._sink_table: Optional[str] = None

    def define_pipeline(self) -> None:
        """
        Define the order ingestion pipeline.
        
        Sets up:
        1. Kafka source table for order events
        2. Iceberg catalog and sink table
        """
        logger.info("Defining order ingest pipeline...")
        
        # Register Kafka source
        self.kafka_source.register_source(self.table_env, "orders_source")
        
        # Register Iceberg catalog and table
        self.iceberg_sink.register_catalog(self.table_env)
        self._sink_table = self.iceberg_sink.register_orders_table(
            self.table_env, 
            "orders"
        )
        
        logger.info(f"Pipeline defined: orders_source -> {self._sink_table}")

    def get_insert_statement(self) -> str:
        """
        Get the INSERT statement for the pipeline.
        
        Transforms:
        - Passes through all source fields
        - Adds processed_at timestamp
        
        Returns:
            SQL INSERT statement
        """
        if self._sink_table is None:
            raise RuntimeError("Pipeline not defined. Call define_pipeline() first.")
        
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
            CURRENT_TIMESTAMP as processed_at
        FROM orders_source
        """


def main():
    """Main entry point for the job"""
    logger.info("Starting Order Ingest Job...")
    
    try:
        config = FlinkConfig.from_env()
        logger.info(f"Configuration loaded: {config.to_dict()}")
        
        job = OrderIngestJob(config)
        job.run()
        
    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
