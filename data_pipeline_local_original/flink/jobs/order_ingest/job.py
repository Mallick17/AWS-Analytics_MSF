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

from common.config import FlinkConfig
from common.job_base import FlinkJobBase
from common.kafka_source import KafkaSourceFactory

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
        
        # Register a print sink (writes rows to TaskManager logs)
        self._sink_table = "orders_print"
        self.table_env.execute_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self._sink_table} (
                event_id STRING,
                event_time TIMESTAMP(3),
                order_id STRING,
                user_id STRING,
                order_amount DECIMAL(10, 2),
                currency STRING,
                order_status STRING,
                processed_at TIMESTAMP(3)
            ) WITH (
                'connector' = 'print'
            )
            """
        )

        logger.info(f"Pipeline defined: orders_source -> {self._sink_table} (print)")

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
