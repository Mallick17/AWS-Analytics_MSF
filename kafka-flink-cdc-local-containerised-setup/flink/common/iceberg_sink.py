import logging

logger = logging.getLogger(__name__)


class IcebergSinkFactory:
    """
    Iceberg sink using local filesystem catalog.
    No REST, no Hadoop, no AWS.
    """

    def __init__(self, config):
        self.config = config

    def register_catalog(self, table_env) -> None:
        logger.info("Registering Iceberg filesystem catalog")

        try:
            table_env.execute_sql("""
            CREATE CATALOG iceberg_catalog WITH (
              'type' = 'iceberg',
              'catalog-type' = 'filesystem',
              'warehouse' = 'file:///iceberg/warehouse'
            )
            """)
        except Exception as e:
            # Catalog already exists → safe to ignore
            logger.info("Iceberg catalog already exists, continuing...")

        table_env.execute_sql("USE CATALOG iceberg_catalog")

    def register_orders_table(self, table_env, table_name: str) -> str:
        full_table_name = f"default.{table_name}"

        logger.info(f"Registering Iceberg table: {full_table_name}")

        table_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            event_id STRING,
            event_time TIMESTAMP(3),
            order_id STRING,
            user_id STRING,
            order_amount DOUBLE,
            currency STRING,
            order_status STRING,
            processed_at TIMESTAMP(3)
        ) WITH (
            'format-version' = '2'
        )
        """)

        return full_table_name