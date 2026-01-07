import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

logger = logging.getLogger(__name__)


class FlinkJobBase:
    """
    Base class for Flink jobs.

    Handles:
    - Environment creation
    - Table environment setup
    - Standard job execution flow
    """

    def __init__(self, job_name: str, config):
        self.job_name = job_name
        self.config = config

        # Stream environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(1)

        # Table environment
        self.table_env = StreamTableEnvironment.create(self.env)

    def define_pipeline(self) -> None:
        """Define sources, catalogs, and tables."""
        raise NotImplementedError

    def get_insert_statement(self) -> str:
        """Return the INSERT SQL for the pipeline."""
        raise NotImplementedError

    def run(self) -> None:
        """
        Execute the Flink job.
        """
        logger.info(f"Starting job: {self.job_name}")

        self.define_pipeline()
        insert_sql = self.get_insert_statement()

        logger.info("Executing insert statement")
        self.table_env.execute_sql(insert_sql)
