"""
Flink Job Base Class

Abstract base class for Flink streaming jobs with common setup and lifecycle.
"""

import logging
from abc import ABC, abstractmethod
from typing import Optional

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

from flink.common.config import FlinkConfig

logger = logging.getLogger(__name__)


class FlinkJobBase(ABC):
    """
    Abstract base class for Flink jobs.
    
    Provides common setup for:
    - Stream execution environment
    - Table environment
    - Checkpointing
    - S3 filesystem configuration
    """

    def __init__(self, job_name: str, config: Optional[FlinkConfig] = None):
        """
        Initialize the Flink job.
        
        Args:
            job_name: Name of the job
            config: Flink configuration (defaults to env-based config)
        """
        self.job_name = job_name
        self.config = config or FlinkConfig.from_env()
        self._env: Optional[StreamExecutionEnvironment] = None
        self._table_env: Optional[StreamTableEnvironment] = None

    @property
    def env(self) -> StreamExecutionEnvironment:
        """Get or create stream execution environment"""
        if self._env is None:
            self._env = self._create_stream_env()
        return self._env

    @property
    def table_env(self) -> StreamTableEnvironment:
        """Get or create table environment"""
        if self._table_env is None:
            self._table_env = self._create_table_env()
        return self._table_env

    def _create_stream_env(self) -> StreamExecutionEnvironment:
        """
        Create and configure stream execution environment.
        
        Returns:
            Configured StreamExecutionEnvironment
        """
        env = StreamExecutionEnvironment.get_execution_environment()
        
        # Set parallelism
        env.set_parallelism(self.config.runtime.parallelism)
        
        # Enable checkpointing
        env.enable_checkpointing(self.config.runtime.checkpoint_interval)
        
        logger.info(
            f"Created stream environment: parallelism={self.config.runtime.parallelism}, "
            f"checkpoint_interval={self.config.runtime.checkpoint_interval}ms"
        )
        
        return env

    def _create_table_env(self) -> StreamTableEnvironment:
        """
        Create and configure table environment.
        
        Returns:
            Configured StreamTableEnvironment
        """
        settings = EnvironmentSettings.new_instance() \
            .in_streaming_mode() \
            .build()
        
        table_env = StreamTableEnvironment.create(self.env, settings)
        
        # Configure S3 for Iceberg
        s3_config = self.config.s3
        table_env.get_config().set("s3.endpoint", s3_config.endpoint_url)
        table_env.get_config().set("s3.access-key", s3_config.access_key)
        table_env.get_config().set("s3.secret-key", s3_config.secret_key)
        table_env.get_config().set("s3.path-style-access", "true")
        
        logger.info("Created table environment with S3 configuration")
        
        return table_env

    @abstractmethod
    def define_pipeline(self) -> None:
        """
        Define the job pipeline.
        
        Subclasses must implement this method to define:
        - Source tables
        - Transformations
        - Sink tables
        """
        pass

    @abstractmethod
    def get_insert_statement(self) -> str:
        """
        Get the INSERT statement for the pipeline.
        
        Returns:
            SQL INSERT statement
        """
        pass

    def run(self) -> None:
        """
        Execute the Flink job.
        
        Sets up the pipeline and starts execution.
        """
        logger.info(f"Starting job: {self.job_name}")
        logger.info(f"Configuration: {self.config.to_dict()}")
        
        # Define the pipeline
        self.define_pipeline()
        
        # Execute the INSERT statement
        insert_sql = self.get_insert_statement()
        logger.info(f"Executing: {insert_sql}")
        
        self.table_env.execute_sql(insert_sql)
        
        logger.info(f"Job {self.job_name} started successfully")

    def stop(self) -> None:
        """Stop the Flink job gracefully"""
        logger.info(f"Stopping job: {self.job_name}")
