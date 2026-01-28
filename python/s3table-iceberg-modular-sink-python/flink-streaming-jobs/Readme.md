# Modular PyFlink Architecture for AWS Kinesis Data Analytics

Based on your existing structure, here's a modular design that supports multiple Kafka topics with different transformations to multiple S3 tables.

## Updated Folder Structure

```
python-datastream/
├── application_properties.json
├── assembly/
│   └── assembly.xml
├── package/
│   └── requirements.txt
├── pom.xml
├── src/
│   └── main/
│       └── java/
│           └── org/
│               └── apache/
│                   └── flink/
│                       └── runtime/
│                           └── util/
│                               └── HadoopUtils.java
├── common/
│   ├── __init__.py
│   ├── config.py
│   ├── catalog_manager.py
│   ├── kafka_sources.py
│   ├── iceberg_sinks.py
│   └── utils.py
├── transformations/
│   ├── __init__.py
│   ├── base_transformer.py
│   ├── bid_events_raw.py
│   ├── bid_events_aggregated.py
│   ├── user_events_processed.py
│   ├── order_events_enriched.py
│   └── session_analytics.py
├── config/
│   ├── topics.yaml
│   └── transformations.yaml
├── streaming_job.py  # Main orchestrator
└── sample_gen.py
```

## Implementation

### 1. **config/topics.yaml** - Topic Definitions

```yaml
kafka_topics:
  bid_events:
    topic_name: "bid-events"
    consumer_group: "flink-bid-consumer"
    startup_mode: "earliest-offset"
    schema:
      event_name: STRING
      user_id: BIGINT
      city_id: INT
      platform: STRING
      session_id: STRING
      timestamp_ms: BIGINT
      bid_amount: DOUBLE
  
  user_events:
    topic_name: "user-events"
    consumer_group: "flink-user-consumer"
    startup_mode: "latest-offset"
    schema:
      event_type: STRING
      user_id: BIGINT
      action: STRING
      timestamp_ms: BIGINT
      metadata: STRING
  
  order_events:
    topic_name: "order-events"
    consumer_group: "flink-order-consumer"
    startup_mode: "earliest-offset"
    schema:
      order_id: STRING
      user_id: BIGINT
      product_id: STRING
      quantity: INT
      price: DOUBLE
      timestamp_ms: BIGINT
```

### 2. **config/transformations.yaml** - Transformation Pipeline

```yaml
transformations:
  # Transformation 1: Raw bid events
  - name: "bid_events_raw"
    enabled: true
    source_topic: "bid_events"
    source_table: "kafka_bid_events"
    sink_table: "bid_events_raw"
    transformer_class: "BidEventsRawTransformer"
    description: "Store raw bid events"
    
  # Transformation 2: Aggregated bid metrics
  - name: "bid_events_aggregated"
    enabled: true
    source_topic: "bid_events"
    source_table: "kafka_bid_events"
    sink_table: "bid_events_hourly_agg"
    transformer_class: "BidEventsAggregatedTransformer"
    description: "Hourly aggregated bid metrics"
    
  # Transformation 3: User events processed
  - name: "user_events_processed"
    enabled: true
    source_topic: "user_events"
    source_table: "kafka_user_events"
    sink_table: "user_events_processed"
    transformer_class: "UserEventsProcessedTransformer"
    description: "Processed user activity events"
    
  # Transformation 4: Enriched order events
  - name: "order_events_enriched"
    enabled: true
    source_topic: "order_events"
    source_table: "kafka_order_events"
    sink_table: "order_events_enriched"
    transformer_class: "OrderEventsEnrichedTransformer"
    description: "Orders with derived fields"
    
  # Transformation 5: Session analytics (from bid + user events)
  - name: "session_analytics"
    enabled: true
    source_topic: "bid_events"  # Primary source
    source_table: "kafka_bid_events"
    sink_table: "session_analytics"
    transformer_class: "SessionAnalyticsTransformer"
    description: "Cross-topic session analysis"
    additional_sources:
      - "kafka_user_events"
```

### 3. **common/config.py** - Configuration Manager

```python
import os
import yaml
from typing import Dict, List, Any
from dataclasses import dataclass

@dataclass
class KafkaTopicConfig:
    topic_name: str
    consumer_group: str
    startup_mode: str
    schema: Dict[str, str]

@dataclass
class TransformationConfig:
    name: str
    enabled: bool
    source_topic: str
    source_table: str
    sink_table: str
    transformer_class: str
    description: str
    additional_sources: List[str] = None

class ConfigLoader:
    def __init__(self, config_dir: str = None):
        if config_dir is None:
            self.config_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), 
                'config'
            )
        else:
            self.config_dir = config_dir
        
        self.topics_config = self._load_yaml('topics.yaml')
        self.transformations_config = self._load_yaml('transformations.yaml')
    
    def _load_yaml(self, filename: str) -> Dict:
        filepath = os.path.join(self.config_dir, filename)
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)
    
    def get_kafka_topics(self) -> Dict[str, KafkaTopicConfig]:
        """Get all Kafka topic configurations"""
        topics = {}
        for topic_key, topic_data in self.topics_config['kafka_topics'].items():
            topics[topic_key] = KafkaTopicConfig(
                topic_name=topic_data['topic_name'],
                consumer_group=topic_data['consumer_group'],
                startup_mode=topic_data['startup_mode'],
                schema=topic_data['schema']
            )
        return topics
    
    def get_transformations(self) -> List[TransformationConfig]:
        """Get all enabled transformations"""
        transformations = []
        for tf in self.transformations_config['transformations']:
            if tf.get('enabled', True):
                transformations.append(TransformationConfig(
                    name=tf['name'],
                    enabled=tf['enabled'],
                    source_topic=tf['source_topic'],
                    source_table=tf['source_table'],
                    sink_table=tf['sink_table'],
                    transformer_class=tf['transformer_class'],
                    description=tf['description'],
                    additional_sources=tf.get('additional_sources', [])
                ))
        return transformations
    
    @staticmethod
    def get_msk_bootstrap_servers() -> str:
        return os.getenv(
            "MSK_BOOTSTRAP_SERVERS",
            "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
            "b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
            "b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098"
        )
    
    @staticmethod
    def get_s3_warehouse() -> str:
        return os.getenv(
            "S3_WAREHOUSE",
            "arn:aws:s3tables:ap-south-1:149815625933:bucket/python-saren"
        )
    
    @staticmethod
    def get_iceberg_namespace() -> str:
        return os.getenv("ICEBERG_NAMESPACE", "sink")
```

### 4. **common/kafka_sources.py** - Kafka Source Factory

```python
from pyflink.table import TableEnvironment
from typing import Dict
from common.config import KafkaTopicConfig

class KafkaSourceFactory:
    def __init__(self, table_env: TableEnvironment, bootstrap_servers: str):
        self.table_env = table_env
        self.bootstrap_servers = bootstrap_servers
    
    def create_source(self, table_name: str, topic_config: KafkaTopicConfig):
        """Create Kafka source table from configuration"""
        
        # Switch to default catalog
        self.table_env.use_catalog("default_catalog")
        self.table_env.use_database("default_database")
        
        # Build schema DDL
        schema_ddl = ",\n            ".join([
            f"{col_name} {col_type}" 
            for col_name, col_type in topic_config.schema.items()
        ])
        
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {schema_ddl}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic_config.topic_name}',
                'properties.bootstrap.servers' = '{self.bootstrap_servers}',
                'properties.group.id' = '{topic_config.consumer_group}',
                'scan.startup.mode' = '{topic_config.startup_mode}',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'AWS_MSK_IAM',
                'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
                'properties.sasl.client.callback.handler.class' = 
                    'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
            )
        """
        
        self.table_env.execute_sql(ddl)
        print(f"✓ Created Kafka source: {table_name} <- {topic_config.topic_name}")
```

### 5. **common/iceberg_sinks.py** - Iceberg Sink Factory

```python
from pyflink.table import TableEnvironment
from typing import Dict, List, Optional

class IcebergSinkFactory:
    def __init__(self, table_env: TableEnvironment, catalog_name: str, namespace: str):
        self.table_env = table_env
        self.catalog_name = catalog_name
        self.namespace = namespace
    
    def create_sink(
        self, 
        table_name: str, 
        schema: Dict[str, str],
        partition_by: Optional[List[str]] = None
    ):
        """Create Iceberg sink table"""
        
        # Switch to Iceberg catalog
        self.table_env.use_catalog(self.catalog_name)
        self.table_env.use_database(self.namespace)
        
        schema_ddl = ",\n            ".join([
            f"{col_name} {col_type}" 
            for col_name, col_type in schema.items()
        ])
        
        partition_clause = ""
        if partition_by:
            partition_clause = f"PARTITIONED BY ({', '.join(partition_by)})"
        
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {schema_ddl}
            ) {partition_clause}
            WITH (
                'format-version' = '2',
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """
        
        self.table_env.execute_sql(ddl)
        print(f"✓ Created Iceberg sink: {table_name}")
```

### 6. **common/catalog_manager.py** - Catalog Setup

```python
from pyflink.table import TableEnvironment

class CatalogManager:
    def __init__(self, table_env: TableEnvironment, warehouse_arn: str, namespace: str):
        self.table_env = table_env
        self.warehouse_arn = warehouse_arn
        self.namespace = namespace
        self.catalog_name = "s3_tables"
    
    def setup(self):
        """Initialize Iceberg catalog"""
        print(f"Setting up Iceberg catalog...")
        
        self.table_env.execute_sql(f"""
            CREATE CATALOG {self.catalog_name} WITH (
                'type' = 'iceberg',
                'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
                'warehouse' = '{self.warehouse_arn}'
            )
        """)
        
        self.table_env.use_catalog(self.catalog_name)
        self.table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {self.namespace}")
        self.table_env.use_database(self.namespace)
        
        print(f"✓ Catalog ready: {self.catalog_name}.{self.namespace}")
        
        return self.catalog_name
```

### 7. **common/utils.py** - Utilities

```python
import os
from pyflink.java_gateway import get_gateway
from pyflink.table import EnvironmentSettings, TableEnvironment

def inject_dependencies():
    """Inject JAR dependencies"""
    gateway = get_gateway()
    jvm = gateway.jvm
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    jar_file = os.path.join(base_dir, "lib", "pyflink-dependencies.jar")
    
    if not os.path.exists(jar_file):
        raise RuntimeError(f"Dependency JAR not found: {jar_file}")
    
    jar_url = jvm.java.net.URL(f"file://{jar_file}")
    jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)
    print(f"✓ Injected JAR: {jar_file}")

def create_table_environment():
    """Create and configure Flink TableEnvironment"""
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    
    config = table_env.get_config().get_configuration()
    config.set_string("table.exec.resource.default-parallelism", "1")
    config.set_string("execution.checkpointing.interval", "60s")
    config.set_string("execution.checkpointing.timeout", "2min")
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    
    print("✓ TableEnvironment created")
    return table_env
```

### 8. **transformations/base_transformer.py** - Base Class

```python
from abc import ABC, abstractmethod
from pyflink.table import TableEnvironment
from typing import Dict

class BaseTransformer(ABC):
    def __init__(self, table_env: TableEnvironment):
        self.table_env = table_env
    
    @abstractmethod
    def get_sink_schema(self) -> Dict[str, str]:
        """Define output schema"""
        pass
    
    @abstractmethod
    def get_transformation_sql(self, source_table: str, sink_table: str) -> str:
        """Define transformation logic"""
        pass
    
    def get_partition_by(self):
        """Optional: Define partitioning strategy"""
        return None
    
    def execute(self, source_table: str, sink_table: str):
        """Execute the transformation"""
        sql = self.get_transformation_sql(source_table, sink_table)
        print(f"Executing: {self.__class__.__name__}")
        return self.table_env.execute_sql(sql)
```

### 9. **transformations/bid_events_raw.py** - Transformation 1

```python
from transformations.base_transformer import BaseTransformer
from typing import Dict

class BidEventsRawTransformer(BaseTransformer):
    def get_sink_schema(self) -> Dict[str, str]:
        return {
            "event_name": "STRING",
            "user_id": "BIGINT",
            "city_id": "INT",
            "platform": "STRING",
            "session_id": "STRING",
            "bid_amount": "DOUBLE",
            "event_time": "TIMESTAMP(3)",
            "ingestion_time": "TIMESTAMP(3)"
        }
    
    def get_partition_by(self):
        return ["DATE(event_time)"]
    
    def get_transformation_sql(self, source_table: str, sink_table: str) -> str:
        return f"""
            INSERT INTO {sink_table}
            SELECT
                event_name,
                user_id,
                city_id,
                platform,
                session_id,
                bid_amount,
                TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS event_time,
                CURRENT_TIMESTAMP AS ingestion_time
            FROM default_catalog.default_database.{source_table}
        """
```

### 10. **transformations/bid_events_aggregated.py** - Transformation 2

```python
from transformations.base_transformer import BaseTransformer
from typing import Dict

class BidEventsAggregatedTransformer(BaseTransformer):
    def get_sink_schema(self) -> Dict[str, str]:
        return {
            "window_start": "TIMESTAMP(3)",
            "window_end": "TIMESTAMP(3)",
            "city_id": "INT",
            "platform": "STRING",
            "total_bids": "BIGINT",
            "unique_users": "BIGINT",
            "avg_bid_amount": "DOUBLE",
            "max_bid_amount": "DOUBLE",
            "min_bid_amount": "DOUBLE"
        }
    
    def get_partition_by(self):
        return ["DATE(window_start)"]
    
    def get_transformation_sql(self, source_table: str, sink_table: str) -> str:
        return f"""
            INSERT INTO {sink_table}
            SELECT
                TUMBLE_START(TO_TIMESTAMP_LTZ(timestamp_ms, 3), INTERVAL '1' HOUR) AS window_start,
                TUMBLE_END(TO_TIMESTAMP_LTZ(timestamp_ms, 3), INTERVAL '1' HOUR) AS window_end,
                city_id,
                platform,
                COUNT(*) AS total_bids,
                COUNT(DISTINCT user_id) AS unique_users,
                AVG(bid_amount) AS avg_bid_amount,
                MAX(bid_amount) AS max_bid_amount,
                MIN(bid_amount) AS min_bid_amount
            FROM default_catalog.default_database.{source_table}
            GROUP BY
                TUMBLE(TO_TIMESTAMP_LTZ(timestamp_ms, 3), INTERVAL '1' HOUR),
                city_id,
                platform
        """
```

### 11. **transformations/user_events_processed.py** - Transformation 3

```python
from transformations.base_transformer import BaseTransformer
from typing import Dict

class UserEventsProcessedTransformer(BaseTransformer):
    def get_sink_schema(self) -> Dict[str, str]:
        return {
            "event_type": "STRING",
            "user_id": "BIGINT",
            "action": "STRING",
            "event_time": "TIMESTAMP(3)",
            "event_date": "DATE",
            "event_hour": "INT",
            "metadata": "STRING",
            "ingestion_time": "TIMESTAMP(3)"
        }
    
    def get_partition_by(self):
        return ["event_date"]
    
    def get_transformation_sql(self, source_table: str, sink_table: str) -> str:
        return f"""
            INSERT INTO {sink_table}
            SELECT
                event_type,
                user_id,
                action,
                TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS event_time,
                DATE(TO_TIMESTAMP_LTZ(timestamp_ms, 3)) AS event_date,
                HOUR(TO_TIMESTAMP_LTZ(timestamp_ms, 3)) AS event_hour,
                metadata,
                CURRENT_TIMESTAMP AS ingestion_time
            FROM default_catalog.default_database.{source_table}
        """
```

### 12. **transformations/order_events_enriched.py** - Transformation 4

```python
from transformations.base_transformer import BaseTransformer
from typing import Dict

class OrderEventsEnrichedTransformer(BaseTransformer):
    def get_sink_schema(self) -> Dict[str, str]:
        return {
            "order_id": "STRING",
            "user_id": "BIGINT",
            "product_id": "STRING",
            "quantity": "INT",
            "price": "DOUBLE",
            "total_amount": "DOUBLE",
            "order_time": "TIMESTAMP(3)",
            "order_date": "DATE",
            "price_category": "STRING",
            "ingestion_time": "TIMESTAMP(3)"
        }
    
    def get_partition_by(self):
        return ["order_date"]
    
    def get_transformation_sql(self, source_table: str, sink_table: str) -> str:
        return f"""
            INSERT INTO {sink_table}
            SELECT
                order_id,
                user_id,
                product_id,
                quantity,
                price,
                quantity * price AS total_amount,
                TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS order_time,
                DATE(TO_TIMESTAMP_LTZ(timestamp_ms, 3)) AS order_date,
                CASE
                    WHEN price < 100 THEN 'LOW'
                    WHEN price >= 100 AND price < 500 THEN 'MEDIUM'
                    ELSE 'HIGH'
                END AS price_category,
                CURRENT_TIMESTAMP AS ingestion_time
            FROM default_catalog.default_database.{source_table}
        """
```

### 13. **transformations/session_analytics.py** - Transformation 5 (Multi-source)

```python
from transformations.base_transformer import BaseTransformer
from typing import Dict

class SessionAnalyticsTransformer(BaseTransformer):
    def get_sink_schema(self) -> Dict[str, str]:
        return {
            "session_id": "STRING",
            "user_id": "BIGINT",
            "session_start": "TIMESTAMP(3)",
            "total_bids": "BIGINT",
            "total_events": "BIGINT",
            "platforms_used": "STRING",
            "cities_accessed": "STRING"
        }
    
    def get_partition_by(self):
        return ["DATE(session_start)"]
    
    def get_transformation_sql(self, source_table: str, sink_table: str) -> str:
        # This is a simplified version - you might need temporal joins
        return f"""
            INSERT INTO {sink_table}
            SELECT
                session_id,
                user_id,
                MIN(TO_TIMESTAMP_LTZ(timestamp_ms, 3)) AS session_start,
                COUNT(*) AS total_bids,
                0 AS total_events,
                LISTAGG(DISTINCT platform, ',') AS platforms_used,
                LISTAGG(DISTINCT CAST(city_id AS STRING), ',') AS cities_accessed
            FROM default_catalog.default_database.{source_table}
            GROUP BY session_id, user_id
        """
```

### 14. **streaming_job.py** - Main Orchestrator

```python
import sys
import time
import importlib

# Redirect logs
sys.stdout = sys.stderr

print("=" * 80)
print("STARTING MODULAR PYFLINK STREAMING JOB")
print("=" * 80)

try:
    from pyflink.table import EnvironmentSettings, TableEnvironment
    
    from common.config import ConfigLoader
    from common.utils import inject_dependencies, create_table_environment
    from common.catalog_manager import CatalogManager
    from common.kafka_sources import KafkaSourceFactory
    from common.iceberg_sinks import IcebergSinkFactory
    
    print("✓ Imports successful")
    
    # Step 1: Load configurations
    config_loader = ConfigLoader()
    kafka_topics = config_loader.get_kafka_topics()
    transformations = config_loader.get_transformations()
    bootstrap_servers = config_loader.get_msk_bootstrap_servers()
    s3_warehouse = config_loader.get_s3_warehouse()
    namespace = config_loader.get_iceberg_namespace()
    
    print(f"✓ Loaded {len(kafka_topics)} topics, {len(transformations)} transformations")
    
    # Step 2: Setup environment
    inject_dependencies()
    table_env = create_table_environment()
    
    # Step 3: Setup Iceberg catalog
    catalog_mgr = CatalogManager(table_env, s3_warehouse, namespace)
    catalog_name = catalog_mgr.setup()
    
    # Step 4: Create Kafka sources for all topics
    kafka_factory = KafkaSourceFactory(table_env, bootstrap_servers)
    created_sources = set()
    
    for topic_key, topic_config in kafka_topics.items():
        source_table = f"kafka_{topic_key}"
        kafka_factory.create_source(source_table, topic_config)
        created_sources.add(source_table)
    
    # Step 5: Create Iceberg sinks and execute transformations
    iceberg_factory = IcebergSinkFactory(table_env, catalog_name, namespace)
    
    job_handles = []
    
    for tf_config in transformations:
        print(f"\n{'=' * 60}")
        print(f"Processing: {tf_config.name}")
        print(f"Description: {tf_config.description}")
        print(f"{'=' * 60}")
        
        # Dynamically load transformer class
        module_name = f"transformations.{tf_config.name}"
        module = importlib.import_module(module_name)
        transformer_class = getattr(module, tf_config.transformer_class)
        transformer = transformer_class(table_env)
        
        # Create sink table
        sink_schema = transformer.get_sink_schema()
        partition_by = transformer.get_partition_by()
        
        iceberg_factory.create_sink(
            table_name=tf_config.sink_table,
            schema=sink_schema,
            partition_by=partition_by
        )
        
        # Execute transformation (non-blocking)
        job_handle = transformer.execute(
            source_table=tf_config.source_table,
            sink_table=tf_config.sink_table
        )
        
        job_handles.append({
            'name': tf_config.name,
            'handle': job_handle
        })
        
        print(f"✓ Started: {tf_config.name} -> {tf_config.sink_table}")
    
    print("\n" + "=" * 80)
    print(f"✓ ALL {len(transformations)} TRANSFORMATIONS SUBMITTED SUCCESSFULLY")
    print("=" * 80)
    
    # Keep alive
    print("\nJob is running... (press Ctrl+C to stop)")
    while True:
        time.sleep(60)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Jobs running: {len(job_handles)}")

except KeyboardInterrupt:
    print("\n\nShutting down gracefully...")
    sys.exit(0)

except Exception as e:
    print("\n" + "=" * 80)
    print("FATAL ERROR")
    print("=" * 80)
    import traceback
    traceback.print_exc()
    print("=" * 80)
    raise
```

## Usage

### Adding a New Transformation

1. **Add topic to `config/topics.yaml`** (if new)
2. **Add transformation to `config/transformations.yaml`**:
```yaml
  - name: "new_transformation"
    enabled: true
    source_topic: "bid_events"
    source_table: "kafka_bid_events"
    sink_table: "new_output_table"
    transformer_class: "NewTransformer"
    description: "My new transformation"
```

3. **Create transformer**: `transformations/new_transformation.py`
```python
from transformations.base_transformer import BaseTransformer

class NewTransformer(BaseTransformer):
    def get_sink_schema(self):
        return {"col1": "STRING", "col2": "BIGINT"}
    
    def get_transformation_sql(self, source_table, sink_table):
        return f"INSERT INTO {sink_table} SELECT * FROM ..."
```

4. **Deploy**: Run `streaming_job.py`

### Benefits

✅ **Single deployment** - One job handles all transformations  
✅ **Dynamic configuration** - Add transformations without code changes  
✅ **Shared resources** - Reuse Kafka sources across transformations  
✅ **Easy testing** - Test transformations independently  
✅ **AWS KDA compatible** - Works with Kinesis Data Analytics  

This architecture allows you to scale from 3 topics/5 transformations to 10+ topics/20+ transformations easily!