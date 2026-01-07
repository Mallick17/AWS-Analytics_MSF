from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("orders") \
    .set_group_id("flink-debug-group") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

ds = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Source"
)

# THIS IS THE KEY LINE
ds.print()

env.execute("kafka-debug-job")