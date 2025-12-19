package com.amazonaws.services.msf;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Flink Streaming Job that integrates with Python FastAPI for business logic
 * Reads from Kafka -> Calls Python API for processing -> Writes to S3 Tables (Iceberg)
 */
public class StreamingJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
            .withZone(ZoneId.systemDefault());

    // ============ EMBEDDED CONFIGURATION ============
    // You can override these via Runtime Properties in Managed Flink
    private static final Map<String, String> DEFAULT_CONFIG = new HashMap<String, String>() {{
        // Kafka Configuration
        put("kafka.bootstrap.servers", 
            "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098," +
            "b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098," +
            "b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098");
        put("kafka.topics", "user_events");
        put("kafka.consumer.group", "flink-s3-tables-python-logic");
        put("kafka.offset", "earliest");
        
        // S3 Tables Configuration
        put("s3.warehouse", "arn:aws:s3tables:ap-south-1:149815625933:bucket/flink-transform-sink");
        put("table.namespace", "sink");
        
        // Python FastAPI Configuration
        put("python.api.url", "http://your-fastapi-service:8000/process");
        put("python.api.timeout.ms", "5000");
        put("python.api.retry.attempts", "3");
        
        // Flink Configuration
        put("parallelism", "2");
        put("checkpoint.interval", "60000");
        
        // Feature Flags
        put("python.logic.enabled", "true");  // Set to false to bypass Python logic
    }};

    public static void main(String[] args) throws Exception {
        
        // Load properties (Runtime Properties override defaults)
        Properties appProperties = loadApplicationProperties(args);
        
        // Get configuration with fallback to defaults
        String bootstrapServers = getProperty(appProperties, "kafka.bootstrap.servers");
        String[] kafkaTopics = getProperty(appProperties, "kafka.topics").split(",");
        String consumerGroup = getProperty(appProperties, "kafka.consumer.group");
        String startOffset = getProperty(appProperties, "kafka.offset");
        String s3Warehouse = getProperty(appProperties, "s3.warehouse");
        String namespace = getProperty(appProperties, "table.namespace");
        
        String pythonApiUrl = getProperty(appProperties, "python.api.url");
        int pythonApiTimeout = Integer.parseInt(getProperty(appProperties, "python.api.timeout.ms"));
        int pythonRetryAttempts = Integer.parseInt(getProperty(appProperties, "python.api.retry.attempts"));
        boolean pythonLogicEnabled = Boolean.parseBoolean(getProperty(appProperties, "python.logic.enabled"));
        
        int parallelism = Integer.parseInt(getProperty(appProperties, "parallelism"));
        long checkpointInterval = Long.parseLong(getProperty(appProperties, "checkpoint.interval"));
        
        LOG.info("=== Configuration ===");
        LOG.info("Kafka Topics: {}", String.join(",", kafkaTopics));
        LOG.info("Consumer Group: {}", consumerGroup);
        LOG.info("Starting Offset: {}", startOffset);
        LOG.info("Namespace: {}", namespace);
        LOG.info("S3 Warehouse: {}", s3Warehouse);
        LOG.info("Python API URL: {}", pythonApiUrl);
        LOG.info("Python Logic Enabled: {}", pythonLogicEnabled);
        LOG.info("Parallelism: {}", parallelism);
        LOG.info("=====================");
        
        // Setup Flink Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointInterval / 2);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        LOG.info("Starting MSK to S3 Tables with Python Logic Integration");

        // S3 Tables Catalog Setup (without Hadoop)
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.CATALOG_IMPL, "software.amazon.s3tables.iceberg.S3TablesCatalog");
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, s3Warehouse);
        
        CatalogLoader catalogLoader = CatalogLoader.custom(
            "s3_tables",
            catalogProps,
            "software.amazon.s3tables.iceberg.S3TablesCatalog"
        );

        // Process each topic
        for (String kafkaTopic : kafkaTopics) {
            kafkaTopic = kafkaTopic.trim();
            String tableName = kafkaTopic.replaceAll("[-.]", "_").toLowerCase();
            
            LOG.info("Processing topic: {} -> table: {}.{}", kafkaTopic, namespace, tableName);
            
            processTopicToTable(
                env, 
                bootstrapServers, 
                kafkaTopic, 
                consumerGroup, 
                startOffset,
                catalogLoader,
                namespace,
                tableName,
                pythonApiUrl,
                pythonApiTimeout,
                pythonRetryAttempts,
                pythonLogicEnabled
            );
        }

        env.execute("MSK to S3 Tables with Python Logic");
    }
    
    /**
     * Process a single Kafka topic with Python logic integration
     */
    private static void processTopicToTable(
        StreamExecutionEnvironment env,
        String bootstrapServers,
        String kafkaTopic,
        String consumerGroup,
        String startOffset,
        CatalogLoader catalogLoader,
        String namespace,
        String tableName,
        String pythonApiUrl,
        int pythonApiTimeout,
        int pythonRetryAttempts,
        boolean pythonLogicEnabled
    ) throws Exception {
        
        // Kafka Source Configuration
        OffsetsInitializer offsetInit = startOffset.equalsIgnoreCase("earliest") 
            ? OffsetsInitializer.earliest() 
            : OffsetsInitializer.latest();
        
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(kafkaTopic)
            .setGroupId(consumerGroup)
            .setStartingOffsets(offsetInit)
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperty("security.protocol", "SASL_SSL")
            .setProperty("sasl.mechanism", "AWS_MSK_IAM")
            .setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
            .setProperty("sasl.client.callback.handler.class", 
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
            .build();

        DataStream<String> kafkaStream = env.fromSource(
            source, 
            WatermarkStrategy.noWatermarks(), 
            "Kafka Source - " + kafkaTopic
        );

        // Parse JSON from Kafka
        DataStream<EventRecord> parsedStream = kafkaStream
            .map(json -> {
                try {
                    return parseEventRecord(json);
                } catch (Exception e) {
                    LOG.error("Failed to parse from topic {}: {}", kafkaTopic, json, e);
                    return null;
                }
            })
            .filter(record -> record != null)
            .name("Parse JSON - " + kafkaTopic);

        // Apply Python Logic if enabled
        DataStream<EventRecord> processedStream;
        if (pythonLogicEnabled) {
            LOG.info("Python logic ENABLED - will call Python API");
            
            // Use AsyncDataStream to call Python API without blocking
            processedStream = AsyncDataStream.unorderedWait(
                parsedStream,
                new PythonLogicAsyncFunction(pythonApiUrl, pythonApiTimeout, pythonRetryAttempts),
                pythonApiTimeout * pythonRetryAttempts, 
                TimeUnit.MILLISECONDS,
                100  // capacity
            ).name("Python Logic Processing - " + kafkaTopic);
            
        } else {
            LOG.info("Python logic DISABLED - using direct passthrough");
            processedStream = parsedStream;
        }

        // Transform to RowData for Iceberg
        DataStream<RowData> rowDataStream = processedStream
            .map(record -> {
                GenericRowData row = new GenericRowData(10);
                row.setField(0, StringData.fromString(record.eventId));
                row.setField(1, StringData.fromString(record.userId));
                row.setField(2, StringData.fromString(record.eventType));
                row.setField(3, record.timestamp);
                row.setField(4, StringData.fromString(record.rideId));
                row.setField(5, record.surgeMultiplier);
                row.setField(6, record.estimatedWaitMinutes);
                row.setField(7, record.fareAmount);
                row.setField(8, record.driverRating);
                
                String eventHour = hourFormatter.format(Instant.ofEpochMilli(record.timestamp));
                row.setField(9, StringData.fromString(eventHour));
                
                return (RowData) row;
            })
            .name("To RowData - " + kafkaTopic);

        // Create Iceberg table if needed
        createIcebergTableIfNotExists(catalogLoader, namespace, tableName);
        
        // Write to Iceberg
        TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);
        tableLoader.open();

        FlinkSink.forRowData(rowDataStream)
            .tableLoader(tableLoader)
            .equalityFieldColumns(java.util.Arrays.asList("event_id", "event_hour"))
            .upsert(true)
            .append();
        
        LOG.info("Sink configured for topic {} -> table {}", kafkaTopic, tableId);
    }
    
    /**
     * Parse event record from JSON
     */
    private static EventRecord parseEventRecord(String json) throws Exception {
        JsonNode node = mapper.readTree(json);
        JsonNode metadata = node.get("metadata");
        
        EventRecord record = new EventRecord();
        record.eventId = node.get("event_id").asText();
        record.userId = node.get("user_id").asText();
        record.eventType = node.get("event_type").asText();
        record.timestamp = node.get("timestamp").asLong();
        record.rideId = node.get("ride_id").asText();
        record.surgeMultiplier = metadata.get("surge_multiplier").asDouble();
        record.estimatedWaitMinutes = metadata.get("estimated_wait_minutes").asInt();
        record.fareAmount = metadata.get("fare_amount").asDouble();
        record.driverRating = metadata.get("driver_rating").asDouble();
        
        return record;
    }
    
    /**
     * Create Iceberg table if it doesn't exist
     */
    private static void createIcebergTableIfNotExists(
        CatalogLoader catalogLoader,
        String namespace,
        String tableName
    ) throws Exception {
        
        TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
        org.apache.iceberg.catalog.Catalog catalog = catalogLoader.loadCatalog();
        
        if (!catalog.tableExists(tableId)) {
            LOG.info("Creating table: {}", tableId);
            
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "event_id", 
                    org.apache.iceberg.types.Types.StringType.get()),
                org.apache.iceberg.types.Types.NestedField.required(2, "user_id", 
                    org.apache.iceberg.types.Types.StringType.get()),
                org.apache.iceberg.types.Types.NestedField.required(3, "event_type", 
                    org.apache.iceberg.types.Types.StringType.get()),
                org.apache.iceberg.types.Types.NestedField.required(4, "event_timestamp", 
                    org.apache.iceberg.types.Types.LongType.get()),
                org.apache.iceberg.types.Types.NestedField.required(5, "ride_id", 
                    org.apache.iceberg.types.Types.StringType.get()),
                org.apache.iceberg.types.Types.NestedField.required(6, "surge_multiplier", 
                    org.apache.iceberg.types.Types.DoubleType.get()),
                org.apache.iceberg.types.Types.NestedField.required(7, "estimated_wait_minutes", 
                    org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.required(8, "fare_amount", 
                    org.apache.iceberg.types.Types.DoubleType.get()),
                org.apache.iceberg.types.Types.NestedField.required(9, "driver_rating", 
                    org.apache.iceberg.types.Types.DoubleType.get()),
                org.apache.iceberg.types.Types.NestedField.required(10, "event_hour", 
                    org.apache.iceberg.types.Types.StringType.get())
            );
            
            org.apache.iceberg.PartitionSpec spec = org.apache.iceberg.PartitionSpec.builderFor(schema)
                .identity("event_hour")
                .build();
            
            Map<String, String> tableProps = new HashMap<>();
            tableProps.put("write.format.default", "parquet");
            tableProps.put("write.parquet.compression-codec", "snappy");
            tableProps.put("format-version", "2");
            tableProps.put("write.upsert.enabled", "true");
            
            catalog.createTable(tableId, schema, spec, tableProps);
            LOG.info("Table created successfully: {}", tableId);
        }
    }
    
    /**
     * Load application properties from KDA Runtime or fallback to defaults
     */
    private static Properties loadApplicationProperties(String[] args) throws IOException {
        Properties props = new Properties();
        
        try {
            Map<String, Properties> applicationProperties = 
                KinesisAnalyticsRuntime.getApplicationProperties();
            
            if (applicationProperties.containsKey("ApplicationProperties")) {
                LOG.info("Loaded properties from ApplicationProperties group");
                props = applicationProperties.get("ApplicationProperties");
            } else if (!applicationProperties.isEmpty()) {
                String firstKey = applicationProperties.keySet().iterator().next();
                LOG.info("Loaded properties from {} group", firstKey);
                props = applicationProperties.get(firstKey);
            }
        } catch (Exception e) {
            LOG.warn("Could not load KDA Runtime properties: {}", e.getMessage());
        }
        
        // Add defaults for any missing properties
        for (Map.Entry<String, String> entry : DEFAULT_CONFIG.entrySet()) {
            if (!props.containsKey(entry.getKey())) {
                props.setProperty(entry.getKey(), entry.getValue());
            }
        }
        
        return props;
    }
    
    /**
     * Get property with fallback to default
     */
    private static String getProperty(Properties props, String key) {
        return props.getProperty(key, DEFAULT_CONFIG.get(key));
    }
}