Issues faced during the AWS MSF using Python for storing the streaming data in S3 Table

- Source: Kafka (MSK) ingestion via connectors.  
- Processing: PyFlink for Python-native ops like windowed aggregations and transformation.
- Sink: Partitioned Iceberg tables on S3 for ACID queries via Athena(tried using rest and hadoop catalog).  
- Flow: Kafka → PyFlink (transform/enrich) → Iceberg. 


## Errors We're Consistently Facing
Tested on Flink 1.19; failures at startup/sink. Root causes: PyFlink immaturity + MSF dep conflicts.

### 1. DataStream API Limitations
- Issue: Row serialization to Iceberg fails (e.g., Avro schema mismatches); no upsert support, OOM on batch commits >10k EPS.  
- Why: Limited Python bindings for Iceberg features vs. Java.  
- Impact: No writes; dev overhead spikes.  
- Repro Snippet:  
  ```python
  from pyflink.datastream import StreamExecutionEnvironment
  from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
  # Kafka setup succeeds, but:
  ds.sink_to(iceberg_sink)  # Schema/upsert error
  env.execute()  # Fails
  ```

### 2. Table API IcebergSink: Shading Conflicts
- Issue: `NoClassDefFoundError: shaded/org/apache/hadoop/conf/Configuration` on CREATE/INSERT.  
- Why: Our shaded ZIP (Hadoop 3.3.6 + Iceberg) clashes with MSF's pre-loaded stubs; PythonDriver can't resolve relocs.  
- Impact: Bootstrap fails; no SQL ops.  
- Repro Snippet:  
  ```python
  t_env.execute_sql("CREATE TABLE iceberg_events (...) WITH ('connector'='iceberg', 'warehouse'='arn:aws:s3tables:...')")
  t_env.execute_sql("INSERT INTO iceberg_events SELECT * FROM kafka_source")  # Hadoop error
  ```

### 3. Runtime Crashes (Exit Code 1)
- Issue: `RestHandlerException` → `Python process exits with code: 1`; app rolls to READY with `CodeError.InvalidApplicationCode`.  
- Why: Dep/env isolation in MSF; no Python logs for deeper diag.  
- Impact: 100% startup failure.  
- Sample Log (Version 16, 2025-12-19T17:09:50):  
  ```json
  {
    "applicationARN": "arn:aws:kinesisanalytics:ap-south-1:149815625933:application/testing-poc-python",
    "applicationVersionId": 16,
    "message": "Failed to transition... Caused by: java.lang.RuntimeException: Python process exits with code: 1 at PythonDriver.main(PythonDriver.java:124)",
    "messageType": "ERROR",
    "errorCode": "CodeError.InvalidApplicationCode"
  }
  ```


The dependencies/packages added in our PyFlink prototypes (via `pom.xml` for Java interop/ZIP packaging and `requirements.txt` for Python), how they contributed to the errors, and ZIP/JAR size issues (e.g., shading bloat exceeding MSF's 512MB upload limit).  
- Packages Involved: Specific deps causing conflicts.  
- What Happened: Sequence of events leading to failure.  
- Size Impact: How packaging inflated artifacts.  
- Mitigation Attempted: Quick fixes tried.  

These stem from aggressive shading to resolve MSF's managed runtime conflicts, but they amplified issues. Total ZIP sizes hit 450MB+ in later iterations, triggering upload warnings.

## Packages Added Overview
To enable PyFlink-Iceberg integration, we added these in `pom.xml` (for shaded JAR in ZIP) and `requirements.txt` (Python runtime). Shading (via Maven Shade Plugin) relocated classes like `org.apache.hadoop` to `shaded.org.apache.hadoop` to avoid MSF pre-loads.
Here you go — I’ve converted both tables into clean, review-ready bullet points, keeping purpose + conflict risk explicit and easy to scan.

---

## Java Dependencies (pom.xml – Shaded into Fat JAR)

* `org.apache.flink:flink-streaming-java` (1.19.1)

  * Core Flink DataStream API used for streaming pipelines
  *  (typically `provided` by MSF runtime)

* `org.apache.flink:flink-connector-kafka` (3.2.0-1.19)

  * Kafka source and sink connector for MSK integration
  *  (authentication and Kafka client dependencies)

* `org.apache.iceberg:iceberg-flink-runtime-1.19` (1.6.1)

  * Enables Apache Iceberg table operations from Flink
  *  (depends on Hadoop and S3 filesystem layers)

* `org.apache.hadoop:hadoop-common` (3.3.6)

  * Provides Hadoop filesystem abstractions, including S3 access
  *  (can clash with MSF’s internally provided Hadoop 3.x stubs)

* `org.apache.hadoop:hadoop-aws` (3.3.6)

  * Adds AWS S3 integration for Hadoop-based file systems
  *  (overlaps AWS SDK and S3 auth classes)

* `com.amazonaws:aws-java-sdk-bundle` (1.12.767)

  * Legacy AWS SDK v1 used for S3 authentication and access
  *  (SDK overlap; shading exclusions were insufficient)

* `software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime` (0.1.5)

  * Custom Iceberg catalog implementation for Amazon S3 Tables
  *  (non-standard catalog initialization and classloading)

* `software.amazon.awssdk:apache-client` (2.20.0)

  * HTTP client used by AWS SDK v2 (hybrid Java–Python interactions)
  * 

* `org.apache.httpcomponents.client5:httpclient5` (5.2.1)

  * Async HTTP client for outbound service calls
  * 

* `com.fasterxml.jackson.core:jackson-databind` (2.15.2)

  * JSON serialization and deserialization
  * 

* `com.amazonaws:aws-kinesisanalytics-runtime` (1.2.0)

  * Loads runtime properties and environment config in Amazon MSF
  *  (environment variable and classloader behavior)

* Logging (`org.apache.logging.log4j:*` – 2.17.1)

  * SLF4J → Log4j logging implementation
  *  (used in runtime scope only)

---

### Shading Configuration Impact

* Security metadata (`META-INF/*.SF`, `*.DSA`, `*.RSA`) excluded correctly
* Hadoop, Iceberg, and AWS SDK were fully shaded
* Resulted in a ~250 MB fat JAR, increasing:

  * Upload time to S3
  * Classloader pressure in MSF
  * Risk of dependency shadowing at runtime

---

## Python Dependencies (requirements.txt – ZIP for PyFlink Shell)

* `fastapi` (0.109.0)

  * Lightweight API layer for hybrid Python-side logic
  * 

* `uvicorn[standard]` (0.27.0)

  * ASGI server used to run FastAPI endpoints
  * 

* `pydantic` (2.5.3)

  * Data validation and schema enforcement
  * 

* `python-multipart` (0.0.6)

  * Multipart form parsing for API requests
  * 

Note: Python pkgs are lightweight (~50MB), but ZIP totals swelled from Java shading.

## ZIP/JAR Size Issues
- Baseline Size: Initial ZIP (no shading): 150MB (Flink provided + basic connectors).  
- Post-Shading Bloat: Hadoop/Iceberg added 200MB+ (dupe classes, uncompressed Parquet libs); total ~450MB by v16.  
- MSF Limits: Upload cap 1GB, but >400MB triggers slow deploys (10-15min) and occasional `InvalidApplicationCode` on unpack.
- What Happened: Builds succeeded locally (`mvn clean package`), but MSF validation failed intermittently with "code too large" warnings in CloudWatch. Mitigation: `--excludes` in Shade Plugin trimmed 50MB, but core conflicts persisted.  
- Impact: Delayed iterations; forced dep pruning (e.g., dropped `aws-java-sdk-bundle` for SDK v2).

### Error 1: DataStream API Limitations (Row Format/Sink Failures)
- Packages Involved: `flink-streaming-java` (core), `flink-connector-kafka` (source), `iceberg-flink-runtime-1.19` (sink bindings).  
- What Happened:  
  1. ZIP upload/deploy succeeds (size ~200MB).  
  2. PyFlink shell starts, Kafka consumer binds (`FlinkKafkaConsumer`).  
  3. `ds.sink_to(iceberg_sink)` invokes Iceberg row serialization—fails on Avro-to-RowData mapping (no Python-native schema resolver).  
  4. Exception: `SchemaInferenceException` bubbles to `env.execute()`, job aborts mid-stream. No writes; partial data loss.  
  5. Logs: "Unsupported row format for Iceberg in PyFlink DataStream."  
- Size Impact: Iceberg dep added 80MB (runtime JARs); no direct bloat, but increased unpack time delayed error surfacing.  
- Mitigation Attempted: Switched to `RowType` manual mapping—partial success locally, but MSF env lacked PyArrow (unshaded). Pruned to 180MB ZIP.

### Error 2: Table API with IcebergSink (NoClassDefFoundError: Hadoop Configuration)
- Packages Involved: `hadoop-common/aws` (3.3.6), `iceberg-flink-runtime-1.19` (catalog deps), `s3-tables-catalog-for-iceberg-runtime` (S3 init).  
- What Happened:  
  1. Shaded JAR built (300MB+ from Hadoop bloat).  
  2. MSF deploys ZIP; PythonDriver launches TableEnv.  
  3. `t_env.execute_sql("CREATE TABLE ... WITH ('connector'='iceberg')")` triggers catalog load—seeks `org.apache.hadoop.conf.Configuration`.  
  4. Shading relocates to `shaded/org.apache.hadoop.conf.Configuration`, but MSF's pre-loaded Hadoop stubs (unshaded) clash; PyFlink can't bridge.  
  5. Throws `NoClassDefFoundError`; stack: `PythonDriver.main` → `CatalogLoader.loadCatalog()` → abort. App rolls to READY.  
  6. Logs: "Class not found: shaded/org/apache/hadoop/conf/Configuration (line 124)."  
- Size Impact: Hadoop pair (`common` + `aws`) inflated JAR by 150MB (includes S3 impls); exclusions in Shade reduced to 120MB, but core classes stayed.  
- Mitigation Attempted: Downgraded Hadoop to 3.2.x (MSF-aligned)—error shifted to AWS SDK mismatch. Removed `s3-tables-catalog` temp (fell back to Glue, size -30MB).

### Error 3: Runtime Crashes (Python Process Exit Code 1)
- Packages Involved: All shaded (esp. `aws-kinesisanalytics-runtime`, logging Log4j), plus Python pkgs (`fastapi` etc. for hybrid tests).  
- What Happened:  
  1. Fat JAR shades to 250MB; ZIP hits 400MB—upload slow, MSF warns "Large code package."  
  2. Deploy starts; `DetachedApplicationRunner` invokes `PythonDriver.main`.  
  3. Driver bootstraps env—loads shaded deps, but MSF isolation blocks Python shell access (e.g., no IAM prop for S3Tables ARN).  
  4. Conflicts cascade: Logging init fails (Log4j unshaded clash), then Hadoop stubs trigger; Python process segfaults (exit 1).  
  5. Wrapper: `RestHandlerException` → `ProgramAbortException` → rollback. No Python stdout; only JVM trace.  
  6. Logs: "RuntimeException: Python process exits with code: 1 at PythonDriver.main(PythonDriver.java:124)." (v4/v16).  
- Size Impact: Cumulative shading + Python wheels (50MB) pushed ZIP over 400MB threshold; unpack failures amplified (MSF timeout on large artifacts).  
- Mitigation Attempted: Split ZIP (core + optional deps)—partial, but MSF rejected multi-file. Pruned logging to SLF4J-only (size -20MB); added env var passthrough—no dice.
