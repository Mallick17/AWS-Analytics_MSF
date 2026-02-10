"# 🚀 Modular PyFlink Streaming Job - Complete Guide

## 📁 Project Structure

```
python-datastream/
├── streaming_job.py              # Main orchestrator (DO NOT MODIFY)
├── pom.xml                       # Maven build config (DO NOT MODIFY)
├── application_properties.json   # Flink runtime properties
│
├── assembly/
│   └── assembly.xml              # Packaging configuration
│
├── package/
│   └── requirements.txt          # Python dependencies
│
├── config/                       # ⭐ YAML CONFIGURATION FILES
│   ├── topics.yaml              # Topic definitions (MODIFY THIS)
│   └── transformations.yaml     # Transformation mappings (MODIFY THIS)
│
├── common/                       # Shared utilities package
│   ├── __init__.py
│   ├── config.py                # Configuration loader
│   ├── catalog_manager.py       # Iceberg catalog setup
│   ├── kafka_sources.py         # Kafka source creator
│   ├── iceberg_sinks.py         # Iceberg sink creator
│   └── utils.py                 # Utility functions
│
└── transformations/              # ⭐ TRANSFORMATION CLASSES
    ├── __init__.py
    ├── base_transformer.py      # Base class for all transformers
    ├── bid_events_raw.py        # ✅ Working example
    └── user_events_processed.py # 📝 Template for new transformers
```

---

## 🎯 How to Add a New Topic

### Step 1: Define Topic in `config/topics.yaml`

Open `config/topics.yaml` and add your topic configuration:

```yaml
topics:
  bid-events:
    enabled: true
    # ... existing config ...
  
  # 👇 ADD YOUR NEW TOPIC HERE
  user-activity:
    enabled: true
    kafka_group_id: \"flink-user-activity-v1\"
    scan_mode: \"earliest-offset\"
    format: \"json\"
    
    source_schema:
      - name: \"user_id\"
        type: \"BIGINT\"
      - name: \"activity_type\"
        type: \"STRING\"
      - name: \"page_url\"
        type: \"STRING\"
      - name: \"timestamp_ms\"
        type: \"BIGINT\"
    
    sink:
      table_name: \"user_activities\"
      schema:
        - name: \"user_id\"
          type: \"BIGINT\"
        - name: \"activity_type\"
          type: \"STRING\"
        - name: \"page_url\"
          type: \"STRING\"
        - name: \"activity_time\"
          type: \"TIMESTAMP(3)\"
        - name: \"ingestion_time\"
          type: \"TIMESTAMP(3)\"
    
    transformation: \"user_events_processed\"
```

### Step 2: Create Transformer Class

Create a new file `transformations/user_events_processed.py`:

```python
from transformations.base_transformer import BaseTransformer
from typing import Dict, Any


class UserEventsProcessedTransformer(BaseTransformer):
    \"\"\"Transformer for user activity events.\"\"\"
    
    def __init__(self, topic_config: Dict[str, Any]):
        super().__init__(topic_config)
    
    def get_transformation_sql(self, source_table: str) -> str:
        \"\"\"Generate transformation SQL.\"\"\"
        sql = f\"\"\"
            SELECT
                user_id,
                activity_type,
                page_url,
                TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS activity_time,
                CURRENT_TIMESTAMP AS ingestion_time
            FROM {source_table}
        \"\"\"
        return sql
    
    def get_description(self) -> str:
        return \"User activity events processing\"
```

### Step 3: Register Transformation in `config/transformations.yaml`

```yaml
transformations:
  bid_events_raw:
    # ... existing ...
  
  # 👇 ADD YOUR TRANSFORMATION MAPPING
  user_events_processed:
    class: \"UserEventsProcessedTransformer\"
    module: \"transformations.user_events_processed\"
    description: \"Processes user activity events\"
```

### Step 4: Build and Deploy

```bash
# Build with Maven
mvn clean package

# Deploy to AWS Managed Flink
# Upload the generated ZIP file: target/pyflink-s3tables-app.zip
```

---

## 🔄 How to Add a New Transformation Type

### Example: Windowed Aggregation

**1. Create transformer class** (`transformations/bid_events_aggregated.py`):

```python
from transformations.base_transformer import BaseTransformer


class BidEventsAggregatedTransformer(BaseTransformer):
    \"\"\"Aggregates bid events by user in 1-minute windows.\"\"\"
    
    def get_transformation_sql(self, source_table: str) -> str:
        sql = f\"\"\"
            SELECT
                user_id,
                COUNT(*) as bid_count,
                TUMBLE_START(TO_TIMESTAMP_LTZ(timestamp_ms, 3), INTERVAL '1' MINUTE) as window_start,
                TUMBLE_END(TO_TIMESTAMP_LTZ(timestamp_ms, 3), INTERVAL '1' MINUTE) as window_end,
                CURRENT_TIMESTAMP as ingestion_time
            FROM {source_table}
            GROUP BY
                user_id,
                TUMBLE(TO_TIMESTAMP_LTZ(timestamp_ms, 3), INTERVAL '1' MINUTE)
        \"\"\"
        return sql
```

**2. Register in `config/transformations.yaml`**:

```yaml
bid_events_aggregated:
  class: \"BidEventsAggregatedTransformer\"
  module: \"transformations.bid_events_aggregated\"
  description: \"Aggregates bid events by time windows\"
```

**3. Use in topic** (create new topic or modify existing):

```yaml
bid-events-aggregated:
  enabled: true
  kafka_group_id: \"flink-bid-agg-v1\"
  # ... same source schema as bid-events ...
  sink:
    table_name: \"bid_events_aggregated\"
    schema:
      - name: \"user_id\"
        type: \"BIGINT\"
      - name: \"bid_count\"
        type: \"BIGINT\"
      - name: \"window_start\"
        type: \"TIMESTAMP(3)\"
      - name: \"window_end\"
        type: \"TIMESTAMP(3)\"
      - name: \"ingestion_time\"
        type: \"TIMESTAMP(3)\"
  transformation: \"bid_events_aggregated\"
```

---

## 📝 Configuration Reference

### Topic Schema Definition

```yaml
topic-name:
  enabled: true/false           # Enable or disable topic processing
  kafka_group_id: \"string\"      # Consumer group ID
  scan_mode: \"earliest-offset\"  # or \"latest-offset\"
  format: \"json\"                # or \"avro\", \"csv\", etc.
  
  source_schema:                # Kafka message structure
    - name: \"field_name\"
      type: \"FLINK_SQL_TYPE\"
  
  sink:
    table_name: \"table_name\"    # Iceberg table name
    schema:                      # Output structure
      - name: \"field_name\"
        type: \"FLINK_SQL_TYPE\"
  
  transformation: \"transformer_name\"
```

### Supported Flink SQL Types

| Type | Description | Example |
|------|-------------|---------|
| `STRING` | Text data | `\"username\": \"STRING\"` |
| `BIGINT` | Large integers | `\"user_id\": \"BIGINT\"` |
| `INT` | Integers | `\"count\": \"INT\"` |
| `DOUBLE` | Floating point | `\"price\": \"DOUBLE\"` |
| `BOOLEAN` | True/false | `\"is_active\": \"BOOLEAN\"` |
| `TIMESTAMP(3)` | Timestamp with milliseconds | `\"event_time\": \"TIMESTAMP(3)\"` |
| `ARRAY<STRING>` | Array of strings | `\"tags\": \"ARRAY<STRING>\"` |

---

## 🏗️ Architecture

### Data Flow

```
Kafka Topic (MSK)
       ↓
[KafkaSourceCreator]
  - Reads topics.yaml
  - Creates Kafka source table
       ↓
[Transformer Class]
  - Applies transformation logic
  - Generates SQL query
       ↓
[IcebergSinkCreator]
  - Reads topics.yaml
  - Creates Iceberg sink table
       ↓
S3 Tables (Iceberg)
```

### Component Interaction

```
streaming_job.py (Orchestrator)
    ↓
    ├─→ Config (loads YAML files)
    ├─→ CatalogManager (Iceberg setup)
    ├─→ KafkaSourceCreator (creates Kafka sources)
    ├─→ IcebergSinkCreator (creates Iceberg sinks)
    └─→ Transformer Classes (data transformation)
```

---

## 🛠️ Development Workflow

### 1. Local Testing (Optional)

You can test transformations locally before deploying:

```python
from transformations.bid_events_raw import BidEventsRawTransformer

# Mock config
config = {
    'source_schema': [...],
    'sink': {'schema': [...]}
}

transformer = BidEventsRawTransformer(config)
sql = transformer.get_transformation_sql(\"test_table\")
print(sql)
```

### 2. Build

```bash
mvn clean package
```

This creates: `target/pyflink-s3tables-app.zip`

### 3. Deploy to AWS Managed Flink

1. Go to AWS Managed Flink Console
2. Create or update your application
3. Upload `pyflink-s3tables-app.zip`
4. Configure runtime properties (if needed)
5. Start the application

### 4. Monitor

Check CloudWatch logs for:
- ✅ Configuration loading
- ✅ Pipeline creation
- ✅ Data processing
- ❌ Any errors

---

## 📊 Example: Complete New Topic Setup

Let's add an \"order-events\" topic end-to-end:

### 1. `config/topics.yaml`

```yaml
order-events:
  enabled: true
  kafka_group_id: \"flink-orders-v1\"
  scan_mode: \"earliest-offset\"
  format: \"json\"
  
  source_schema:
    - name: \"order_id\"
      type: \"STRING\"
    - name: \"user_id\"
      type: \"BIGINT\"
    - name: \"amount\"
      type: \"DOUBLE\"
    - name: \"status\"
      type: \"STRING\"
    - name: \"timestamp_ms\"
      type: \"BIGINT\"
  
  sink:
    table_name: \"orders\"
    schema:
      - name: \"order_id\"
        type: \"STRING\"
      - name: \"user_id\"
        type: \"BIGINT\"
      - name: \"amount\"
        type: \"DOUBLE\"
      - name: \"status\"
        type: \"STRING\"
      - name: \"order_time\"
        type: \"TIMESTAMP(3)\"
      - name: \"ingestion_time\"
        type: \"TIMESTAMP(3)\"
  
  transformation: \"order_events_enriched\"
```

### 2. `transformations/order_events_enriched.py`

```python
from transformations.base_transformer import BaseTransformer


class OrderEventsEnrichedTransformer(BaseTransformer):
    \"\"\"Enriches order events with additional metadata.\"\"\"
    
    def get_transformation_sql(self, source_table: str) -> str:
        sql = f\"\"\"
            SELECT
                order_id,
                user_id,
                amount,
                status,
                TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS order_time,
                CURRENT_TIMESTAMP AS ingestion_time
            FROM {source_table}
            WHERE amount > 0
        \"\"\"
        return sql
    
    def get_description(self) -> str:
        return \"Order events with enrichment and filtering\"
```

### 3. `config/transformations.yaml`

```yaml
order_events_enriched:
  class: \"OrderEventsEnrichedTransformer\"
  module: \"transformations.order_events_enriched\"
  description: \"Enriches order events\"
```

### 4. Build and Deploy

```bash
mvn clean package
# Deploy ZIP to AWS Managed Flink
```

✅ **Done!** The new pipeline will automatically process order events.

---

## 🔍 Troubleshooting

### Issue: Topic Not Processing

**Check:**
1. `enabled: true` in `topics.yaml`
2. Transformation class exists and is registered
3. Source and sink schemas match transformation output

### Issue: Transformation Not Found

**Solution:**
Ensure transformer is registered in `config/transformations.yaml` with correct module and class name.

### Issue: Schema Mismatch

**Solution:**
- Source schema must match Kafka message structure
- Sink schema must match transformation SQL output

### Issue: Build Fails

**Check:**
- All Python files have proper syntax
- YAML files are valid
- All transformer classes inherit from `BaseTransformer`

---

## ✅ Benefits of This Architecture

| Before | After |
|--------|-------|
| Hardcoded configurations | YAML-based configs |
| Single monolithic file | Modular packages |
| Difficult to extend | Easy to add topics/transformations |
| Code changes for new topics | Just add YAML + transformer class |
| No separation of concerns | Clear component boundaries |

---

## 📚 Next Steps

1. ✅ Review the working example (`bid-events` topic)
2. ✅ Understand the YAML structure
3. ✅ Create your first custom transformer
4. ✅ Add a new topic following the guide
5. ✅ Build and deploy to AWS Managed Flink

---

## 🆘 Quick Reference

### Add New Topic Checklist

- [ ] Add topic config to `config/topics.yaml`
- [ ] Set `enabled: true`
- [ ] Create transformer class in `transformations/`
- [ ] Register transformer in `config/transformations.yaml`
- [ ] Build with `mvn clean package`
- [ ] Deploy to AWS Managed Flink

### File Modification Matrix

| File | Modify? | When? |
|------|---------|-------|
| `config/topics.yaml` | ✅ YES | Adding/modifying topics |
| `config/transformations.yaml` | ✅ YES | Adding transformations |
| `transformations/*.py` | ✅ YES | Creating transformers |
| `streaming_job.py` | ❌ NO | Core logic (don't modify) |
| `common/*.py` | ❌ NO | Utilities (don't modify) |
| `pom.xml` | ❌ NO | Dependencies (per requirement) |

---

**🎉 Your modular Flink streaming job is ready! Add topics and transformations with ease.**
"