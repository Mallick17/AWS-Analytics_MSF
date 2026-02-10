"# 🎯 QUICK MARKERS - Where to Add Topics & Transformations

## 📍 Exact Locations to Modify

### 1️⃣ Add New Topic → `config/topics.yaml`

**Location:** After line 70 (after bid-events definition)

```yaml
# 👇 ADD NEW TOPICS HERE (line ~72)
user-activity:
  enabled: true
  kafka_group_id: \"flink-user-activity-v1\"
  scan_mode: \"earliest-offset\"
  format: \"json\"
  source_schema:
    - name: \"user_id\"
      type: \"BIGINT\"
    # ... more fields
  sink:
    table_name: \"user_activities\"
    schema:
      - name: \"user_id\"
        type: \"BIGINT\"
      # ... more fields
  transformation: \"user_events_processed\"
```

---

### 2️⃣ Add New Transformation Mapping → `config/transformations.yaml`

**Location:** After line 10 (after bid_events_raw)

```yaml
# 👇 ADD NEW TRANSFORMATION MAPPINGS HERE (line ~12)
user_events_processed:
  class: \"UserEventsProcessedTransformer\"
  module: \"transformations.user_events_processed\"
  description: \"Process user events\"
```

---

### 3️⃣ Create Transformer Class → `transformations/your_transformer.py`

**Template file:** See `transformations/user_events_processed.py` (currently commented out)

```python
from transformations.base_transformer import BaseTransformer
from typing import Dict, Any


class YourTransformer(BaseTransformer):
    \"\"\"Your transformer description.\"\"\"
    
    def __init__(self, topic_config: Dict[str, Any]):
        super().__init__(topic_config)
    
    def get_transformation_sql(self, source_table: str) -> str:
        sql = f\"\"\"
            SELECT
                field1,
                field2,
                TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS event_time,
                CURRENT_TIMESTAMP AS ingestion_time
            FROM {source_table}
        \"\"\"
        return sql
    
    def get_description(self) -> str:
        return \"Your transformation description\"
```

---

## 🚀 3-Step Process

### Step 1: Enable/Add Topic in YAML
```bash
File: config/topics.yaml
Line: ~72 (after bid-events)
Action: Copy bid-events block, rename, modify fields
```

### Step 2: Create Transformer Class
```bash
File: transformations/your_transformer.py
Action: Copy bid_events_raw.py, modify SQL logic
```

### Step 3: Register Transformation
```bash
File: config/transformations.yaml
Line: ~12 (after bid_events_raw)
Action: Add mapping with class name and module path
```

---

## 📂 Directory Markers

```
python-datastream/
├── config/
│   ├── topics.yaml                    ⭐ MARKER 1: Add topics here (line ~72)
│   └── transformations.yaml           ⭐ MARKER 2: Register here (line ~12)
│
├── transformations/
│   ├── bid_events_raw.py             ✅ Working example
│   ├── user_events_processed.py      📝 Template (commented)
│   └── [your_new_transformer.py]     ⭐ MARKER 3: Create new file here
│
└── streaming_job.py                   ❌ DO NOT MODIFY
```

---

## ✅ Working Example: bid-events

### 1. Topic Config (`config/topics.yaml` - line 22)
```yaml
bid-events:
  enabled: true
  kafka_group_id: \"flink-s3tables-json-v1\"
  # ... full config shown in file
```

### 2. Transformer Class (`transformations/bid_events_raw.py`)
```python
class BidEventsRawTransformer(BaseTransformer):
    def get_transformation_sql(self, source_table: str) -> str:
        # SQL transformation logic
```

### 3. Transformation Mapping (`config/transformations.yaml` - line 5)
```yaml
bid_events_raw:
  class: \"BidEventsRawTransformer\"
  module: \"transformations.bid_events_raw\"
```

---

## 📝 Copy-Paste Templates

### Template A: Simple Timestamp Conversion Topic

**In `config/topics.yaml`:**
```yaml
your-topic:
  enabled: true
  kafka_group_id: \"flink-your-topic-v1\"
  scan_mode: \"earliest-offset\"
  format: \"json\"
  source_schema:
    - name: \"field1\"
      type: \"STRING\"
    - name: \"timestamp_ms\"
      type: \"BIGINT\"
  sink:
    table_name: \"your_table\"
    schema:
      - name: \"field1\"
        type: \"STRING\"
      - name: \"event_time\"
        type: \"TIMESTAMP(3)\"
  transformation: \"your_transformer\"
```

**In `config/transformations.yaml`:**
```yaml
your_transformer:
  class: \"YourTransformer\"
  module: \"transformations.your_transformer\"
  description: \"Your description\"
```

**In `transformations/your_transformer.py`:**
```python
from transformations.base_transformer import BaseTransformer

class YourTransformer(BaseTransformer):
    def get_transformation_sql(self, source_table: str) -> str:
        return f\"\"\"
            SELECT
                field1,
                TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS event_time
            FROM {source_table}
        \"\"\"
```

---

## 🔍 Verification Checklist

After adding a new topic:

- [ ] `config/topics.yaml` has new topic with `enabled: true`
- [ ] Transformer class created in `transformations/`
- [ ] Transformer registered in `config/transformations.yaml`
- [ ] Class name matches in YAML and Python file
- [ ] Module path is correct in YAML
- [ ] Source schema matches Kafka message
- [ ] Sink schema matches SQL output
- [ ] SQL syntax is valid Flink SQL

---

## 🎨 Visual Markers

```
config/topics.yaml
====================
Line 1-21:  Global kafka config
Line 22-70: ✅ bid-events (working example)
Line 72+:   🔸 ADD YOUR TOPICS HERE 🔸

config/transformations.yaml
===========================
Line 1-4:   Global header
Line 5-10:  ✅ bid_events_raw (working example)
Line 12+:   🔸 ADD YOUR TRANSFORMATIONS HERE 🔸

transformations/
================
✅ base_transformer.py      - Base class (don't modify)
✅ bid_events_raw.py        - Working example
📝 user_events_processed.py - Template (commented out)
🔸 YOUR NEW FILES GO HERE  🔸
```

---

## ⚡ Ultra-Quick Guide

**Goal:** Add \"user-activity\" topic

1. **Edit** `config/topics.yaml` → Copy bid-events block → Rename to user-activity → Change fields
2. **Create** `transformations/user_events_processed.py` → Copy bid_events_raw.py → Modify SQL
3. **Edit** `config/transformations.yaml` → Add user_events_processed mapping
4. **Build** `mvn clean package`
5. **Deploy** Upload ZIP to AWS Managed Flink

**Time:** < 10 minutes ⏱️

---

## 🎯 Summary

| What | Where | Line |
|------|-------|------|
| Add Topic | `config/topics.yaml` | ~72 |
| Register Transformation | `config/transformations.yaml` | ~12 |
| Create Transformer | `transformations/new_file.py` | New file |

**Remember:** After creating files, always run `mvn clean package` and redeploy!

---

**🎉 You now know exactly where to add new topics and transformations!**
"