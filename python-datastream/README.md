"# PyFlink Modular Streaming Job

A modular, configuration-driven Apache Flink streaming job for processing Kafka topics and writing to S3 Tables (Iceberg).

## 🏗️ Architecture

**Modular, YAML-driven, class-based transformations**

```
Kafka (MSK) → Flink Transformations → S3 Tables (Iceberg)
```

## 📁 Project Structure

```
python-datastream/
├── streaming_job.py           # Main orchestrator
├── config/                    # ⭐ YAML configurations
│   ├── topics.yaml           # Topic definitions
│   └── transformations.yaml  # Transformation mappings
├── common/                    # Shared utilities
├── transformations/           # ⭐ Transformation classes
│   ├── base_transformer.py
│   └── bid_events_raw.py     # Example transformer
├── assembly/                  # Maven assembly config
├── package/                   # Python requirements
├── pom.xml                    # Maven build
└── application_properties.json
```

## 🚀 Quick Start

### 1. Add a New Topic

**Edit `config/topics.yaml`:**
```yaml
your-topic:
  enabled: true
  kafka_group_id: \"flink-your-topic-v1\"
  scan_mode: \"earliest-offset\"
  format: \"json\"
  source_schema:
    - name: \"field1\"
      type: \"STRING\"
  sink:
    table_name: \"your_table\"
    schema:
      - name: \"field1\"
        type: \"STRING\"
  transformation: \"your_transformer\"
```

### 2. Create Transformer Class

**Create `transformations/your_transformer.py`:**
```python
from transformations.base_transformer import BaseTransformer

class YourTransformer(BaseTransformer):
    def get_transformation_sql(self, source_table: str) -> str:
        return f\"SELECT * FROM {source_table}\"
```

### 3. Register Transformation

**Edit `config/transformations.yaml`:**
```yaml
your_transformer:
  class: \"YourTransformer\"
  module: \"transformations.your_transformer\"
  description: \"Your transformation\"
```

### 4. Build & Deploy

```bash
mvn clean package
# Upload target/pyflink-s3tables-app.zip to AWS Managed Flink
```

## 📚 Documentation

- **[MODULAR_GUIDE_V2.md](MODULAR_GUIDE_V2.md)** - Complete user guide
- **[MARKERS.md](MARKERS.md)** - Quick reference with exact locations
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Visual diagrams (if exists)

## ✨ Features

✅ **YAML-based configuration** - No code changes for new topics  
✅ **Modular transformations** - Each transformation is a separate class  
✅ **Multiple topics support** - Process multiple Kafka topics in one job  
✅ **Easy to extend** - Clear separation of concerns  
✅ **AWS Managed Flink compatible** - No dependency changes needed  

## 🎯 Current Topics

- ✅ **bid-events** - Working example with timestamp conversion

## 🔧 Dependencies

- Apache Flink 1.19.0
- PyYAML 6.0.1
- AWS MSK IAM Auth
- S3 Tables Iceberg Catalog

## 📖 How It Works

1. **Configuration Loading** - Reads YAML configs from `config/`
2. **Kafka Source Creation** - Dynamically creates Kafka source tables
3. **Transformation** - Applies transformer class logic
4. **Iceberg Sink** - Writes to S3 Tables in Iceberg format
5. **Streaming** - Runs continuously processing data

## 🛠️ Build

```bash
# Package everything into a deployment ZIP
mvn clean package

# Output: target/pyflink-s3tables-app.zip
```

## 🚦 Deployment

1. Build the project: `mvn clean package`
2. Go to AWS Managed Flink Console
3. Upload `target/pyflink-s3tables-app.zip`
4. Configure runtime properties if needed
5. Start the application

## 📊 Example Use Cases

- Real-time event processing
- Stream enrichment
- Data transformation and validation
- Windowed aggregations
- Multi-topic processing

## ⚠️ Important Notes

- **DO NOT MODIFY:** `pom.xml`, `streaming_job.py`, `common/*`
- **SAFE TO MODIFY:** YAML configs, transformation classes
- All topics must be enabled in `config/topics.yaml`
- Each transformer must inherit from `BaseTransformer`

## 🆘 Troubleshooting

**Topic not processing?**
- Check `enabled: true` in `config/topics.yaml`
- Verify transformer class exists and is registered

**Build fails?**
- Check Python syntax in transformer classes
- Validate YAML syntax in config files

**Schema mismatch?**
- Ensure source_schema matches Kafka message
- Ensure sink_schema matches transformation output

## 📝 License

Internal project - AWS Managed Flink compatible

---

**For detailed instructions, see [MODULAR_GUIDE_V2.md](MODULAR_GUIDE_V2.md)**

**For quick reference, see [MARKERS.md](MARKERS.md)**
"