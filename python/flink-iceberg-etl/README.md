# PyFlink MSK to S3 Tables (Iceberg) ETL

A production-ready Python solution for streaming ETL from Amazon MSK (Kafka) to **S3 Tables** using Apache Iceberg format on AWS Managed Service for Apache Flink.

## 🎯 What is S3 Tables?

**S3 Tables** is AWS's managed Apache Iceberg service - it's NOT a regular S3 bucket!

Key differences:
- ✅ **S3 Tables**: `arn:aws:s3tables:region:account:bucket/bucket-name` (Managed Iceberg)
- ❌ **Regular S3**: `arn:aws:s3:::bucket-name` (Just object storage)

**S3 Tables Benefits:**
- Automatic file optimization and compaction
- Built-in metadata management
- Integrated with AWS Glue Data Catalog
- ACID transactions out of the box
- No manual Iceberg maintenance needed

## 📋 Your Configuration

**Your AWS Environment:**
- **S3 Application Bucket** (for code): `testing-python-flink-connector`
  - ARN: `arn:aws:s3:::testing-python-flink-connector`
  - URI: `s3://testing-python-flink-connector/`
- **S3 Tables Bucket** (for data): `testing-python-flink-table-bucket`
  - ARN: `arn:aws:s3tables:ap-south-1:149815625933:bucket/testing-python-flink-table-bucket`
  - Namespace: `sink`
  - Type: **S3 Tables (Managed Iceberg)**
- **Region**: `ap-south-1`
- **Account**: `149815625933`

## 🏗️ Architecture

```
┌─────────────┐      ┌──────────────────┐      ┌────────────────────────┐
│  Amazon MSK │─────▶│  AWS Managed     │─────▶│  S3 Tables (Managed    │
│   (Kafka)   │      │  Flink (Python)  │      │  Iceberg)              │
│             │      │  + PyIceberg     │      │  testing-python-flink- │
└─────────────┘      └──────────────────┘      │  table-bucket          │
                              │                 │  namespace: sink       │
                              ▼                 └────────────────────────┘
                     ┌─────────────────┐
                     │  AWS Glue       │
                     │  Data Catalog   │
                     │  (Automatic)    │
                     └─────────────────┘
```

## 📁 Project Files (Simplified)

```
flink-iceberg-etl/
├── main.py              # ← ONLY file you need (PyIceberg + S3 Tables)
├── requirements.txt     # Python dependencies
├── .pyiceberg.yaml     # PyIceberg configuration
├── test_local.py       # Local testing
├── build.sh            # Build and package script
├── DEPLOYMENT.md       # Deployment guide
└── README.md           # This file
```

**Note**: We removed `main_native_iceberg.py` - it's not needed because PyIceberg is the correct approach for S3 Tables.

## 🚀 Quick Start

### Prerequisites

- Python 3.9 or 3.10
- AWS CLI configured with credentials for account 149815625933
- Access to Amazon MSK cluster
- S3 Tables bucket `testing-python-flink-table-bucket` created
- Regular S3 bucket `testing-python-flink-connector` for application code

### Local Testing

1. **Install dependencies**:
```bash
pip install -r requirements.txt
```

2. **Run local tests**:
```bash
python test_local.py
```

Expected output:
```
========================================
  PyFlink Iceberg ETL - Local Tests
========================================

S3 Bucket: testing-python-flink-connector (for app code)
S3 Tables: testing-python-flink-table-bucket (for data - Managed Iceberg)
Namespace: sink
Region: ap-south-1
========================================

✓ All tests passed!
```

## 📦 Building for Deployment

### Using Build Script (Recommended)

```bash
chmod +x build.sh
./build.sh
```

Output:
```
==================================
Build Complete!
==================================
Package: flink-iceberg-etl.zip
Size: ~145M

To deploy to S3:
  aws s3 cp flink-iceberg-etl.zip s3://testing-python-flink-connector/applications/ --region ap-south-1
```

### What Gets Packaged

- `main.py` - Your ETL application using PyIceberg
- `requirements.txt` - Python dependencies
- `.pyiceberg.yaml` - S3 Tables catalog configuration
- `lib/` directory with JARs:
  - Kafka connector
  - MSK IAM auth
  - Iceberg AWS bundle

## 🚢 Deployment

### 1. Upload to S3

```bash
# Upload to your application bucket (regular S3)
aws s3 cp flink-iceberg-etl.zip s3://testing-python-flink-connector/applications/ --region ap-south-1

# Verify
aws s3 ls s3://testing-python-flink-connector/applications/ --region ap-south-1
```

### 2. Create Flink Application

**Environment Variables (Critical):**
```
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = your-msk-brokers:9098
KAFKA_TOPICS = user_events
KAFKA_CONSUMER_GROUP = flink-python-consumer
KAFKA_OFFSET = earliest

# S3 Tables Configuration (NOT regular S3!)
S3_WAREHOUSE = arn:aws:s3tables:ap-south-1:149815625933:bucket/testing-python-flink-table-bucket
TABLE_NAMESPACE = sink
AWS_REGION = ap-south-1

# Flink Configuration
PARALLELISM = 2
CHECKPOINT_INTERVAL = 60000
```

### 3. Start Application

```bash
aws kinesisanalyticsv2 start-application \
  --region ap-south-1 \
  --application-name flink-iceberg-etl
```

## 🔍 Understanding S3 Tables vs Regular S3

### S3 Tables (What We Use) ✅

```python
# S3 Tables ARN format
warehouse = "arn:aws:s3tables:ap-south-1:149815625933:bucket/testing-python-flink-table-bucket"

catalog = load_catalog(
    "s3_tables",
    **{
        "type": "glue",  # Uses Glue Data Catalog
        "warehouse": warehouse,
        "client.region": "ap-south-1",
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    }
)

# Write to S3 Tables (managed Iceberg)
table = catalog.load_table("sink.user_events")
table.append(data)  # Automatic optimization!
```

**Features:**
- ✅ Automatic file compaction
- ✅ Automatic metadata management
- ✅ Built-in optimization
- ✅ Integrated with Glue
- ✅ ACID transactions
- ✅ No manual maintenance

### Regular S3 + Iceberg (We DON'T Use) ❌

```python
# Regular S3 ARN format (NOT what we use)
warehouse = "s3://my-bucket/warehouse"  # Wrong for S3 Tables!

# Would require manual:
# - File compaction
# - Metadata management
# - Optimization
# - Glue catalog updates
```

## 📊 Data Flow in S3 Tables

```
1. Kafka Message → 2. Parse & Transform → 3. Batch Buffer → 4. PyIceberg → 5. S3 Tables
                                             (100 records)     .append()    (Auto-optimize)
                                                                            
                                                               ↓
                                                         AWS Glue Catalog
                                                         (Auto-updated)
```

**What S3 Tables Does Automatically:**
1. Writes Parquet files
2. Manages partitions
3. Compacts small files
4. Updates Glue catalog
5. Optimizes metadata
6. Maintains statistics

## 📈 Monitoring S3 Tables

### Query with Athena

```sql
-- S3 Tables automatically appear in Athena
-- No need to create external table manually!

SELECT 
    event_hour,
    COUNT(*) as event_count,
    AVG(fare_amount) as avg_fare
FROM sink.user_events  -- Auto-registered in Glue
GROUP BY event_hour
ORDER BY event_hour DESC
LIMIT 10;
```

### Query with PyIceberg

```python
from pyiceberg.catalog import load_catalog

# Connect to S3 Tables
catalog = load_catalog(
    "s3_tables",
    **{
        "type": "glue",
        "warehouse": "arn:aws:s3tables:ap-south-1:149815625933:bucket/testing-python-flink-table-bucket",
        "client.region": "ap-south-1"
    }
)

# Load table
table = catalog.load_table("sink.user_events")

# Query
df = table.scan().to_pandas()
print(df.head())
```

### Check S3 Tables Status

```bash
# List tables in S3 Tables bucket
aws s3tables list-tables \
  --table-bucket-arn arn:aws:s3tables:ap-south-1:149815625933:bucket/testing-python-flink-table-bucket \
  --namespace sink \
  --region ap-south-1

# Get table metadata
aws s3tables get-table-metadata-location \
  --table-bucket-arn arn:aws:s3tables:ap-south-1:149815625933:bucket/testing-python-flink-table-bucket \
  --namespace sink \
  --name user_events \
  --region ap-south-1
```

## 🔧 Configuration Details

### S3 Tables Properties (Automatic)

When we create tables in S3 Tables:

```python
table_props = {
    "write.format.default": "parquet",           # Binary columnar format
    "write.parquet.compression-codec": "snappy", # Fast compression
    "format-version": "2",                       # Latest Iceberg format
    "write.upsert.enabled": "true",             # Allow updates
}
```

**S3 Tables also adds automatically:**
- File size optimization
- Compaction policies
- Metadata retention
- Snapshot management

### Partition Strategy

Tables are partitioned by `event_hour`:

```python
partition_spec = PartitionSpec(
    PartitionField(
        source_id=10,  # event_hour field
        field_id=1000,
        transform=IdentityTransform(),
        name="event_hour"
    )
)
```

**Benefits:**
- ✅ Query only relevant partitions
- ✅ Efficient time-based filtering
- ✅ Automatic partition pruning

## 🎯 Key Differences Summary

| Feature | S3 Tables (We Use) | Regular S3 + Iceberg |
|---------|-------------------|---------------------|
| **ARN Format** | `arn:aws:s3tables:...` | `arn:aws:s3:::...` |
| **Service** | Managed by AWS | Self-managed |
| **Optimization** | Automatic | Manual |
| **Compaction** | Built-in | Manual jobs needed |
| **Catalog** | Auto Glue sync | Manual updates |
| **Cost** | Higher | Lower |
| **Maintenance** | Zero | Regular work |
| **Best For** | Production | Cost-sensitive |

## 💰 Cost Considerations

**S3 Tables Pricing:**
- Storage: Similar to S3
- Requests: Managed service fee
- Management: Included
- Compaction: Included

**Worth it because:**
- ✅ No ops overhead
- ✅ Automatic optimization
- ✅ Better query performance
- ✅ Less data scanning

## 🔍 Troubleshooting

### "Access Denied" for S3 Tables

```bash
# IAM role needs these S3 Tables permissions:
s3tables:GetTable
s3tables:GetTableBucket
s3tables:CreateTable
s3tables:PutObject
s3tables:GetObject
s3tables:ListTables
s3tables:UpdateTableMetadataLocation
```

### Tables Not Showing in Athena

```bash
# S3 Tables should auto-register with Glue
# Check namespace exists:
aws s3tables list-table-buckets --region ap-south-1

# Check tables in namespace:
aws s3tables list-tables \
  --table-bucket-arn arn:aws:s3tables:ap-south-1:149815625933:bucket/testing-python-flink-table-bucket \
  --namespace sink \
  --region ap-south-1
```

### PyIceberg Connection Issues

```python
# Verify your warehouse ARN format
# CORRECT (S3 Tables):
warehouse = "arn:aws:s3tables:ap-south-1:149815625933:bucket/testing-python-flink-table-bucket"

# WRONG (Regular S3):
warehouse = "s3://testing-python-flink-table-bucket"  # Won't work!
```

## 📝 Production Checklist

- [ ] S3 Tables bucket `testing-python-flink-table-bucket` exists
- [ ] Namespace `sink` created in S3 Tables
- [ ] Regular S3 bucket `testing-python-flink-connector` for app code
- [ ] IAM role has S3 Tables permissions (not just S3)
- [ ] PyIceberg configured with correct S3 Tables ARN format
- [ ] CloudWatch alarms configured
- [ ] Monitoring dashboard created
- [ ] Cost alerts enabled

## 🔗 Key Resources

- **S3 Tables**: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html
- **PyIceberg**: https://py.iceberg.apache.org/
- **PyFlink**: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/
- **AWS Managed Flink**: https://docs.aws.amazon.com/kinesisanalytics/

---

**Important Notes:**
- ✅ Use `main.py` only (PyIceberg + S3 Tables)
- ✅ S3 Tables ARN format: `arn:aws:s3tables:region:account:bucket/name`
- ✅ Regular S3 ARN format: `arn:aws:s3:::bucket-name`
- ✅ They are different services!
- ✅ S3 Tables provides managed Iceberg with automatic optimization