# Project Structure - Simplified

## 📁 Files You Need

```
flink-iceberg-etl/
│
├── main.py                      ✅ MAIN APPLICATION (use this)
│   └─ PyIceberg + S3 Tables
│   └─ Kafka → Transform → S3 Tables
│
├── requirements.txt             ✅ Python dependencies
│   └─ pyflink, pyiceberg, boto3, etc.
│
├── .pyiceberg.yaml             ✅ S3 Tables catalog config
│   └─ Warehouse ARN, region, etc.
│
├── test_local.py               ✅ Local testing
│   └─ Test without Flink cluster
│
├── build.sh                    ✅ Build & package script
│   └─ Downloads JARs, creates ZIP
│
├── DEPLOYMENT.md               ✅ Deployment guide
│   └─ Step-by-step AWS setup
│
├── README.md                   ✅ Project documentation
│   └─ Quick start, architecture
│
└── S3_TABLES_EXPLAINED.md      ✅ Understanding S3 Tables
    └─ What is S3 Tables vs S3
```

## ❌ Files Removed

```
main_native_iceberg.py          ❌ REMOVED (not needed)
└─ Why removed:
   • Flink native connector doesn't fully support S3 Tables
   • PyIceberg is AWS's recommended approach
   • Adds complexity without benefits
   • Would require additional JAR dependencies
```

## 🎯 Why Only One Main File?

### main.py (What We Use) ✅

```python
# Uses PyIceberg library
from pyiceberg.catalog import load_catalog

# Direct S3 Tables support
catalog = load_catalog(
    "s3_tables",
    **{
        "type": "glue",
        "warehouse": "arn:aws:s3tables:...",  # S3 Tables ARN
    }
)

# Simple, direct writes
table.append(data)  # S3 Tables handles everything!
```

**Benefits:**
- ✅ Direct S3 Tables support
- ✅ No extra dependencies
- ✅ Simpler code
- ✅ AWS recommended
- ✅ Active development

### main_native_iceberg.py (Removed) ❌

```python
# Would use Flink Table API + SQL
from pyflink.table import StreamTableEnvironment

# Create Iceberg catalog in Flink
table_env.execute_sql("""
    CREATE CATALOG iceberg_catalog WITH (
        'type' = 'iceberg',
        'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
        ...
    )
""")
```

**Problems:**
- ❌ More complex setup
- ❌ S3 Tables support still maturing
- ❌ Requires additional JARs
- ❌ Less flexible
- ❌ Harder to debug

## 📦 What Gets Deployed

When you run `build.sh`, it creates:

```
flink-iceberg-etl.zip
├── main.py                 ← Your application
├── requirements.txt        ← Dependencies list
├── .pyiceberg.yaml        ← Catalog config
└── lib/                   ← JAR dependencies
    ├── flink-sql-connector-kafka-1.18.0.jar
    ├── aws-msk-iam-auth-1.1.6-all.jar
    ├── iceberg-flink-runtime-1.18-1.4.3.jar
    ├── iceberg-aws-bundle-1.4.3.jar
    └── bundle-2.20.18.jar
```

**Upload to:**
```
s3://testing-python-flink-connector/applications/flink-iceberg-etl.zip
```

## 🔄 Workflow

```
1. Development
   ├── Edit main.py
   ├── Test with test_local.py
   └── Update requirements.txt

2. Build
   ├── Run build.sh
   └── Creates flink-iceberg-etl.zip

3. Deploy
   ├── Upload to S3: testing-python-flink-connector
   ├── Create Flink app in AWS Console
   └── Configure environment variables

4. Run
   ├── Start Flink application
   ├── Monitor CloudWatch logs
   └── Query data in Athena
```

## 📊 Data Flow

```
Source                  Processing              Destination
──────                  ──────────              ───────────

MSK Kafka    →    PyFlink (main.py)    →    S3 Tables
user_events           │                       sink.user_events
                      ├─ Parse JSON
                      ├─ Transform
                      ├─ Batch (100)
                      └─ PyIceberg
                         └─ table.append()
                            └─ Auto-optimized!
```

## 🎓 File Purposes

### main.py - Core Application

**What it does:**
1. Connects to MSK Kafka with IAM auth
2. Reads messages from topics
3. Parses and transforms JSON
4. Batches records (100 at a time)
5. Writes to S3 Tables using PyIceberg
6. S3 Tables auto-optimizes everything

**Key classes:**
- `KafkaToS3TablesETL` - Main ETL orchestrator
- `S3TablesWriter` - Batch writer for S3 Tables
- Uses PyIceberg for all Iceberg operations

### requirements.txt - Dependencies

```txt
apache-flink==1.18.1        # PyFlink framework
pyflink==1.18.1             # Python Flink API
pyiceberg[pyarrow,s3fs,glue]==0.6.1  # S3 Tables support
boto3>=1.34.0               # AWS SDK
pyarrow>=14.0.0             # Arrow format
```

### .pyiceberg.yaml - Catalog Config

```yaml
catalog:
  s3_tables:
    type: glue                # S3 Tables uses Glue
    warehouse: arn:aws:s3tables:...  # Your S3 Tables bucket
    client.region: ap-south-1
    io-impl: org.apache.iceberg.aws.s3.S3FileIO
```

### test_local.py - Local Testing

**Tests:**
- ✅ Message transformation
- ✅ Schema validation
- ✅ Iceberg operations (local)
- ✅ S3 Tables connection (if credentials available)
- ✅ Performance benchmarks

### build.sh - Build Automation

**What it does:**
1. Creates `build/` directory
2. Copies Python files
3. Downloads JAR dependencies
4. Creates `flink-iceberg-etl.zip`
5. Shows upload command

### DEPLOYMENT.md - Deployment Steps

**Covers:**
- IAM role setup
- S3 upload
- Flink app creation
- Environment variables
- Monitoring setup

### README.md - Quick Start

**Provides:**
- Architecture overview
- Quick start guide
- Configuration examples
- Troubleshooting tips

### S3_TABLES_EXPLAINED.md - Concepts

**Explains:**
- S3 Tables vs Regular S3
- Why use PyIceberg
- ARN format differences
- Data flow

## 🎯 Decision Tree

```
Need to modify code?
├─ Yes → Edit main.py
└─ No
   │
   Need to add dependency?
   ├─ Yes → Update requirements.txt
   └─ No
      │
      Need to test locally?
      ├─ Yes → Run test_local.py
      └─ No
         │
         Ready to deploy?
         ├─ Yes → Run build.sh → Upload ZIP
         └─ No → Read DEPLOYMENT.md
```

## ✅ Simplified Benefits

**Before (with both files):**
```
main.py + main_native_iceberg.py
├─ Confusion: Which one to use?
├─ Duplication: Similar functionality
└─ Complexity: Two approaches
```

**After (single file):**
```
main.py only
├─ Clear: One way to do it
├─ Simple: PyIceberg + S3 Tables
└─ Reliable: AWS recommended approach
```

## 🚀 Getting Started

```bash
# 1. Test locally
python test_local.py

# 2. Build package
./build.sh

# 3. Upload to S3
aws s3 cp flink-iceberg-etl.zip \
  s3://testing-python-flink-connector/applications/ \
  --region ap-south-1

# 4. Deploy in AWS Console
# Follow DEPLOYMENT.md

# 5. Start application
aws kinesisanalyticsv2 start-application \
  --application-name flink-iceberg-etl \
  --region ap-south-1
```

## 📝 Summary

**Single file approach (`main.py`):**
- ✅ Uses PyIceberg (AWS recommended)
- ✅ Direct S3 Tables support
- ✅ Simpler architecture
- ✅ Easier to maintain
- ✅ Better for production

**No need for multiple approaches!**