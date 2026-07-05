---
name: cloud-infra-data
description: AWS/GCP/Azure data infrastructure — S3/GCS/ADLS partitioning, BigQuery slot management, Redshift spectrum, Snowflake warehouses, IAM roles for data access, cost optimization, and managed service selection. Use this skill whenever the user is deploying a pipeline to cloud, choosing between managed data services, configuring storage for a data lake, setting up IAM/permissions for pipelines, asking about BigQuery pricing, Redshift vs. BigQuery vs. Snowflake, S3 bucket layout, or cloud-specific performance tuning. Also trigger when the user mentions cloud costs, slow BigQuery queries, Redshift concurrency scaling, storage formats in the cloud, or cross-account data access. If it touches cloud + data together, this skill should be active.
---

# Cloud Infrastructure for Data Pipelines

## Service Selection Guide

### Compute (query engines)

| Service | Best fit | Cost model |
|---|---|---|
| **BigQuery** | Variable/spiky workloads, serverless preference | Per-TB scanned (on-demand) or slot reservations |
| **Snowflake** | Multi-cloud, strong SQL, virtual warehouse isolation | Per-credit (compute time) |
| **Redshift** | AWS-native, predictable workloads, RA3 storage separation | Per-node/hour or serverless per-RPU |
| **Databricks** | Spark workloads, ML/data science teams | DBU per hour |
| **Athena** | Ad-hoc queries on S3, minimal ops | Per-TB scanned |

The biggest practical difference: BigQuery and Athena are serverless (no cluster to manage); Snowflake and Redshift require you to think about concurrency and warehouse sizing.

### Storage

| Service | Use for |
|---|---|
| **S3 (AWS)** | Data lake, staging area, Parquet/Delta/Iceberg tables |
| **GCS (GCP)** | Same as S3 in the GCP ecosystem |
| **ADLS Gen2 (Azure)** | Azure data lake, hierarchical namespace for Hadoop compatibility |

All three are object stores — they look like key-value stores, not filesystems. The "folder" structure in the key name is just a naming convention.

---

## Storage Layout and Partitioning

### S3 / GCS bucket layout

```
s3://my-data-lake/
  raw/
    source=salesforce/
      year=2024/month=01/day=15/
        events_20240115_001.parquet
  processed/
    domain=churn/
      year=2024/month=01/
        churn_features_20240101.parquet
  archive/
    ...
```

Separate raw and processed data in the key hierarchy so you can apply different retention policies and IAM permissions to each layer.

### Partition strategy

Partition on the columns most commonly used in WHERE clauses. For time-series data, `year/month/day` is standard. Avoid over-partitioning — having millions of tiny files is worse than having a few large ones.

```python
# PySpark — write with partitioning
df.write \
    .partitionBy("year", "month", "day") \
    .mode("overwrite") \
    .parquet("s3://my-bucket/processed/events/")
```

**Partition column types matter:** BigQuery and Athena push partition filters down efficiently. Use date/timestamp columns for time partitioning, not string representations — `2024-01-15` as a DATE, not `"20240115"` as a STRING.

### File size and format

| Format | Best for | Compression |
|---|---|---|
| **Parquet** | Columnar analytics, default choice | Snappy (fast), Zstd (small) |
| **Delta Lake** | ACID transactions, upserts, time travel | Parquet underneath |
| **Iceberg** | Multi-engine, large tables, schema evolution | Parquet underneath |
| **ORC** | Hive/EMR workloads | Zlib |

Target 128MB–1GB per file after compression. Files smaller than ~10MB create metadata overhead that slows queries on all columnar engines.

---

## BigQuery

### Cost control

BigQuery on-demand charges per TB scanned. The biggest lever is how much data your queries touch.

```sql
-- Always filter on partition column to prune scans
SELECT user_id, event_type
FROM `project.dataset.events`
WHERE DATE(created_at) BETWEEN '2024-01-01' AND '2024-01-31'  -- partition pruning
  AND event_type = 'purchase';

-- Check bytes scanned before running expensive queries
-- In BigQuery UI: the validator shows estimated bytes in the top right
```

Cluster your tables on the columns most used in WHERE and JOIN after the partition column — clustering reduces bytes scanned within a partition.

```sql
CREATE TABLE `project.dataset.events`
PARTITION BY DATE(created_at)
CLUSTER BY user_id, event_type
AS SELECT ...;
```

### Slot reservations vs. on-demand

On-demand is cheaper for infrequent/bursty workloads. Reservations (flat-rate pricing) make sense when you're spending > ~$2,000/month on on-demand, or when you need predictable query concurrency for dashboards.

### BigQuery Storage API

Use the Storage Read API for fast data export to Pandas/Spark — much faster than exporting to CSV first.

```python
from google.cloud import bigquery_storage

client = bigquery_storage.BigQueryReadClient()
# Reads directly into Arrow/Pandas without intermediate storage
```

---

## Redshift

### RA3 node types (recommended)

RA3 separates compute from storage — you pay for storage on S3 and scale compute independently. This is almost always the right choice for new Redshift clusters.

### Distribution and sort keys

```sql
-- Distribute fact tables by the join key to minimize data movement
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10,2)
)
DISTSTYLE KEY DISTKEY (user_id)  -- co-locate with users table
SORTKEY (created_at);            -- skip scans on date range queries
```

Mismatched distribution keys between joined tables cause expensive data redistribution across nodes. Check `SVL_QUERY_SUMMARY` for `DS_DIST_BOTH` steps — those are the expensive ones.

### Redshift Spectrum

Query Parquet/ORC files directly on S3 without loading into Redshift:

```sql
CREATE EXTERNAL SCHEMA raw_lake
FROM DATA CATALOG DATABASE 'my_glue_db'
IAM_ROLE 'arn:aws:iam::123456:role/redshift-spectrum-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

SELECT * FROM raw_lake.events WHERE event_date = '2024-01-15';
```

Use Spectrum to query raw/archive data without paying for Redshift storage on cold data.

---

## IAM Patterns for Data Pipelines

### Principle of least privilege

Each pipeline component should have only the permissions it needs, scoped to the specific resources it touches.

```json
// S3 policy for a pipeline that reads raw and writes processed
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::my-bucket/raw/*",
        "arn:aws:s3:::my-bucket"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::my-bucket/processed/*"
    }
  ]
}
```

### Cross-account access

When pipelines need to read from another team's account, use IAM roles + resource-based policies rather than sharing credentials.

```json
// Bucket policy in Account A — allows Role in Account B to read
{
  "Principal": {"AWS": "arn:aws:iam::ACCOUNT_B:role/pipeline-role"},
  "Action": ["s3:GetObject"],
  "Resource": "arn:aws:s3:::account-a-bucket/shared/*"
}
```

### GCP service accounts

```bash
# Create a service account per pipeline
gcloud iam service-accounts create churn-pipeline-sa

# Grant BigQuery read on the source dataset
gcloud projects add-iam-policy-binding my-project \
  --member="serviceAccount:churn-pipeline-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

# Grant GCS write on the output bucket only
gsutil iam ch serviceAccount:churn-pipeline-sa@my-project.iam.gserviceaccount.com:objectCreator gs://my-output-bucket/
```

---

## Cost Optimization

| Pattern | Savings |
|---|---|
| Parquet + Snappy instead of CSV | 60–80% storage reduction; 5–10x less scanned in BigQuery/Athena |
| Partition pruning in all queries | Often 90%+ reduction in bytes scanned |
| S3 Intelligent-Tiering for archive data | 40–68% storage cost on cold data |
| Right-size Redshift/Snowflake warehouses | Pause warehouses when idle; use auto-suspend |
| Columnar projection — select only needed columns | Directly reduces scanned bytes in columnar engines |
| Compact small files before querying | Reduces metadata overhead and improves scan speed |

---

## Infrastructure Checklist

Before going to production:
- [ ] Storage layout uses partition columns that match query patterns
- [ ] File sizes are 128MB–1GB after compression
- [ ] IAM roles follow least-privilege (no `s3:*` or `bigquery.admin` for pipelines)
- [ ] Credentials are in Secrets Manager / Secret Manager, not environment variables or code
- [ ] BigQuery tables are partitioned and clustered on the right columns
- [ ] Redshift DISTKEY matches the primary join key for fact tables
- [ ] Cost alerts configured on the cloud account
- [ ] Data lifecycle policy set — raw data retention, archive after N days
