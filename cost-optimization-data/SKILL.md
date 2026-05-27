---
name: cost-optimization-data
description: Query cost analysis, partition pruning, slot reservation strategies, storage tiering, and cloud data warehouse cost reduction. Use this skill whenever the cloud data bill is unexpectedly high, a specific query is scanning too much data, the team wants to understand what's driving BigQuery/Snowflake/Redshift costs, or when choosing between on-demand vs. reserved capacity. Also trigger when the user mentions bytes scanned, slot utilization, query cost, storage costs, Redshift concurrency, Snowflake credits, or when trying to set up cost alerts and budgets. If someone says "our BigQuery bill jumped" or "this query is expensive", this skill should be active immediately.
---

# Cost Optimization for Cloud Data Infrastructure

## Find the Money First

Before optimizing anything, identify the actual cost drivers. Cloud consoles lie by omission — the default billing view shows totals, not which queries or tables are responsible.

### BigQuery: find expensive queries

```sql
-- Top 20 most expensive queries in the last 7 days
SELECT
    user_email,
    total_bytes_processed / POW(1024, 4) AS tb_processed,
    total_bytes_processed / POW(1024, 4) * 6.25 AS estimated_cost_usd,  -- on-demand rate
    query,
    creation_time
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
    creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND job_type = 'QUERY'
    AND state = 'DONE'
ORDER BY total_bytes_processed DESC
LIMIT 20;
```

```sql
-- Top tables by bytes scanned (which tables are being read most expensively)
SELECT
    referenced_table.table_id AS table_name,
    COUNT(*) AS query_count,
    SUM(total_bytes_processed) / POW(1024, 4) AS total_tb_scanned
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT,
    UNNEST(referenced_tables) AS referenced_table
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY 1
ORDER BY total_tb_scanned DESC
LIMIT 20;
```

### Snowflake: find credit-burning warehouses

```sql
-- Warehouse credit consumption by day
SELECT
    warehouse_name,
    DATE(start_time) AS usage_date,
    SUM(credits_used) AS credits_used,
    SUM(credits_used) * 3.0 AS estimated_cost_usd  -- adjust to your contract rate
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY credits_used DESC;
```

### Redshift: find slow, expensive queries

```sql
SELECT
    userid,
    query,
    elapsed / 1000000.0 AS elapsed_seconds,
    substring(querytxt, 1, 100) AS query_preview
FROM stv_recents
WHERE status = 'Done'
ORDER BY elapsed DESC
LIMIT 20;
```

---

## BigQuery Cost Reduction

### Partition pruning — the biggest lever

Every query that doesn't filter on a partition column scans the entire table. On a 10TB table, that's $62.50 per query at on-demand rates.

```sql
-- Without partition filter — scans entire table
SELECT user_id, event_type FROM events WHERE user_id = '123';

-- With partition filter — scans only one day's data
SELECT user_id, event_type FROM events
WHERE event_date = '2024-01-15'   -- partition column
  AND user_id = '123';
```

Check if your tables are partitioned and if queries are actually pruning:

```sql
-- Verify a table's partition column
SELECT table_name, partition_expiration_ms, require_partition_filter
FROM `project.dataset`.INFORMATION_SCHEMA.TABLES
WHERE table_name = 'events';

-- Force queries to include partition filter (prevents full scans)
ALTER TABLE `project.dataset.events`
SET OPTIONS (require_partition_filter = TRUE);
```

### Clustering — secondary savings

After partitioning, cluster on the columns most used in WHERE and JOIN. Clustering reduces bytes scanned within a partition.

```sql
-- Re-create table with clustering
CREATE OR REPLACE TABLE `project.dataset.events`
PARTITION BY DATE(event_time)
CLUSTER BY user_id, event_type
AS SELECT * FROM `project.dataset.events_old`;
```

Good clustering columns: high-cardinality columns used in filters after the partition filter. Not good: boolean columns, columns rarely used in WHERE.

### On-demand vs. flat-rate (slots)

| Monthly BQ spend | Recommendation |
|---|---|
| < $2,000 | Stay on on-demand — reservations won't pay off |
| $2,000–$5,000 | Analyze query concurrency patterns before committing |
| > $5,000 | Flat-rate reservations likely cheaper; model your slot utilization |

Flat-rate pricing: 100 slots costs ~$2,000/month. If your on-demand spend is > $2,000/month, do the math — but only if queries run during business hours (idle slots at night still cost money).

### Query-level cost controls

```sql
-- Dry run to estimate bytes before actually running (BigQuery UI shows this too)
-- Via CLI:
bq query --dry_run --use_legacy_sql=false 'SELECT * FROM `project.dataset.events`'

-- Set a maximum bytes billed limit per query to prevent runaway scans
bq query \
  --maximum_bytes_billed=10737418240 \  # 10GB limit
  --use_legacy_sql=false \
  'SELECT ...'
```

---

## Snowflake Cost Reduction

### Right-size warehouses

The biggest Snowflake waste is warehouses that are too large and don't auto-suspend.

```sql
-- Check average query duration vs. warehouse size
-- If avg query is < 30 seconds, you may be over-provisioned
SELECT
    warehouse_name,
    warehouse_size,
    AVG(execution_time) / 1000 AS avg_exec_seconds,
    COUNT(*) AS query_count
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY avg_exec_seconds DESC;
```

```sql
-- Auto-suspend idle warehouses after 60 seconds
ALTER WAREHOUSE my_warehouse SET AUTO_SUSPEND = 60;
ALTER WAREHOUSE my_warehouse SET AUTO_RESUME = TRUE;
```

### Query acceleration and result caching

Snowflake caches query results for 24 hours if the underlying data hasn't changed. Repeated identical queries are free. Design BI tools to issue identical SQL (same parameters, same column order) to hit the cache.

### Multi-cluster warehouses — only when needed

Multi-cluster warehouses handle concurrency spikes. Enable them only for warehouses serving many concurrent users (dashboards, BI tools). Leave ETL warehouses as single-cluster.

---

## Redshift Cost Reduction

### Concurrency scaling

Concurrency scaling adds temporary capacity during peak loads and charges per-second. Enable it selectively — only for user-facing queries, not batch ETL.

```sql
-- Enable concurrency scaling for a specific workload group
CREATE WORKLOAD GROUP dashboard_users WITH (CONCURRENCY_SCALING = auto);
-- Keep ETL jobs on base cluster (no concurrency scaling charges)
CREATE WORKLOAD GROUP etl_jobs WITH (CONCURRENCY_SCALING = off);
```

### RA3 — separate storage from compute

If still on DS2 or DC2 nodes, migrating to RA3 decouples storage (billed separately at S3 rates) from compute. You pay for compute hours only when the cluster is running — and you can pause it overnight.

```bash
# Pause Redshift cluster overnight (saves ~8 hours of compute per day)
aws redshift pause-cluster --cluster-identifier my-cluster
aws redshift resume-cluster --cluster-identifier my-cluster
```

---

## Storage Tiering

Storage is cheap but not free, and cold data in hot storage is wasted money.

### S3 lifecycle rules

```json
{
  "Rules": [{
    "ID": "archive-raw-after-90-days",
    "Filter": {"Prefix": "raw/"},
    "Status": "Enabled",
    "Transitions": [
      {"Days": 30, "StorageClass": "STANDARD_IA"},
      {"Days": 90, "StorageClass": "GLACIER_IR"},
      {"Days": 365, "StorageClass": "DEEP_ARCHIVE"}
    ],
    "Expiration": {"Days": 2555}
  }]
}
```

| S3 Class | Cost (per GB/month) | Retrieval | Use for |
|---|---|---|---|
| Standard | ~$0.023 | Instant | Active data, last 30 days |
| Standard-IA | ~$0.0125 | Instant | Accessed < once/month |
| Glacier IR | ~$0.004 | Instant | Rarely accessed, need fast retrieval |
| Deep Archive | ~$0.00099 | 12 hours | Compliance archival |

### BigQuery storage optimization

```sql
-- Check table storage costs
SELECT
    table_id,
    row_count,
    size_bytes / POW(1024, 3) AS size_gb,
    (size_bytes / POW(1024, 3)) * 0.02 AS active_storage_cost_usd,
    last_modified_time
FROM `project.dataset`.__TABLES__
ORDER BY size_bytes DESC;
```

BigQuery automatically moves data to long-term storage (half price) after 90 days of no modification. Avoid unnecessary `UPDATE` statements on archive tables — they reset the 90-day clock.

---

## Cost Alerting

Set budget alerts before costs spiral — not after.

```bash
# GCP budget alert — notify at 50%, 90%, 100% of monthly budget
gcloud billing budgets create \
  --billing-account=BILLING_ACCOUNT_ID \
  --display-name="BigQuery Monthly Budget" \
  --budget-amount=5000 \
  --threshold-rule=percent=0.5 \
  --threshold-rule=percent=0.9 \
  --threshold-rule=percent=1.0 \
  --all-updates-rule-monitoring-notification-channels=projects/my-project/notificationChannels/123
```

---

## Optimization Checklist

When investigating a high bill:
- [ ] Identified top 10 queries by bytes scanned / credits used
- [ ] Confirmed expensive tables have partition columns and queries filter on them
- [ ] `require_partition_filter = TRUE` set on large tables to prevent full scans
- [ ] Clustering applied after partitioning on high-cardinality filter columns
- [ ] Snowflake warehouses have `AUTO_SUSPEND = 60` set
- [ ] Redshift cluster paused during off-hours if workload allows
- [ ] S3 lifecycle rules tiering data older than 30/90 days
- [ ] Budget alert configured to fire before bill is already large
- [ ] On-demand vs. flat-rate decision revisited if monthly BQ spend > $2,000
