---
name: pipeline-design
description: Design ETL/ELT pipelines end-to-end — source connectors, extraction strategies, transform logic, load patterns, idempotency, scheduling, and error handling. Use this skill whenever the user is starting a new ingestion job, planning how data moves from a source (REST API, database, file, webhook, message queue) into a data warehouse or data lake. Also trigger when the user asks about pipeline architecture, incremental vs. full loads, backfill strategies, CDC, retry logic, or orchestration choices (Airflow, Prefect, dbt). This skill should feel like pairing with a senior data engineer on day one of a new pipeline project.
---

# Pipeline Design

## Starting Point: Understand Before You Build

Before writing a single line of ingestion code, answer these four questions. Every architectural decision flows from them.

1. **Source shape** — Is this a REST API, a database (Postgres, MySQL), a file drop (S3/GCS), a message queue (Kafka, Pub/Sub), or a webhook push?
2. **Volume and velocity** — Millions of rows daily? Tens of thousands? Real-time events or nightly batch?
3. **Change pattern** — Does the source expose a `updated_at` timestamp? A CDC stream? Or must you full-scan every time?
4. **Downstream contract** — Who consumes this data and how quickly do they need it? That drives scheduling and latency tolerance.

---

## Extraction Strategies

### Full load
Re-extract everything every run. Simple, always correct, expensive at scale. Use when:
- The source has no reliable `updated_at` or sequence key
- The table is small (< a few million rows)
- Correctness > cost

### Incremental load (watermark-based)
Track the highest `updated_at` (or auto-increment ID) seen so far. On each run, pull rows where `updated_at > last_watermark`.

```python
last_watermark = read_watermark(pipeline_name)
rows = source.query(f"SELECT * FROM orders WHERE updated_at > '{last_watermark}'")
write_to_warehouse(rows)
save_watermark(pipeline_name, max(row["updated_at"] for row in rows))
```

**Risk:** Rows with backdated `updated_at` (late-arriving data) are silently missed. Add a safety buffer of a few hours if this is a concern.

### CDC (Change Data Capture)
Read the database's transaction log (Debezium, Fivetran, AWS DMS). Captures inserts, updates, and deletes without polling. Use when:
- Deletes must be propagated
- Source table is too large to query incrementally
- Near-real-time latency is required

---

## Idempotency — The Most Important Property

A pipeline run should produce the same result if run once or ten times on the same input window. This is what makes restarts and backfills safe.

**Pattern: write to a staging table, then swap**

```sql
-- Step 1: write to a dated staging partition
INSERT INTO staging.orders_2024_03_15 SELECT ...;

-- Step 2: delete the target window and reload
DELETE FROM warehouse.orders WHERE date = '2024-03-15';
INSERT INTO warehouse.orders SELECT * FROM staging.orders_2024_03_15;
```

Or on BigQuery, use `WRITE_TRUNCATE` on a partition:

```python
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    range_partitioning=...,
)
```

**Why this matters:** Without idempotency, a retry appends duplicates. A backfill creates chaos. Idempotency turns "was this run successful?" from a scary question into a boring one.

---

## Load Strategies

| Strategy | When to use | Warehouse support |
|---|---|---|
| Full replace | Small tables, no history needed | All |
| Partition overwrite | Large tables, date-partitioned | BigQuery, Snowflake, Spark |
| Upsert (MERGE) | Key-based deduplication with updates | Snowflake, BigQuery, dbt |
| Append-only | Immutable event streams | All |
| SCD Type 2 | History must be preserved for dimension changes | dbt snapshots, Snowflake streams |

---

## ELT vs ETL

Modern warehouses (BigQuery, Snowflake) are cheap and powerful for computation. Prefer **ELT**:

1. **Extract** raw data as-is into a raw layer (preserve source fidelity)
2. **Load** immediately — no blocking transforms
3. **Transform** inside the warehouse with SQL/dbt

Reserve **ETL** (transform before load) for:
- PII that must never land in the warehouse raw
- Schema normalization that's cheaper to do at extraction time
- Heavy computation the warehouse can't do efficiently (ML inference, geo operations)

---

## Pipeline Layers

Organize your warehouse into logical layers so consumers know what to trust:

```
raw/           ← exact copy of source, never modified
  └── orders_raw

staging/       ← cleaned types, renamed columns, deduplicated
  └── stg_orders

intermediate/  ← joins and business-logic CTEs (optional)

marts/         ← final fact/dim tables consumed by BI tools and stakeholders
  └── fct_orders
  └── dim_customers
```

dbt enforces this naturally with its sources / staging / models convention.

---

## Error Handling & Observability

A pipeline without observability is a pipe bomb — it'll fail silently at the worst moment.

**Minimum viable observability:**
- Log row counts at extraction, load, and validation steps
- Alert on zero rows (silent failure pattern — often worse than an error)
- Alert on row count deviation > N% from prior run
- Store pipeline run metadata: start time, end time, rows read, rows written, status

**Retry logic:**
- Transient errors (rate limits, timeouts): retry with exponential backoff + jitter
- Data errors (bad schema, null violations): fail fast, send to dead-letter storage for investigation
- Never silently swallow exceptions — a pipeline that hides errors lies to stakeholders

---

## Orchestration Patterns

### Airflow / Prefect / Dagster
Use a DAG/flow/asset when:
- Multiple tasks depend on each other
- You need retry, backfill, and scheduling in one place
- Visibility into run history matters to stakeholders

**Task design principle:** Each task should be independently re-runnable. Avoid global state between tasks — pass data via XComs, parameters, or intermediate storage, not in-memory objects.

### dbt for transform-layer orchestration
dbt handles dependency resolution and incremental logic for SQL models. Let it manage `updated_at`-based incremental runs rather than coding them yourself.

```yaml
# dbt model config for incremental
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='sync_all_columns'
) }}

SELECT ...
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

---

## Backfill Strategy

Always design for backfill before you go live:
- Parameterize your pipeline on a date range, not "today minus X"
- Test a backfill of the last 30 days before production launch
- Ensure idempotency so a backfill can be re-run safely

---

## Pipeline Design Checklist

- [ ] Extraction strategy chosen (full / incremental / CDC) and justified
- [ ] Idempotency guaranteed for every load step
- [ ] Raw layer preserves source data unmodified
- [ ] Row count logging at each stage
- [ ] Zero-row alert configured
- [ ] Retry logic for transient failures, dead-letter for data errors
- [ ] Backfill tested before going live
- [ ] Scheduling interval matches downstream SLA
