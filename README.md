# Data Engineering Skills Directory

Claude Code skill modules for data engineering work. Each skill activates automatically when a relevant task is detected, or you can reference it by name in your prompt.

---

## Quick Navigation

| | Skill | Jump to |
|---|---|---|
| **Core** | Pipeline design, SQL, Python, Schema, Data quality | [Core Data Engineering](#core-data-engineering) |
| **Orchestration** | Airflow, Prefect, Dagster, Streaming | [Orchestration and Scheduling](#orchestration-and-scheduling) |
| **Cloud** | AWS / GCP / Azure, Cost optimization | [Cloud Infrastructure](#cloud-infrastructure) |
| **Transform** | dbt models, macros, incremental | [Transformation and Modeling](#transformation-and-modeling) |
| **Reliability** | Contracts, Governance, Migration | [Data Reliability and Governance](#data-reliability-and-governance) |
| **ML** | Feature stores, training/serving skew | [Machine Learning Integration](#machine-learning-integration) |
| **Communication** | Incident reports, stakeholder updates | [Communication](#communication) |
| **Utilities** | Excel, Skill creator | [Utilities](#utilities) |

---

## Core Data Engineering

### [pipeline-design](./pipeline-design/SKILL.md)
Design ETL/ELT pipelines from source to warehouse — extraction strategies, load patterns, idempotency, and error handling.

- Full load vs. incremental vs. CDC
- Backfill and retry logic
- Orchestration choices (Airflow, Prefect, dbt)

> **When to use:** Starting a new ingestion job, planning how data moves from a source into a data warehouse or lake.
>
> **Example:** Ingesting Salesforce opportunity data into BigQuery on a daily schedule — choosing incremental load, designing idempotent inserts, wiring up failure alerts.

---

### [sql-patterns](./sql-patterns/SKILL.md)
Production SQL for analytical workloads on columnar warehouses.

- Window functions, CTEs, deduplication
- Query optimization and partition pruning
- Warehouse idioms: BigQuery, Snowflake, Redshift, DuckDB

> **When to use:** Writing complex queries, reviewing SQL for performance or correctness, working with aggregations, ranking, running totals, or lag/lead comparisons.
>
> **Example:** Finding each user's most recent session and computing a 7-day rolling average — using `ROW_NUMBER()` and `ROWS BETWEEN` correctly for your target warehouse.

---

### [python-data-patterns](./python-data-patterns/SKILL.md)
Pandas, Polars, and PySpark idioms for production-scale data processing.

- Chunked reads, memory-safe transforms
- Vectorized operations and type optimization
- Row-by-row iteration refactoring

> **When to use:** Python transformation scripts hitting memory errors, slow PySpark jobs, or large CSV/Parquet files that need efficient processing.
>
> **Example:** A script processing 50 million rows crashes with out-of-memory — switching to chunked Pandas reads or Polars lazy evaluation.

---

### [schema-design](./schema-design/SKILL.md)
Data modeling for analytical workloads.

- Star schema, snowflake schema, one big table (OBT)
- Slowly changing dimensions (SCD Types 1, 2, 3)
- Grain definition, surrogate keys, normalization tradeoffs

> **When to use:** Designing or reviewing a data warehouse schema, planning fact and dimension table layouts, structuring data for BI tools.
>
> **Example:** Modeling SaaS subscription events to support both current-state dashboards and historical trend analysis — choosing the right SCD type for the subscription dimension.

---

### [data-quality](./data-quality/SKILL.md)
Systematic data quality checks and monitoring for production pipelines.

- Great Expectations suites, dbt tests
- Null/type/range/referential integrity assertions
- Anomaly detection and alerting patterns

> **When to use:** Setting up validation before or after a load step, adding tests to dbt models, or when stakeholders keep finding incorrect numbers.
>
> **Example:** Finance reports revenue figures look off — adding row count checks, null rate monitoring, and a statistical anomaly alert to catch issues before the dashboard refreshes.

---

## Orchestration and Scheduling

### [orchestration-patterns](./orchestration-patterns/SKILL.md)
DAG design for Airflow, Prefect, and Dagster.

- Task dependencies and idempotency
- Retry strategies with exponential backoff
- SLA monitoring, sensors, and backfill

> **When to use:** Building a scheduled multi-step pipeline, handling task failures, designing retries or alerts, or reprocessing historical data.
>
> **Example:** A nightly pipeline fails silently on step 3 — adding per-task retries, a Slack failure callback, and a 6am SLA alert to catch it before the business day starts.

---

### [streaming-patterns](./streaming-patterns/SKILL.md)
Event-driven pipeline design for Kafka, Flink, Kinesis, and Spark Structured Streaming.

- Consumer groups, partitioning, exactly-once semantics
- Windowing, watermarks, late-arriving data
- Lag monitoring and stream-table joins

> **When to use:** Batch latency is too high for business requirements, redesigning a pipeline as streaming, or dealing with Kafka consumer lag and late events.
>
> **Example:** Building a fraud alert dashboard with sub-minute latency — evaluating whether streaming is warranted, then configuring Kafka partitioning and Flink watermarks.

---

## Cloud Infrastructure

### [cloud-infra-data](./cloud-infra-data/SKILL.md)
AWS, GCP, and Azure data service selection and configuration.

- S3/GCS/ADLS storage layout and partitioning
- BigQuery clustering, Redshift distribution keys, Snowflake warehouses
- IAM roles and least-privilege access for pipelines

> **When to use:** Deploying a pipeline to cloud, choosing between managed services, configuring a data lake, or setting up IAM permissions.
>
> **Example:** Designing a new data lake on AWS — S3 bucket layout, Parquet file sizing, Glue catalog setup, and IAM role structure following least-privilege from day one.

---

### [cost-optimization-data](./cost-optimization-data/SKILL.md)
Find and reduce cloud data warehouse costs.

- Diagnostic queries for BigQuery, Snowflake, Redshift
- Partition pruning enforcement, clustering strategies
- On-demand vs. slot reservations, storage tiering (S3 lifecycle)

> **When to use:** Cloud bill spikes unexpectedly, a query is scanning too much data, or deciding between on-demand and reserved capacity.
>
> **Example:** BigQuery bill doubled after new analysts started running ad-hoc queries — diagnosing top queries by bytes scanned and enabling `require_partition_filter` on large tables.

---

## Transformation and Modeling

### [dbt-patterns](./dbt-patterns/SKILL.md)
dbt project structure and best practices for analytics engineering.

- Staging / intermediate / mart layer architecture
- `ref` and `source` conventions, schema tests
- Incremental models, macros, source freshness checks

> **When to use:** Writing or reviewing dbt models, configuring tests, designing model layers, asking about incremental strategies, or troubleshooting `dbt run` failures.
>
> **Example:** Team adopts dbt for the first time — establishing the three-layer model structure, writing staging models that cast types correctly, and adding `unique` and `not_null` tests on primary keys.

---

## Data Reliability and Governance

### [data-contracts](./data-contracts/SKILL.md)
Define and enforce schema contracts between producer and consumer teams.

- Field types, nullability, allowed values in YAML
- Breaking vs. non-breaking change classification
- Enforcement via Great Expectations, dbt contracts, or Schema Registry

> **When to use:** Upstream schema changes keep breaking downstream pipelines silently, or a team wants to formalize what a dataset promises to its consumers.
>
> **Example:** CRM team renamed a column and the analytics pipeline broke 3 hours later — defining a YAML contract, classifying the rename as breaking, and adding a CI check that catches the mismatch before deployment.

---

### [data-governance](./data-governance/SKILL.md)
PII classification, access control, lineage, and compliance.

- PII tier tagging (public / internal / confidential / restricted)
- Column-level access control (BigQuery policy tags, Snowflake RBAC)
- Audit logging, data retention, right-to-erasure workflows

> **When to use:** Subject to PDPA, GDPR, or HIPAA; an audit requires proof of data access; PII fields need classification; or building a data catalog.
>
> **Example:** Preparing for a PDPA audit — classifying PII fields, applying BigQuery column-level policies, and writing an audit log query showing who accessed sensitive data in the last 30 days.

---

### [data-migration](./data-migration/SKILL.md)
Safe, validated data migration between systems without downtime.

- Big bang vs. incremental vs. dual-write strategy selection
- Checksum and row count validation across systems
- Shadow reads, zero-downtime cutover sequence, rollback planning

> **When to use:** Migrating from one database or warehouse to another, replacing a legacy pipeline, or planning a cutover that cannot have downtime.
>
> **Example:** Migrating from a legacy MySQL warehouse to Snowflake — structuring the work into backfill, dual-write, validation, and cutover phases with a tested rollback plan and checksum comparison before switching traffic.

---

## Machine Learning Integration

### [ml-feature-engineering](./ml-feature-engineering/SKILL.md)
Feature pipelines that bridge data engineering and MLOps.

- Point-in-time correct joins (prevents label leakage)
- Training/serving skew prevention via shared compute functions
- Feature store patterns: Feast, internal dbt-based stores
- Label design and feature documentation for ML teams

> **When to use:** ML team needs feature pipelines, debugging a training/serving skew problem, designing point-in-time correct features, or sharing features across multiple models.
>
> **Example:** Churn model performs well in validation but underperforms in production — identifying features computed differently at training vs. serving time, consolidating into a shared function called by both pipelines.

---

## Communication

### [stakeholder-reporting](./stakeholder-reporting/SKILL.md)
Translate technical data failures into clear business communications.

- Incident notification structure (impact, status, ETA)
- Technical-to-plain-language translation guide
- Post-mortem template and weekly pipeline health updates

> **When to use:** Data was late or wrong and a manager or business team needs an explanation, writing incident reports, or communicating SLA breaches.
>
> **Example:** Morning dashboard was 3 hours late and the sales director is asking questions — writing an incident notification that states business impact and resolution time without mentioning Airflow or DAGs.

---

## Utilities

### [xlsx](./xlsx/SKILL.md)
Read, transform, and generate Excel files from data pipelines.

- Multi-sheet workbooks, named ranges, formula preservation
- Generating formatted Excel reports as pipeline outputs

> **When to use:** Working with `.xlsx` files as a data source or output, generating Excel reports for business teams.

---

### [skill-creator](./skill-creator/SKILL.md)
Create and improve skill modules for this directory.

- Draft SKILL.md from a description of intent
- Run evaluation test cases and benchmark quality
- Optimize skill descriptions for reliable triggering

> **When to use:** Creating a new skill, improving an existing one, or running evals to verify skill quality.

---

## Adding a New Skill

```
.claude/skills/
  <skill-name>/
    SKILL.md          # required — frontmatter + instructions
    references/       # optional — large docs loaded on demand
    scripts/          # optional — reusable scripts bundled with the skill
```

1. Create `.claude/skills/<skill-name>/SKILL.md`
2. Add `name` and `description` in YAML frontmatter — the description drives triggering
3. Keep the body under 500 lines; use `references/` for anything larger
4. Add an entry to this README under the appropriate section
