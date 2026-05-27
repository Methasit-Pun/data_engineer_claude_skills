# Claude Code Skills — Data Engineering

This directory contains skill definitions for Claude Code. Each skill is a focused knowledge module that activates automatically when a relevant task is detected. Skills provide Claude with domain-specific patterns, code examples, checklists, and decision frameworks so you get expert-level guidance without having to re-explain context every session.

---

## How Skills Work

Each skill lives in its own subdirectory as a `SKILL.md` file. The skill's `description` field in the frontmatter determines when it triggers — Claude reads the description and decides whether to consult the skill based on what you're asking. The more specific the description, the more reliably it activates.

You can also reference a skill explicitly by naming it in your prompt.

---

## Skill Index

### Core Data Engineering

---

#### pipeline-design
**What it does:** End-to-end ETL/ELT pipeline design — source connectors, extraction strategies (full load vs. incremental vs. CDC), transform logic, load patterns, idempotency, and error handling.

**When it activates:** Starting a new ingestion pipeline, deciding how data moves from a source into a warehouse or data lake, asking about backfill strategies, retry logic, or orchestration choices.

**Example use case:** Your team needs to ingest Salesforce opportunity data into BigQuery on a daily schedule. The skill guides you through choosing incremental vs. full load, designing an idempotent load pattern, and wiring up error handling.

---

#### sql-patterns
**What it does:** Production-grade SQL for analytical workloads — window functions, CTEs, query optimization, deduplication, partitioning strategies, and warehouse-specific idioms for BigQuery, Snowflake, Redshift, and DuckDB.

**When it activates:** Writing complex SQL queries, reviewing existing SQL for performance or correctness, using aggregations, ranking, running totals, session analysis, or lag/lead comparisons.

**Example use case:** You need to find each user's most recent session and compute a 7-day rolling average of events. The skill provides the correct `ROW_NUMBER()` and `ROWS BETWEEN` window function patterns for your target warehouse.

---

#### python-data-patterns
**What it does:** Pandas, Polars, and PySpark idioms for production data engineering — chunked reads, memory-safe transforms, vectorized operations, type optimization, and performance patterns.

**When it activates:** Writing Python data transformation scripts, handling large CSV or Parquet files, encountering memory errors, optimizing a slow PySpark job, or iterating row-by-row over a DataFrame (a strong signal to refactor).

**Example use case:** A script processing 50 million rows crashes with an out-of-memory error. The skill shows how to switch to chunked reads with Pandas or migrate the logic to Polars with lazy evaluation.

---

#### schema-design
**What it does:** Data modeling for analytical workloads — star schema, snowflake schema, one big table (OBT), slowly changing dimensions (SCD), grain definition, normalization tradeoffs, and surrogate key strategies.

**When it activates:** Designing or reviewing a data warehouse schema, planning fact and dimension table layouts, deciding how to model a business entity, or structuring data for BI tools.

**Example use case:** You need to model subscription events for a SaaS product in a way that supports both current-state dashboards and historical trend analysis. The skill walks through choosing between SCD Type 1, 2, or 3 for the subscription dimension.

---

#### data-quality
**What it does:** Systematic data quality checks — Great Expectations suites, dbt tests, anomaly detection, null/type/range/referential integrity assertions, and monitoring patterns for production pipelines.

**When it activates:** Setting up validation before or after a load step, adding tests to dbt models, detecting when upstream data has changed shape, or when stakeholders keep finding incorrect numbers in reports.

**Example use case:** The finance team reports that revenue figures look off. The skill helps you add row count checks, null rate monitoring, and a statistical anomaly alert that would have caught the issue before the dashboard refreshed.

---

### Orchestration and Scheduling

---

#### orchestration-patterns
**What it does:** Airflow, Prefect, and Dagster DAG design — task dependencies, retry strategies with exponential backoff, SLA monitoring, sensors for external dependencies, and backfill strategies for historical reprocessing.

**When it activates:** Building a scheduled pipeline with multiple steps, handling task failures, setting up retries or alerts, choosing between orchestrators, or reprocessing historical data.

**Example use case:** Your nightly pipeline has five steps and fails silently on step 3, causing downstream tables to go stale. The skill shows how to add per-task retries, a Slack failure alert, and an SLA check that fires if the pipeline hasn't completed by 6am.

---

#### streaming-patterns
**What it does:** Kafka, Flink, Kinesis, and Spark Structured Streaming design — consumer groups, partitioning, exactly-once semantics, windowing, watermarks, lag monitoring, and stream-table joins.

**When it activates:** Redesigning a batch pipeline into streaming, needing real-time or near-real-time data processing, evaluating whether streaming is actually necessary, or dealing with Kafka consumer lag or late-arriving events.

**Example use case:** Stakeholders want a live fraud alert dashboard with sub-minute latency. The skill helps evaluate whether streaming is warranted, then walks through Kafka partitioning strategy, Flink windowing, and watermark configuration for handling late events.

---

### Cloud Infrastructure

---

#### cloud-infra-data
**What it does:** AWS, GCP, and Azure data services — S3/GCS/ADLS storage layout, BigQuery slot management and clustering, Redshift node types and distribution keys, Snowflake virtual warehouses, IAM roles for pipeline access, and managed service selection.

**When it activates:** Deploying a pipeline to cloud, choosing between managed data services, configuring a data lake, setting up IAM permissions, asking about BigQuery pricing or Redshift vs. BigQuery vs. Snowflake.

**Example use case:** Your team is designing a new data lake on AWS. The skill guides the S3 bucket layout, Parquet file sizing, Glue catalog setup, and IAM role structure so the pipeline follows least-privilege access from day one.

---

#### cost-optimization-data
**What it does:** Query cost analysis, partition pruning, slot reservation decisions, storage tiering strategies, and warehouse-specific cost reduction for BigQuery, Snowflake, and Redshift.

**When it activates:** Cloud data bill spikes unexpectedly, a specific query is scanning too much data, trying to understand what is driving costs, or deciding between on-demand and reserved capacity.

**Example use case:** The BigQuery bill doubled after a new analyst started running ad-hoc queries. The skill provides diagnostic SQL to find the top queries by bytes scanned, and then shows how to add `require_partition_filter` to the large tables to prevent full-table scans.

---

### Transformation and Modeling

---

#### dbt-patterns
**What it does:** dbt model design, staging/intermediate/mart layer architecture, `ref` and `source` conventions, schema tests, incremental model strategies, macros, source freshness checks, and documentation best practices.

**When it activates:** Writing or reviewing dbt models, configuring dbt tests, designing model layers, asking about incremental models, writing macros, setting up `sources.yml`, or troubleshooting `dbt run` failures.

**Example use case:** The team is adopting dbt for the first time. The skill establishes the three-layer model architecture, shows how to write staging models that cast types correctly, and sets up `unique` and `not_null` tests on all primary keys.

---

### Data Reliability and Contracts

---

#### data-contracts
**What it does:** Schema contract definition between producer and consumer teams — field types, nullability, allowed values, semantic versioning, breaking vs. non-breaking change classification, and validation enforcement with Great Expectations or dbt contracts.

**When it activates:** Upstream schema changes keep breaking downstream pipelines silently, a team wants to formalize what a dataset promises to its consumers, or assessing the downstream impact of a proposed schema change.

**Example use case:** The CRM team renamed a column and the analytics pipeline broke three hours later when someone noticed the dashboard. The skill shows how to define a YAML contract, classify the rename as a breaking change, and add a CI check that detects the mismatch before deployment.

---

#### data-governance
**What it does:** PII classification and tagging, column-level access control, data lineage tracking (dbt + OpenLineage), audit logging, retention policies, right-to-erasure workflows, and data catalog metadata standards for regulatory compliance.

**When it activates:** Subject to PDPA, GDPR, or HIPAA; an audit requires proof of data access; PII fields need to be identified; setting up role-based data access; or building a data catalog.

**Example use case:** The company is preparing for a PDPA audit and needs to demonstrate which teams can access which customer data fields. The skill provides a PII tier classification system, BigQuery column-level policy tags, and an audit log query that shows who accessed sensitive fields in the last 30 days.

---

#### data-migration
**What it does:** Safe data migration across systems — cutover planning, idempotent backfill patterns, dual-write strategies, checksum-based validation, shadow reads, zero-downtime cutover sequences, and decommissioning checklists.

**When it activates:** Migrating from one database or warehouse to another, replacing a legacy pipeline, performing a major schema change on a live table, or planning a cutover that cannot have downtime.

**Example use case:** The company is migrating from a legacy MySQL warehouse to Snowflake. The skill structures the migration into backfill, dual-write, validation, and cutover phases — with a rollback plan and a checksum comparison query to verify the new system matches the old one before switching traffic.

---

### Machine Learning Integration

---

#### ml-feature-engineering
**What it does:** Feature store patterns, point-in-time correct joins, training/serving skew prevention, feature pipelines, label design, and bridging data engineering conventions with MLOps requirements.

**When it activates:** An ML team needs feature pipelines, building or deciding whether to use a feature store, debugging a training/serving skew problem, designing point-in-time correct feature computation, or when features need to be shared across multiple models.

**Example use case:** The churn model performs well in validation but underperforms in production. The skill identifies the likely cause (features computed differently at training time vs. serving time) and shows how to consolidate the computation into a shared function called by both pipelines.

---

### Communication

---

#### stakeholder-reporting
**What it does:** Translating pipeline metrics, SLA breaches, and data quality failures into clear non-technical summaries — incident notifications, SLA breach communications, post-mortem templates, and regular pipeline health updates.

**When it activates:** Data was late or wrong and someone needs to communicate what happened to a manager or business team, writing incident reports, or explaining a technical failure to a non-technical audience.

**Example use case:** The morning dashboard was three hours late and the sales director is asking questions. The skill provides an incident notification template that explains the impact in business terms, states what is being done, and gives a specific resolution time — without mentioning Airflow or DAGs.

---

### Utilities

---

#### xlsx
**What it does:** Reading, transforming, and writing Excel files — multi-sheet workbooks, named ranges, formatting, formula preservation, and generating Excel reports from data pipelines.

**When it activates:** Working with `.xlsx` files as a data source or output format, generating Excel reports, or processing Excel files received from business teams.

---

#### skill-creator
**What it does:** Creating new skills, iteratively improving existing ones, running evaluation test cases, benchmarking skill performance with qualitative and quantitative metrics, and optimizing skill descriptions for better trigger accuracy.

**When it activates:** Creating a skill from scratch, editing or improving an existing skill, running evals to test skill quality, or optimizing a skill's description.

---

## Skill Summary Table

| Skill | Domain | Use Frequency |
|---|---|---|
| pipeline-design | Ingestion | Daily |
| sql-patterns | Transformation | Daily |
| python-data-patterns | Transformation | Daily |
| schema-design | Modeling | Often |
| data-quality | Reliability | Often |
| orchestration-patterns | Scheduling | Often |
| dbt-patterns | Transformation | Often |
| streaming-patterns | Real-time | Sometimes |
| cloud-infra-data | Infrastructure | Sometimes |
| cost-optimization-data | Infrastructure | Sometimes |
| data-contracts | Reliability | Sometimes |
| data-governance | Compliance | Sometimes |
| data-migration | Infrastructure | Rarely |
| ml-feature-engineering | ML Integration | Rarely |
| stakeholder-reporting | Communication | Rarely |
| xlsx | Utilities | As needed |
| skill-creator | Meta | As needed |

---

## Adding New Skills

1. Create a directory under `.claude/skills/<skill-name>/`
2. Add a `SKILL.md` file with YAML frontmatter containing `name` and `description`
3. Write the skill body in Markdown — keep it under 500 lines
4. Add an entry to this README under the appropriate section
5. Test that the description is specific enough to trigger reliably on relevant prompts
