---
name: data-architecture
description: Objective-first data architecture design and decision framework. Establish the target object FIRST (BI, ML, or both), elicit requirements and constraints (cloud vs. on-prem, resource/compute budget, team skills, latency and freshness SLAs, compliance), then design the layered architecture and recommend a model — evaluating cost-effectiveness per layer across Azure, AWS, and GCP. Use this skill whenever the user is planning a new data platform or a major redesign, deciding cloud vs. on-prem, choosing between clouds/warehouses at the architecture level, or asking "how should I structure this whole thing". This is a ROUTER/FRAMEWORK: it owns objective and constraint elicitation and the layer design, and delegates cloud-service specifics to cloud-infra-data and pricing detail to cost-optimization-data.
origin: grouped
---

# Data Architecture Design (Framework + Router)

Architecture is decided by the **objective and the constraints**, not by tooling fashion. This skill runs the decision conversation top-down, then routes to the focused cloud and cost skills for the specifics. Don't restate cloud/pricing detail here — **invoke the sub-skills**.

## Step 1 — Know the object first

Before anything, pin down what the platform is *for*. The object changes every downstream choice.

| Objective | What it demands of the architecture |
|---|---|
| **BI / analytics** | Batch-friendly, star/OBT models, warehouse-centric, cost per query matters, SQL consumers |
| **ML** | Point-in-time-correct features, offline+online stores, reproducibility, feature freshness → hands to [[ml-feature-engineering]] |
| **Both** | Shared curated layer feeding a BI mart and an ML feature layer — design the split explicitly |
| **Operational / real-time** | Streaming backbone, low latency → hands to [[data-pipelines]] (streaming) |

## Step 2 — Elicit requirements & constraints (ask, don't assume)

Work through these with the user explicitly. Missing answers are the #1 cause of wrong architecture.

- **Deployment:** cloud, on-prem, or hybrid? Any mandate (data residency, existing contracts)?
- **Resource budget:** how much compute can you actually run? How many concurrent jobs? Team size and skills (SQL-only? Spark? Kubernetes)?
- **Cost ceiling:** monthly budget, and is spend fixed (reserved) or variable (on-demand)?
- **Latency / freshness SLA:** real-time, hourly, daily? Drives batch vs. streaming.
- **Volume & growth:** current size and 12-month projection (from [[data-sourcing]] inventory + [[data-profiling]] sizing).
- **Compliance:** PII, PDPA/GDPR/HIPAA, audit → hands to [[data-reliability]] (governance).

### On-prem vs. cloud — the short decision

- **Cloud** when: elastic/spiky load, small ops team, fast start, variable volume, want managed services.
- **On-prem** when: hard data-residency mandate, very stable predictable load at large scale where owned hardware beats rental, or existing heavy investment.
- **Hybrid** when: sensitive data must stay on-prem but burst compute is cheaper in cloud.

## Step 3 — Design the layers, price each one

Design layer by layer. For **each layer**, choose the service and estimate cost — and this is where you route.

| Layer | Purpose | Decide | Route to |
|---|---|---|---|
| Ingestion | Get data in (batch/stream/CDC) | Connector, batch vs. stream | [[data-pipelines]] |
| Storage (raw) | Durable landing zone | Object store layout, format, partitioning | `cloud-infra-data` |
| Processing | Transform/curate | Warehouse-native SQL vs. Spark vs. serverless | `cloud-infra-data` |
| Serving | BI marts / ML features / APIs | Warehouse, feature store, cache | `cloud-infra-data`, [[ml-feature-engineering]] |
| Cost of each | Is this layer cost-effective? | On-demand vs. reserved, tiering | `cost-optimization-data` |

**Routing is mandatory here:** for any cloud service selection call `Skill(skill="cloud-infra-data")`; for any per-layer or whole-platform cost estimate call `Skill(skill="cost-optimization-data")`. Combine their output into the architecture — don't guess pricing or service limits from memory.

## Step 4 — Cross-cloud cost-effectiveness

When the cloud isn't mandated, compare Azure / AWS / GCP **per layer** rather than picking a vendor wholesale — the cheapest storage and the cheapest compute are often on different clouds, but egress and integration cost usually favor consolidating. Get the concrete numbers from `cost-optimization-data` and the service mapping from `cloud-infra-data`, then recommend:

- A **primary cloud** (consolidation usually wins on egress + ops simplicity), and
- The **warehouse/engine** (BigQuery vs. Snowflake vs. Redshift vs. Synapse vs. Databricks) justified against the objective and budget.

## Step 5 — Recommend & document

Deliver: a one-page architecture (layers + chosen services), the model recommendation (BI/ML/both), the constraint assumptions it rests on, and the estimated cost per layer. State what would change the recommendation (e.g. "if volume 10×, switch to reserved slots").

## Hand-off

Feeds the build → [[data-pipelines]] and [[medallion-design]]; the model → [[data-modeling]]; governance → [[data-reliability]]. Upstream inputs come from [[data-sourcing]] and [[data-profiling]]. Lifecycle overview: [[data-lifecycle]].
