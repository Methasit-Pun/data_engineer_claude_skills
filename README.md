# Data Engineering Skills Directory

Claude Code skill modules for data engineering work, organized by **stage of the data engineering lifecycle**. Each skill is a self-contained folder with a `SKILL.md`. Reference a skill by name in your prompt, or let it activate when a relevant task is detected.

> **New here?** Start at **[`00-start-here/data-lifecycle`](./00-start-here/data-lifecycle/SKILL.md)** — it routes you to the right stage for whatever you're doing.

---

## The lifecycle

```
01 Sourcing → 02 Profiling/ER → 03 Modeling → 04 Architecture → 05 ETL Build → 06 Reliability → 07 ML Delivery → 08 Code Quality → 09 Routine
```

Each stage is a numbered folder with its own README index. Follow the arrows for a greenfield project, or jump straight to your stage.

| Stage | Folder | Skills | What it answers |
|---|---|---|---|
| **00** | [start-here](./00-start-here/) | data-lifecycle | Where do I start? (master router) |
| **01** | [sourcing](./01-sourcing/) | data-sourcing | What data do I need, and where do I get it? |
| **02** | [profiling](./02-profiling/) | data-profiling | What does the data actually look like? (+ ER diagram) |
| **03** | [modeling](./03-modeling/) | data-modeling, schema-design, sql-patterns, dbt-patterns, python-data-patterns | How is the data modeled and transformed? |
| **04** | [architecture](./04-architecture/) | data-architecture, cloud-data-infra, cloud-infra-data, cost-optimization-data | How is the platform structured, on what infra, at what cost? |
| **05** | [etl-build](./05-etl-build/) | medallion-design, data-pipelines, pipeline-design, orchestration-patterns, streaming-patterns, data-migration | How do I build the pipeline? |
| **06** | [reliability](./06-reliability/) | data-reliability, data-quality, data-contracts, data-governance | Is the data correct, trustworthy, compliant? |
| **07** | [ml-delivery](./07-ml-delivery/) | ml-feature-engineering | Serve to ML. |
| **08** | [code-quality](./08-code-quality/) | notebook-refactor | Make the code reviewable. |
| **09** | [routine](./09-routine/) | stakeholder-reporting | Recurring reporting & incident comms. |

---

## Routers (grouped entry points)

Some folders include a **router skill** that fans out to the focused skills beneath it — call the router when you're not sure which specific skill applies, and it invokes the right one(s):

| Router | Lives in | Fans out to |
|---|---|---|
| **data-lifecycle** | 00-start-here | every stage (01–08) |
| **data-modeling** | 03-modeling | schema-design, sql-patterns, dbt-patterns, python-data-patterns |
| **cloud-data-infra** | 04-architecture | cloud-infra-data, cost-optimization-data |
| **data-pipelines** | 05-etl-build | pipeline-design, orchestration-patterns, streaming-patterns, data-migration |
| **data-reliability** | 06-reliability | data-quality, data-contracts, data-governance |

---

## Shared code

- **[`utils/`](./utils/)** — copy-paste-able building blocks (bronze/silver/gold, SCD, watermark, metadata, key discovery, quality profiling). `medallion-design` composes these directly.

## Not part of the lifecycle

- **[`_tooling/`](./_tooling/)** — `skill-creator`, `agent-sort` (meta-skills for managing skills).
- **[`_backend/`](./_backend/)** — `api-design`, `backend-patterns` (generic backend, kept for reference).
