---
name: data-pipelines
description: Umbrella skill for moving data from source to destination — end-to-end ETL/ELT design, DAG orchestration, real-time streaming, and system-to-system migration. Use this whenever the user is building, scheduling, debugging, or migrating a pipeline and it isn't yet clear which sub-area dominates. This skill ROUTES to the focused sub-skills (pipeline-design, orchestration-patterns, streaming-patterns, data-migration) and pulls in more than one when a task spans them (e.g. "design a streaming pipeline and orchestrate its backfill"). Trigger on: new ingestion job, batch vs. streaming choice, Airflow/Prefect/Dagster DAGs, CDC, backfill, cutover, dual-write, or "move data from X to Y".
origin: grouped
---

# Data Pipelines (Router)

This is a **router skill**. It groups the four skills that deal with getting data from a source into a destination and keeping it flowing. Diagnose which sub-area(s) the task touches, then **invoke the matching sub-skill(s) with the Skill tool**. For a task that spans areas, invoke several and combine their guidance.

## How to route

| If the task is about… | Invoke sub-skill |
|---|---|
| Designing a new ingestion job end-to-end: extraction strategy, idempotency, load patterns, raw/staging/marts layers, ELT vs ETL | `pipeline-design` |
| Scheduling multi-step pipelines, task dependencies, retries, SLAs, sensors, failure recovery, choosing between Airflow/Prefect/Dagster | `orchestration-patterns` |
| Real-time / near-real-time processing, Kafka/Flink/Kinesis/Spark Structured Streaming, consumer lag, windowing, exactly-once, late data | `streaming-patterns` |
| Moving between systems safely: cutover planning, backfill, dual-write, shadow reads, validation, rollback, retiring a legacy pipeline | `data-migration` |

## Routing rules

- **Default new-pipeline questions to `pipeline-design` first** — it frames source shape, volume, change pattern, and downstream contract, which the others build on.
- **Batch vs. streaming undecided?** Invoke `streaming-patterns` (it contains the stream-vs-batch decision) before committing to an architecture.
- **Anything scheduled with >2 dependent steps** also pulls in `orchestration-patterns`.
- **Replacing or moving off an existing system** always pulls in `data-migration`, usually alongside `pipeline-design`.
- Invoke via the Skill tool by name, e.g. call `Skill(skill="orchestration-patterns")`. Combine outputs; don't paraphrase from memory.

## Related groups

- Modeling the data once it lands → [[data-modeling]]
- Validating and governing it → [[data-reliability]]
- Where it runs and what it costs → [[cloud-data-infra]]
