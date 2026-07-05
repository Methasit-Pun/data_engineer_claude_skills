# 05 · ETL Build

Build the pipeline — medallion layers, orchestration, streaming, migration.

| Skill | Use when |
|---|---|
| **medallion-design** | Interactive bronze/silver/gold, gold-first. Wraps `utils/`. |
| **data-pipelines** | Router for the pipeline skills below. |
| **pipeline-design** | ETL/ELT mechanics: extraction, idempotency, loads. |
| **orchestration-patterns** | Airflow/Prefect/Dagster DAGs, retries, SLAs. |
| **streaming-patterns** | Kafka/Flink/Kinesis, exactly-once, windowing. |
| **data-migration** | Cutover, backfill, dual-write, rollback. |

← [04 · Architecture](../04-architecture/) · → next: [06 · Reliability](../06-reliability/)
