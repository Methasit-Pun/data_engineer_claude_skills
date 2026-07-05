---
name: data-modeling
description: Umbrella skill for shaping data once it has landed — warehouse schema design (star/snowflake/OBT/SCD/grain), SQL for analytics (window functions, CTEs, optimization), dbt model layers/tests/macros/incrementals, and Python transforms (pandas/Polars/PySpark performance). Use this whenever the user is designing tables, writing or reviewing analytical SQL, building dbt models, or writing Python data transformations and it isn't yet clear which tool dominates. This skill ROUTES to the focused sub-skills (schema-design, sql-patterns, dbt-patterns, python-data-patterns) and pulls in more than one when a task spans them. Trigger on: fact/dimension design, table grain, SCD, window functions, deduplication, dbt refs/materializations, or slow/memory-heavy DataFrame code.
origin: grouped
---

# Data Modeling & Transformation (Router)

This is a **router skill**. It groups the four skills that deal with structuring and transforming data after it lands in the warehouse or a processing job. Diagnose which sub-area(s) the task touches, then **invoke the matching sub-skill(s) with the Skill tool**.

## How to route

| If the task is about… | Invoke sub-skill |
|---|---|
| Modeling business entities: star/snowflake schema, one-big-table, slowly changing dimensions, grain definition, surrogate keys, normalization tradeoffs | `schema-design` |
| Writing/reviewing analytical SQL: window functions, CTEs, ranking, running totals, dedup, query optimization, anti-patterns (BigQuery/Snowflake/Redshift/DuckDB) | `sql-patterns` |
| dbt work: model layers (staging/intermediate/marts), refs, sources, tests, macros, materializations, incremental strategies | `dbt-patterns` |
| Python transforms: pandas/Polars/PySpark idioms, chunked reads, memory-safe transforms, vectorization, performance | `python-data-patterns` |

## Routing rules

- **Start with `schema-design` when the table doesn't exist yet** — decide grain and layout before writing SQL against it. A bad schema is expensive to fix later.
- **SQL running inside dbt** pulls in both `sql-patterns` (the query itself) and `dbt-patterns` (materialization, refs, tests).
- **Transformation in Python vs. SQL undecided?** Warehouse-native transforms → `sql-patterns`/`dbt-patterns`; external/large-file or ML-adjacent processing → `python-data-patterns`.
- **Row-by-row DataFrame iteration or memory errors** → `python-data-patterns` immediately.
- Invoke via the Skill tool by name, e.g. `Skill(skill="dbt-patterns")`. Combine outputs; don't paraphrase from memory.

## Related groups

- Getting data into the warehouse first → [[data-pipelines]]
- Testing/validating the models you build → [[data-reliability]]
- Warehouse cost of these queries → [[cloud-data-infra]]
