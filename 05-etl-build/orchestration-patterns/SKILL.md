---
name: orchestration-patterns
description: Airflow/Prefect/Dagster DAG design — task dependencies, retries, SLAs, backfill strategies, sensors, and failure recovery. Use this skill whenever the user is building or debugging a scheduled pipeline with multiple steps, asking how to handle task failures, setting up retries or alerts, designing a DAG structure, choosing between orchestrators, or dealing with backfill/reprocessing of historical data. Also trigger when the user mentions Airflow operators, Prefect flows, Dagster assets, task queues, or pipeline scheduling — even if they don't say "orchestration" explicitly. If a pipeline has more than two steps and needs to run on a schedule, this skill should be active.
---

# Orchestration Patterns for Data Pipelines

## When this skill applies

Orchestration is the layer that turns a collection of scripts into a reliable, observable pipeline. Reach for these patterns any time you're wiring up tasks that depend on each other, need to run on a schedule, or must recover gracefully from failures. The right design here saves enormous debugging time downstream.

---

## Choosing an Orchestrator

| Orchestrator | Best fit | Watch out for |
|---|---|---|
| **Airflow** | Large teams, mature ecosystem, lots of operators | Heavy setup; scheduler can be a bottleneck |
| **Prefect** | Python-native, quick start, hybrid deployments | Smaller operator ecosystem than Airflow |
| **Dagster** | Asset-centric thinking, strong type system, great UI | Steeper learning curve for teams new to assets |

The biggest split is **task-centric** (Airflow, Prefect) vs **asset-centric** (Dagster). Asset-centric thinking — "what data does this job produce?" rather than "what does this job do?" — makes lineage and freshness checks natural. If you're starting fresh and the team can learn, Dagster is worth the investment.

---

## DAG Structure Principles

### One task, one responsibility

Each task should do exactly one thing that can be individually retried, skipped, or monitored. Avoid "god tasks" that extract, transform, and load in one function — a failure anywhere forces the whole thing to rerun.

```python
# Airflow — split extract/transform/load into separate operators
extract = PythonOperator(task_id="extract_events", python_callable=extract_events)
transform = PythonOperator(task_id="transform_events", python_callable=transform_events)
load = PythonOperator(task_id="load_events", python_callable=load_events)

extract >> transform >> load
```

### Make tasks idempotent

A task is idempotent if running it twice produces the same result as running it once. This is the single most important property for reliable pipelines — it means retries are safe and backfills are predictable.

```python
def load_events(ds, **context):
    # Delete-then-insert on partition date, not append
    conn.execute(f"DELETE FROM events WHERE event_date = '{ds}'")
    conn.execute(f"INSERT INTO events SELECT * FROM staging_events WHERE event_date = '{ds}'")
```

### Parameterize by logical date, not wall-clock time

Use the DAG's logical execution date (`ds` in Airflow, `scheduled_time` in Prefect) rather than `datetime.now()`. This makes backfills predictable — when you rerun yesterday's DAG, it processes yesterday's data, not today's.

---

## Retry and Failure Patterns

### Tiered retry strategy

Not all failures are equal. Transient network errors warrant fast retries; upstream data delays warrant longer waits.

```python
# Airflow default task retry config
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
}
```

Use exponential backoff to avoid hammering a struggling upstream service. Cap the maximum delay so a task doesn't silently wait hours before alerting.

### Alerting on failure

Failures should page someone or post to Slack — silent failures are worse than loud ones.

```python
# Airflow — send Slack alert on failure
def alert_slack(context):
    SlackWebhookOperator(
        task_id="slack_alert",
        http_conn_id="slack_webhook",
        message=f"Task {context['task_instance'].task_id} failed on {context['ds']}",
    ).execute(context)

default_args = {"on_failure_callback": alert_slack}
```

### SLA misses vs. task failures

An SLA miss means the task didn't finish within the expected window — even if it eventually succeeds. Set SLAs on tasks that feed downstream consumers with time expectations.

```python
# Airflow SLA — alert if task hasn't completed within 2 hours of schedule
PythonOperator(
    task_id="load_daily_summary",
    python_callable=load_summary,
    sla=timedelta(hours=2),
)
```

---

## Sensors and External Dependencies

Use sensors when a task depends on something outside the DAG — a file landing in S3, a table partition becoming available, or an upstream DAG completing.

```python
# Airflow — wait for upstream partition
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id="wait_for_events_dag",
    external_dag_id="events_pipeline",
    external_task_id="load_events",
    timeout=3600,         # give up after 1 hour
    poke_interval=120,    # check every 2 minutes
    mode="reschedule",    # free the worker slot while waiting
)
```

Prefer `mode="reschedule"` over `mode="poke"` for long waits — it frees the worker slot so other tasks can run.

---

## Backfill Strategies

Backfills reprocess historical data. The key is that your tasks must be idempotent (see above) and your DAG must be parameterized by logical date.

```bash
# Airflow — backfill a date range
airflow dags backfill my_dag --start-date 2024-01-01 --end-date 2024-01-31

# Run with max concurrency to avoid overwhelming upstream
airflow dags backfill my_dag --start-date 2024-01-01 --end-date 2024-01-31 --max-active-runs 3
```

For very large backfills, consider running in chunks and checking intermediate results rather than firing off 365 runs at once.

---

## Prefect Patterns

```python
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3, retry_delay_seconds=60, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def extract(date: str) -> list:
    ...

@task
def transform(records: list) -> list:
    ...

@flow(name="daily-events-pipeline")
def events_pipeline(date: str):
    raw = extract(date)
    clean = transform(raw)
    load(clean)
```

Cache results with `task_input_hash` so a failed flow can resume from the last successful task rather than restarting from scratch.

---

## Dagster Asset Patterns

```python
from dagster import asset, AssetIn, FreshnessPolicy

@asset(freshness_policy=FreshnessPolicy(maximum_lag_minutes=60))
def raw_events(context) -> pd.DataFrame:
    ...

@asset(ins={"raw_events": AssetIn()})
def clean_events(raw_events: pd.DataFrame) -> pd.DataFrame:
    ...
```

Assets make freshness SLAs first-class — Dagster will alert when `raw_events` is stale. The lineage graph is automatic.

---

## Common Pitfalls

| Pitfall | Why it hurts | Fix |
|---|---|---|
| Tasks that use `datetime.now()` | Backfills process wrong dates | Use logical execution date |
| Append-only load without dedup | Reruns duplicate data | Delete partition before insert |
| Monolithic tasks | One failure reruns everything | Split into extract / transform / load |
| `mode="poke"` on long sensors | Starves worker pool | Use `mode="reschedule"` |
| No alerting on failure | Silent failures go unnoticed for days | Add `on_failure_callback` |
| Unbounded parallelism in backfills | Overwhelms upstream | Set `max_active_runs` |

---

## Design Checklist

Before shipping a new DAG:
- [ ] Every task is idempotent — safe to retry or rerun
- [ ] All date references use logical execution date, not `now()`
- [ ] Retries configured with backoff and a max delay
- [ ] Failure alerts wired up (Slack, PagerDuty, email)
- [ ] SLAs set on tasks with downstream time commitments
- [ ] Sensors use `reschedule` mode for long waits
- [ ] Backfill tested on at least one historical date range
