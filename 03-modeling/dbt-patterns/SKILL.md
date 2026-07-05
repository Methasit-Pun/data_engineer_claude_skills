---
name: dbt-patterns
description: dbt model design, ref chains, sources, tests, macros, incremental strategies, materializations, and documentation best practices. Use this skill whenever the user is writing or reviewing dbt models, configuring dbt tests, designing model layers (staging/intermediate/marts), asking about incremental models, choosing materializations, writing macros, setting up sources.yml, or troubleshooting dbt run/test failures. Also trigger when the user mentions dbt refs, lineage graphs, model dependencies, dbt Cloud, the dbt CLI, or when they want to transform data already in the warehouse using SQL. If the project uses dbt at all, this skill should be active for any transformation questions.
---

# dbt Patterns for Analytics Engineering

## Model Layer Architecture

The most important design decision in a dbt project is how to organize model layers. A clear layer structure means every model has exactly one place it belongs, and anyone reading the project can understand what each model does.

```
models/
  staging/          # 1:1 with source tables — rename, cast, light cleaning only
    stg_salesforce__accounts.sql
    stg_stripe__charges.sql
  intermediate/     # business logic, joins, calculations — not exposed to BI
    int_customer_lifetime_value.sql
    int_churn_features.sql
  marts/            # final business-facing tables, by domain
    core/
      dim_customers.sql
      fct_subscriptions.sql
    finance/
      fct_revenue.sql
```

**Why layers matter:** Staging models are cheap to replace when sources change. Marts are stable surfaces for BI tools. Intermediate is where the hard logic lives — if it's complex, it belongs there, not in a mart. Never skip straight from raw source to mart.

---

## Staging Models

Staging models are one-to-one with source tables. Their only job is to standardize — rename columns to snake_case, cast types, and add trivial derived columns. No joins, no aggregations.

```sql
-- stg_stripe__charges.sql
WITH source AS (
    SELECT * FROM {{ source('stripe', 'charges') }}
),
renamed AS (
    SELECT
        id                                      AS charge_id,
        customer                                AS customer_id,
        amount / 100.0                          AS amount_usd,  -- Stripe stores cents
        currency,
        status,
        CAST(created AS TIMESTAMP)              AS created_at,
        CAST(_sdc_received_at AS TIMESTAMP)     AS loaded_at
    FROM source
)
SELECT * FROM renamed
```

Always cast at staging — downstream models rely on correct types, and fixing a type mismatch once at the source is far better than fixing it in five marts.

---

## Refs and Sources

Use `{{ ref() }}` to reference other dbt models. Use `{{ source() }}` only in staging models — it's how dbt tracks lineage to raw tables.

```sql
-- Correct — marts reference staging via ref()
SELECT c.customer_id, SUM(o.amount) AS lifetime_value
FROM {{ ref('stg_stripe__charges') }} c
JOIN {{ ref('stg_salesforce__accounts') }} a USING (customer_id)
```

Never hardcode schema or table names. `ref()` makes the lineage graph accurate and handles environment-specific schema names automatically.

---

## Incremental Models

Incremental models only process new/changed rows on each run, rather than rebuilding the whole table. Use them when the table is large enough that a full refresh is too slow or expensive.

```sql
-- models/marts/fct_events.sql
{{
  config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge'   -- or 'delete+insert' for partitioned tables
  )
}}

SELECT
    event_id,
    user_id,
    event_type,
    occurred_at
FROM {{ ref('stg_events') }}

{% if is_incremental() %}
    -- Only process rows newer than the max already in the table
    WHERE occurred_at > (SELECT MAX(occurred_at) FROM {{ this }})
{% endif %}
```

**Incremental strategy choices:**
- `merge` — upsert on `unique_key`. Safe, handles late-arriving updates. Default for most warehouses.
- `delete+insert` — delete the affected partition, reinsert. Faster on BigQuery for large partitions.
- `append` — just adds rows, no dedup. Only correct when events are truly immutable and never late.

Always test that `unique_key` actually uniquely identifies a row — a bad unique key silently duplicates data with `merge`.

---

## Materializations

| Materialization | When to use |
|---|---|
| `view` | Staging models, rarely queried directly |
| `table` | Marts and frequently queried intermediate models |
| `incremental` | Large fact tables that grow over time |
| `ephemeral` | Pure CTEs you want to reuse — no storage, inlined at compile time |

Ephemeral models are useful for DRY logic but don't appear in the warehouse or lineage graph, which makes debugging harder. Prefer intermediate tables for anything complex.

---

## Tests

dbt has two kinds of tests: schema tests (in YAML) and singular tests (SQL files that should return zero rows).

### Schema tests

```yaml
# models/staging/schema.yml
models:
  - name: stg_stripe__charges
    columns:
      - name: charge_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_salesforce__accounts')
              field: customer_id
      - name: status
        tests:
          - accepted_values:
              values: ['succeeded', 'pending', 'failed', 'refunded']
```

Run `dbt test` after every `dbt run` in CI. Failing tests should block deployment.

### Singular tests

```sql
-- tests/assert_revenue_non_negative.sql
-- This query should return zero rows
SELECT *
FROM {{ ref('fct_revenue') }}
WHERE total_revenue < 0
```

Use singular tests for business rules that don't fit the four built-in schema test types.

### dbt-utils / dbt-expectations

The `dbt-utils` package adds useful generic tests:

```yaml
- name: fct_subscriptions
  tests:
    - dbt_utils.unique_combination_of_columns:
        combination_of_columns: ['customer_id', 'subscription_month']
```

Add `dbt-utils` and `dbt-expectations` to `packages.yml` — they cover most of what you'd write as custom singular tests.

---

## Sources

Declare raw tables as sources so dbt tracks lineage from the warehouse all the way back to the source system.

```yaml
# models/staging/sources.yml
sources:
  - name: stripe
    database: raw
    schema: stripe_fivetran
    freshness:
      warn_after: {count: 6, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: _sdc_received_at
    tables:
      - name: charges
      - name: customers
```

The `freshness` block enables `dbt source freshness` to alert when upstream data stops arriving — an early warning before it causes bad numbers in dashboards.

---

## Macros

Use macros to avoid copy-pasting SQL logic across models. A good macro has a clear input/output and explains the business reason it exists.

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name) %}
    ({{ column_name }} / 100.0)
{% endmacro %}

-- usage in a model
SELECT {{ cents_to_dollars('amount') }} AS amount_usd
```

```sql
-- macros/generate_date_spine.sql — useful for filling gaps in time series
{% macro date_spine(start_date, end_date) %}
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ start_date ~ "' as date)",
        end_date="cast('" ~ end_date ~ "' as date)"
    ) }}
{% endmacro %}
```

Keep macros small and composable. If a macro is longer than ~20 lines, consider whether it belongs as a model instead.

---

## Documentation

Document models and columns in YAML — dbt generates a searchable site from these.

```yaml
models:
  - name: fct_subscriptions
    description: >
      One row per customer per month. The grain is (customer_id, subscription_month).
      Includes all active, churned, and paused subscriptions.
    columns:
      - name: customer_id
        description: FK to dim_customers
      - name: subscription_month
        description: First day of the calendar month (always the 1st)
      - name: mrr_usd
        description: Monthly recurring revenue in USD. Null for churned/paused customers.
```

Run `dbt docs generate && dbt docs serve` to browse the lineage graph and column descriptions. A well-documented project is the difference between a team that trusts the data and one that doesn't.

---

## CI/CD Pattern

```yaml
# .github/workflows/dbt_ci.yml
- name: dbt build (slim CI)
  run: |
    dbt build \
      --select state:modified+ \   # only changed models and their dependents
      --defer \                     # use prod manifest for unmodified upstream models
      --state prod-manifest/
```

`state:modified+` with `--defer` is the key pattern for fast CI — it only runs what changed, borrowing results for everything else from production.

---

## Project Checklist

- [ ] Model layers are clear: staging / intermediate / marts
- [ ] `{{ source() }}` used only in staging; `{{ ref() }}` everywhere else
- [ ] Every staging model casts types explicitly
- [ ] All mart models have `unique` and `not_null` tests on primary keys
- [ ] Incremental models have a tested `unique_key`
- [ ] Sources have freshness checks configured
- [ ] CI runs `dbt build --select state:modified+`
- [ ] `dbt docs generate` produces an accurate lineage graph
