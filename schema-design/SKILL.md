---
name: schema-design
description: Data modeling for analytical workloads — star schema, snowflake schema, one big table (OBT), slowly changing dimensions (SCD), normalization tradeoffs, grain definition, and surrogate key strategies. Use this skill whenever the user is designing or reviewing a data warehouse schema, planning a fact/dimension table layout, deciding how to model a business entity (customer, order, event, product), or asking how to handle historical changes to dimension attributes. Also trigger when the user asks about dbt model design, table granularity, or how to structure data for BI tools like Looker, Tableau, or Power BI. Get this right before building the pipeline — a bad schema is expensive to fix later.
---

# Schema Design for Analytical Workloads

## Start with the Grain

The single most important decision in schema design is the **grain**: what does one row represent? Be explicit and precise.

Bad grain definition: "orders"
Good grain definition: "one row per order line item, at the time of fulfillment"

Every column in a fact table must be true at that grain. If you try to mix grains in one table (order-level and line-item-level facts), you'll produce incorrect aggregations and confuse every analyst who touches it.

---

## Star Schema — Default Choice for Analytics

A star schema has one central fact table surrounded by dimension tables. It's optimized for BI tool query patterns — simple JOINs, fast aggregations.

```
fct_orders
  ├── order_id (PK)
  ├── customer_key (FK → dim_customers)
  ├── product_key  (FK → dim_products)
  ├── date_key     (FK → dim_date)
  ├── quantity
  └── revenue_usd

dim_customers
  ├── customer_key (surrogate PK)
  ├── customer_id  (natural/source key)
  ├── name
  ├── country
  └── segment

dim_date
  ├── date_key
  ├── full_date
  ├── year, month, week, day_of_week
  └── is_holiday
```

**Rules:**
- Fact tables contain measures (numbers you aggregate) and foreign keys to dimensions
- Dimension tables contain descriptive attributes (text, categories, dates)
- Dimension tables are denormalized — repeat values rather than normalizing them out

---

## Slowly Changing Dimensions (SCD)

What happens when a customer changes their country, or a product changes its category? Choose the SCD type that matches how history matters.

### SCD Type 1 — Overwrite
No history. Just update the row.

Use when: history doesn't matter (e.g., fixing a typo in a name).

### SCD Type 2 — Add a new row (most common for analytics)
Keep the old row, insert a new one. Mark which is current.

```sql
CREATE TABLE dim_customers (
  customer_key   INT PRIMARY KEY,    -- surrogate key, never reused
  customer_id    VARCHAR,             -- source system natural key
  name           VARCHAR,
  country        VARCHAR,
  segment        VARCHAR,
  valid_from     DATE,
  valid_to       DATE,               -- NULL or '9999-12-31' = current
  is_current     BOOLEAN
);
```

This preserves historical accuracy: an order placed in 2022 when the customer was in "Germany" still shows Germany even if they moved to "France" in 2024.

**dbt implementation:** use `dbt snapshot` — it handles the `valid_from`, `valid_to`, and `is_current` columns automatically.

### SCD Type 3 — Add a column for the previous value
Store only one prior value alongside the current.

Use when: you only ever need "current" and "previous" — not full history.

```sql
ALTER TABLE dim_customers ADD COLUMN prev_country VARCHAR;
```

Rare in practice — SCD Type 2 is usually more flexible.

---

## Surrogate Keys vs. Natural Keys

Always use **surrogate keys** (synthetic integers or UUIDs) as primary keys in dimension tables, not the source system's ID.

Why:
- Source IDs can change, merge, or be reused across systems
- SCD Type 2 requires multiple rows for the same source entity — only surrogate keys can distinguish them
- Joins on integers are faster than on strings

Keep the natural key (`customer_id`) as a separate column for traceability back to the source.

---

## Fact Table Types

| Type | Description | Example |
|---|---|---|
| Transaction fact | One row per event at a point in time | Orders, page views, payments |
| Periodic snapshot | One row per entity per time period | Daily account balance, weekly active users |
| Accumulating snapshot | One row per process lifecycle, updated as milestones complete | Order fulfillment pipeline, loan application |

Choose the type based on the business question, not the source data shape.

---

## One Big Table (OBT) — When to Use It

Denormalize everything into one wide table — no joins required for queries.

**Pros:** Blazing fast queries in columnar stores; great for dashboards that always join the same tables.  
**Cons:** Data redundancy; hard to maintain when dimensions change; not suitable for SCD Type 2.

Use OBT for:
- Final mart tables consumed by a single BI dashboard
- Flat event tables where every query is a simple GROUP BY
- Situations where query latency matters more than storage cost

Avoid OBT as your only model layer — maintain normalized dimension tables upstream and derive OBTs as mart-layer views.

---

## Normalization vs. Denormalization Tradeoff

| | Normalized (3NF) | Denormalized (star/OBT) |
|---|---|---|
| Storage | Efficient | Redundant |
| Query complexity | High (many JOINs) | Low |
| Write performance | Better | Worse |
| Analytics queries | Slow on large scans | Fast on columnar store |
| Data consistency | Enforced by DB | Managed by pipeline |

In a data warehouse, lean toward denormalization. The warehouse doesn't do OLTP writes — you won't pay the write-performance penalty, and you gain enormously on read performance.

---

## Naming Conventions

Consistent naming makes schemas self-documenting:

- `fct_` prefix for fact tables (`fct_orders`, `fct_sessions`)
- `dim_` prefix for dimension tables (`dim_customers`, `dim_products`)
- `stg_` prefix for staging models (`stg_raw_orders`)
- `_key` suffix for surrogate keys (`customer_key`)
- `_id` suffix for natural/source keys (`customer_id`)
- `_at` suffix for timestamps (`created_at`, `updated_at`)
- `_date` suffix for date columns (`order_date`)
- `is_` prefix for booleans (`is_active`, `is_deleted`)
- Measures named as `noun_unit`: `revenue_usd`, `quantity_items`, `duration_seconds`

---

## Schema Review Checklist

- [ ] Grain defined precisely and every column is true at that grain
- [ ] Fact table contains only measures + FK columns (no attributes)
- [ ] Surrogate keys used in all dimension tables
- [ ] SCD strategy chosen for each dimension that can change
- [ ] Naming conventions applied consistently
- [ ] Date dimension exists and has useful attributes (week, quarter, fiscal year, holidays)
- [ ] No measure columns in dimension tables; no attribute columns in fact tables
- [ ] OBT marts derived from normalized upstream models, not built from scratch
