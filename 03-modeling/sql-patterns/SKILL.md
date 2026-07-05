---
name: sql-patterns
description: Best-practice SQL for analytical workloads — window functions, CTEs, query optimization, partitioning strategies, and anti-patterns to avoid. Use this skill whenever the user is writing or reviewing a SQL query that goes beyond a basic SELECT, especially on BigQuery, Snowflake, Redshift, or DuckDB. Trigger on mentions of aggregations, ranking, running totals, session analysis, lag/lead comparisons, deduplication, slowly-changing lookups, or any time the user asks "how do I write a query for X". Also trigger when a query looks slow, returns wrong results, or the user asks for a code review of existing SQL.
---

# SQL Patterns for Analytics

## When to use this skill

Reach for these patterns any time you're writing transformations, aggregations, or analytical queries on a columnar warehouse (BigQuery, Snowflake, Redshift, DuckDB, Spark SQL). The goal is SQL that is readable, correct, and runs efficiently at scale — not just SQL that produces the right answer on a small sample.

---

## CTEs — Structure First, Optimize Later

Break complex logic into named stages. Each CTE should do one thing and have a name that reads like a sentence fragment explaining what it holds.

```sql
WITH
  active_users AS (
    SELECT user_id, MAX(event_time) AS last_seen
    FROM events
    WHERE event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
    GROUP BY user_id
  ),
  churned AS (
    SELECT u.user_id
    FROM users u
    LEFT JOIN active_users a USING (user_id)
    WHERE a.user_id IS NULL
  )
SELECT * FROM churned;
```

**Why this matters:** Deep subqueries collapse context. A CTE chain lets any reader (including future you) audit each stage independently. Optimizers on modern warehouses inline CTEs anyway — readability is free.

---

## Window Functions

### Running totals and moving averages

```sql
SELECT
  event_date,
  revenue,
  SUM(revenue) OVER (ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue,
  AVG(revenue) OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)          AS revenue_7d_avg
FROM daily_revenue;
```

### Ranking within a group

```sql
SELECT *
FROM (
  SELECT
    user_id,
    session_id,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY started_at DESC) AS rn
  FROM sessions
)
WHERE rn = 1; -- latest session per user
```

Use `ROW_NUMBER()` when you need exactly one row. Use `RANK()` when ties should share a rank. Use `DENSE_RANK()` when there should be no gaps after ties.

### Lag / Lead comparisons

```sql
SELECT
  user_id,
  event_date,
  LAG(event_date) OVER (PARTITION BY user_id ORDER BY event_date) AS prev_event_date,
  DATE_DIFF(event_date, LAG(event_date) OVER (PARTITION BY user_id ORDER BY event_date), DAY) AS days_since_prev
FROM events;
```

Tip: define the window once with a named window clause to avoid repeating it:

```sql
SELECT
  user_id,
  event_date,
  LAG(event_date)  OVER w AS prev_date,
  LEAD(event_date) OVER w AS next_date
FROM events
WINDOW w AS (PARTITION BY user_id ORDER BY event_date);
```

---

## Deduplication

The safest way to deduplicate is `ROW_NUMBER()`, not `DISTINCT` — `DISTINCT` works only when every column is identical.

```sql
WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY user_id, event_type ORDER BY created_at DESC) AS rn
  FROM raw_events
)
SELECT * EXCEPT (rn) FROM deduped WHERE rn = 1;
```

---

## Query Optimization Patterns

### Predicate pushdown — filter early

Always filter before joining. Move WHERE conditions as close to the raw table as possible, ideally inside a CTE.

```sql
-- Good: filter before joining
WITH recent_orders AS (
  SELECT * FROM orders WHERE order_date >= '2024-01-01'
)
SELECT u.name, o.amount FROM users u JOIN recent_orders o USING (user_id);

-- Bad: filter after join
SELECT u.name, o.amount
FROM users u JOIN orders o USING (user_id)
WHERE o.order_date >= '2024-01-01';
```

### Partition pruning on BigQuery / Snowflake

Always filter on the partition column when it exists. On BigQuery, this is usually a DATE or TIMESTAMP column declared in `PARTITION BY`. Without it, the query scans the entire table.

```sql
-- Include partition filter to prune scan
WHERE DATE(created_at) BETWEEN '2024-01-01' AND '2024-03-31'
```

### Avoid `SELECT *` in production

Columnar warehouses charge/measure by bytes scanned. Name every column you actually need.

### Use `APPROX_COUNT_DISTINCT` for cardinality estimation

When exact uniqueness counts aren't required (e.g., dashboards), `APPROX_COUNT_DISTINCT` is 10-100x faster and within ~1% accuracy.

---

## Anti-Patterns to Avoid

| Anti-pattern | Why it hurts | Fix |
|---|---|---|
| `SELECT *` in CTEs | Scans unused columns; wastes bytes | Name columns explicitly |
| Correlated subqueries | Executes once per row — O(n) | Rewrite as a JOIN or window function |
| Multiple `COUNT(DISTINCT)` in one query | Forces multiple passes | Use HyperLogLog approx or subquery |
| Non-SARGable predicates | Prevents partition/index use | Avoid `CAST` or functions on filter columns |
| Implicit type coercion in JOINs | Silent cartesian or missed joins | Ensure join key types match |
| `ORDER BY` without `LIMIT` | Sorts entire result set for nothing | Remove or add `LIMIT` |

---

## Idiomatic Patterns by Warehouse

### BigQuery
- Use `DATE_TRUNC`, `TIMESTAMP_TRUNC` for time bucketing
- Prefer `UNNEST` over string splitting
- Use `QUALIFY` for post-window filtering instead of a wrapper CTE when available

### Snowflake
- Use `FLATTEN` for semi-structured JSON
- Use `QUALIFY ROW_NUMBER() OVER (...) = 1` directly in the SELECT

### DuckDB (local / dbt dev)
- `EXCLUDE` and `REPLACE` column modifiers reduce boilerplate
- `PIVOT` / `UNPIVOT` are native
- Read Parquet/CSV directly with `read_parquet()` or `read_csv_auto()`

---

## Query Review Checklist

Before finalizing a query, verify:
- [ ] CTEs named descriptively, one responsibility each
- [ ] Partition/cluster columns present in WHERE
- [ ] No correlated subqueries
- [ ] Window functions use explicit `ROWS BETWEEN` or `RANGE BETWEEN` when order matters
- [ ] Deduplication logic is intentional (row number vs. distinct)
- [ ] Join keys have matching types
- [ ] No `SELECT *` in final output unless prototyping
