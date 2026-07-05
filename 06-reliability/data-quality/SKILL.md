---
name: data-quality
description: Write systematic data quality checks — validation rules, Great Expectations suites, dbt tests, anomaly detection, null/type/range/referential integrity assertions, and monitoring patterns for production pipelines. Use this skill whenever the user is dealing with bad data in a pipeline, setting up validation before or after a load step, adding tests to dbt models, writing Great Expectations expectations, or trying to detect when upstream data has changed shape. Also trigger when stakeholders keep finding incorrect numbers, when a pipeline silently loads garbage, or when the user asks "how do I make sure my data is correct". Prevention is cheaper than debugging.
---

# Data Quality

## Why Data Quality Fails Silently

Pipelines succeed (exit 0) even when they load garbage. A missing WHERE clause, a silent NULL coercion, an upstream schema change — none of these throw exceptions. The result is a dashboard that looks fine until someone cross-references it with reality.

Systematic data quality checks turn silent failures into loud ones. The goal is to catch issues at the pipeline boundary — not after a stakeholder finds them.

---

## Four Dimensions to Check

Every check falls into one of these categories:

| Dimension | What it catches | Examples |
|---|---|---|
| **Completeness** | Missing or null values | `user_id IS NULL`, row count = 0 |
| **Validity** | Values outside expected domain | Negative revenue, future birthdates, unknown status codes |
| **Consistency** | Internal contradictions | `end_date < start_date`, child with no parent |
| **Freshness** | Data is stale or arrived late | Last `updated_at` is 3 days ago when it should be hourly |

---

## dbt Tests — Start Here

dbt's built-in tests cover the most common checks with zero code:

```yaml
# models/staging/schema.yml
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique

      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'processing', 'shipped', 'delivered', 'cancelled']

      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id

      - name: revenue_usd
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
```

Run on every CI push: `dbt test --select stg_orders`.

### dbt_utils and dbt_expectations

Install `dbt-utils` and `dbt-expectations` for extended checks:

```yaml
- name: order_date
  tests:
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: "'2020-01-01'"
        max_value: "current_date"

- name: email
  tests:
    - dbt_expectations.expect_column_values_to_match_regex:
        regex: "^[^@]+@[^@]+\\.[^@]+$"
```

---

## Great Expectations — For Python-Based Pipelines

Use Great Expectations (GE) when you need validation outside dbt, or when you want to checkpoint data mid-pipeline before loading.

### Quick start pattern

```python
import great_expectations as gx

context = gx.get_context()
ds = context.sources.add_pandas("my_source")
da = ds.add_dataframe_asset("orders_asset")

batch = da.get_batch_request()
validator = context.get_validator(batch_request=batch, expectation_suite_name="orders_suite")

# Completeness
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_not_be_null("customer_id")

# Validity
validator.expect_column_values_to_be_in_set("status", ["pending", "shipped", "delivered"])
validator.expect_column_values_to_be_between("revenue_usd", min_value=0)

# Freshness
validator.expect_column_max_to_be_between(
    "created_at",
    min_value=str(datetime.utcnow() - timedelta(hours=25)),
    max_value=str(datetime.utcnow())
)

results = validator.validate()
if not results.success:
    raise ValueError(f"Data quality check failed: {results}")
```

### Checkpoint pattern — run at pipeline ingestion boundary

Place a GE checkpoint immediately after loading raw data and before any transformation. If validation fails, halt the pipeline and alert — don't let bad raw data propagate downstream.

---

## Anomaly Detection Patterns

### Row count deviation check

A pipeline that loads zero rows is almost always a bug, not a business reality.

```python
def assert_row_count(df, min_rows=1, max_deviation_pct=50, baseline=None):
    count = len(df)
    assert count >= min_rows, f"Too few rows: {count}"
    if baseline:
        deviation = abs(count - baseline) / baseline * 100
        assert deviation <= max_deviation_pct, \
            f"Row count {count} deviates {deviation:.1f}% from baseline {baseline}"
```

### Column sum / metric drift

Catch when a key metric shifts unexpectedly between runs:

```sql
-- Store yesterday's total
SELECT SUM(revenue_usd) AS total FROM fct_orders WHERE order_date = CURRENT_DATE - 1;

-- Alert if today's total deviates > 30%
WITH yesterday AS (SELECT 1000000 AS total),  -- parameterize this
     today    AS (SELECT SUM(revenue_usd) AS total FROM fct_orders WHERE order_date = CURRENT_DATE)
SELECT
  today.total,
  yesterday.total,
  ABS(today.total - yesterday.total) / yesterday.total AS deviation
FROM today, yesterday
WHERE ABS(today.total - yesterday.total) / yesterday.total > 0.30;
```

### Schema change detection

Upstream sources change their schemas silently. Check that expected columns exist before loading:

```python
EXPECTED_COLUMNS = {"order_id", "customer_id", "status", "revenue_usd", "created_at"}

def assert_schema(df):
    missing = EXPECTED_COLUMNS - set(df.columns)
    extra = set(df.columns) - EXPECTED_COLUMNS
    assert not missing, f"Missing expected columns: {missing}"
    # Extra columns are usually OK — just log them
    if extra:
        logging.warning(f"Unexpected new columns: {extra}")
```

---

## Null / Type / Range Assertion Templates

### Null checks
```python
# Fail on any null in required columns
for col in ["order_id", "customer_id", "created_at"]:
    null_count = df[col].isna().sum()
    assert null_count == 0, f"Column {col} has {null_count} nulls"
```

### Type checks
```python
import pandas as pd

assert pd.api.types.is_datetime64_any_dtype(df["created_at"]), "created_at must be datetime"
assert pd.api.types.is_numeric_dtype(df["revenue_usd"]), "revenue_usd must be numeric"
```

### Range checks
```python
assert df["revenue_usd"].ge(0).all(), "revenue_usd must be non-negative"
assert df["quantity"].between(1, 10000).all(), "quantity out of expected range"
assert df["order_date"].max() <= pd.Timestamp.today(), "order_date in the future"
```

### Referential integrity
```python
valid_customer_ids = set(customers_df["customer_id"])
orphaned = df[~df["customer_id"].isin(valid_customer_ids)]
assert len(orphaned) == 0, f"{len(orphaned)} orders reference unknown customer_ids"
```

---

## Where to Place Checks in a Pipeline

```
Source → [EXTRACT checks] → Raw load → [FRESHNESS + SCHEMA checks] → Transform → [BUSINESS RULE checks] → Mart load → [METRIC DRIFT checks]
```

- **Extract:** Row count > 0, no schema surprises
- **After raw load:** Freshness, completeness on key columns
- **After transform:** Business rule assertions (no negative revenue, referential integrity)
- **After mart load:** Metric drift vs. prior run, row count sanity

---

## Data Quality Checklist

- [ ] Not-null checks on every required column
- [ ] Unique constraint check on all primary keys
- [ ] Accepted values check for all categorical/enum columns
- [ ] Range checks for all numeric measures
- [ ] Referential integrity between fact and dimension tables
- [ ] Row count > 0 alert
- [ ] Row count deviation alert vs. prior run
- [ ] Freshness check — data arrived within expected window
- [ ] Schema change detection before loading
- [ ] All checks run in CI before merging model changes
