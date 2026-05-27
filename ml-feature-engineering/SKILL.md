---
name: ml-feature-engineering
description: Feature store patterns, training/serving skew prevention, feature pipelines for ML teams, point-in-time correct joins, and bridging data engineering with MLOps conventions. Use this skill whenever an ML team needs feature pipelines, when building a feature store or deciding whether to use one, when there's a training/serving skew problem (model performance in production differs from validation), when features need to be shared across multiple models, or when designing point-in-time correct feature computation. Also trigger when the user mentions feature stores (Feast, Tecton, Hopsworks), label leakage, backfilling features, offline/online store separation, or when data engineering work feeds directly into model training. If the words "features", "training set", "model pipeline", or "MLOps" appear alongside data engineering questions, this skill should be active.
---

# ML Feature Engineering Patterns

## The Data Engineering / ML Boundary

Data engineers own the data. ML engineers own the models. Feature engineering sits at the boundary — and when it's designed poorly, both sides pay for it. The most common failures:

1. **Training/serving skew** — features computed differently at training time vs. prediction time → model performs worse in production than in validation
2. **Label leakage** — features computed using data from the future, making the training set artificially easy → model fails in production
3. **Feature duplication** — each ML team recomputes the same features independently → inconsistent definitions, wasted compute

The patterns in this skill address all three.

---

## Point-in-Time Correct Joins

This is the most important concept in ML feature engineering. A model trained to predict churn should only "see" data that would have been available at the time of the prediction — not data from the future.

### The wrong way

```sql
-- BAD: This leaks future data into training
-- If we're predicting churn as of 2024-01-15, we shouldn't know about
-- events that happened on 2024-01-20
SELECT
    u.user_id,
    u.subscription_tier,
    COUNT(e.event_id) AS events_last_30d,  -- counts events AFTER the label date!
    u.churned AS label
FROM users u
JOIN events e ON u.user_id = e.user_id
    AND e.event_date >= DATEADD(day, -30, CURRENT_DATE)  -- wrong: uses today's date
WHERE u.label_date = '2024-01-15'
GROUP BY 1, 2, 4;
```

### The right way — as-of join

```sql
-- GOOD: Features are computed as of the label date, using only past data
SELECT
    u.user_id,
    u.subscription_tier,
    COUNT(e.event_id) AS events_last_30d,
    u.churned AS label
FROM user_labels u  -- contains (user_id, label_date, churned)
LEFT JOIN events e ON u.user_id = e.user_id
    AND e.event_date >= DATEADD(day, -30, u.label_date)  -- relative to label date
    AND e.event_date < u.label_date                       -- only past events
GROUP BY 1, 2, 4;
```

The rule: every date range in a feature join must be bounded by `< label_date` on the upper end.

### Point-in-time with slowly-changing dimensions

When a dimension table changes over time (e.g., a user's subscription tier changes), you need the value that was true at `label_date`, not the current value.

```sql
-- Get the subscription tier each user had at each label date
-- (assuming subscription_changes has valid_from/valid_to columns)
SELECT
    l.user_id,
    l.label_date,
    s.subscription_tier AS tier_at_label_date
FROM user_labels l
JOIN subscription_changes s ON l.user_id = s.user_id
    AND s.valid_from <= l.label_date
    AND (s.valid_to > l.label_date OR s.valid_to IS NULL)
```

---

## Feature Pipeline Architecture

A feature pipeline has two paths:
- **Offline** — batch computation, used for training and bulk scoring
- **Online** — low-latency lookup, used for real-time predictions

```
Offline pipeline:
  Raw events → Transform → Feature store (offline) → Training dataset → Model training

Online pipeline:
  Raw events → Transform → Feature store (online) → Real-time prediction API
```

The transform logic must be identical in both paths. If it isn't, you get training/serving skew.

### The serving skew trap

```python
# Training time (Python/Pandas)
df["days_since_login"] = (df["label_date"] - df["last_login_date"]).dt.days

# Serving time (different code, different team)
days_since_login = (datetime.now() - user.last_login).days  # subtly different
```

Any divergence in feature computation between training and serving will degrade model performance in production. The model won't fail — it will just quietly perform worse.

**Prevention:** Keep feature logic in a single, shared function called by both the training pipeline and the serving code. Write integration tests that verify identical output for both paths on the same input.

```python
# Shared feature computation — called by BOTH training pipeline and serving
def compute_engagement_features(user_id: str, as_of_date: date) -> dict:
    events = db.query("""
        SELECT COUNT(*) AS event_count,
               MAX(event_date) AS last_event_date
        FROM events
        WHERE user_id = %s AND event_date < %s
          AND event_date >= %s
    """, user_id, as_of_date, as_of_date - timedelta(days=30))

    return {
        "events_last_30d": events["event_count"],
        "days_since_last_event": (as_of_date - events["last_event_date"]).days
            if events["last_event_date"] else None
    }
```

---

## Feature Store Patterns

A feature store is a system that manages feature computation, storage, and serving. Whether you use a managed one (Feast, Tecton, Hopsworks) or a simple internal one, the key components are the same.

### When you need a feature store

- Multiple models need the same features (churn model, LTV model, propensity model all need engagement features)
- Features need to be available both for offline training and online serving
- You want a catalog of what features exist and who owns them
- You need to backfill features for historical label dates

If only one model exists and it only runs in batch, you may not need a full feature store — a well-structured dbt model is sufficient.

### Simple internal feature store (dbt + BigQuery)

```sql
-- The feature table — one row per (entity, feature_date)
-- Materialized as an incremental dbt model
-- models/feature_store/user_engagement_features.sql

{{
  config(
    materialized='incremental',
    unique_key=['user_id', 'feature_date'],
    partition_by={'field': 'feature_date', 'data_type': 'date'},
  )
}}

SELECT
    user_id,
    DATE(event_date) AS feature_date,
    COUNT(*) AS events_last_30d,
    COUNT(DISTINCT session_id) AS sessions_last_30d,
    MAX(event_date) AS last_event_date,
    DATE_DIFF(DATE(event_date), MAX(event_date), DAY) AS days_since_last_event
FROM {{ ref('stg_events') }}
{% if is_incremental() %}
WHERE event_date >= DATE_SUB((SELECT MAX(feature_date) FROM {{ this }}), INTERVAL 31 DAY)
{% endif %}
GROUP BY 1, 2
```

Training time: query the feature table with a point-in-time join against your label table.
Serving time: query the feature table for `feature_date = today()`.

### Feast (managed feature store)

```python
from feast import FeatureStore, Entity, Feature, FeatureView, FileSource
from feast.types import Float64, Int64

# Define entity
user = Entity(name="user_id", join_keys=["user_id"])

# Define feature view
user_engagement_fv = FeatureView(
    name="user_engagement",
    entities=[user],
    ttl=timedelta(days=1),
    features=[
        Feature(name="events_last_30d", dtype=Int64),
        Feature(name="days_since_last_event", dtype=Int64),
    ],
    source=BigQuerySource(table="project.dataset.user_engagement_features"),
)

store = FeatureStore(repo_path=".")

# Training — get historical features (point-in-time correct automatically)
training_df = store.get_historical_features(
    entity_df=label_df,  # contains user_id and event_timestamp (the label date)
    features=["user_engagement:events_last_30d", "user_engagement:days_since_last_event"],
).to_df()

# Serving — get online features (millisecond latency from Redis/DynamoDB)
features = store.get_online_features(
    features=["user_engagement:events_last_30d"],
    entity_rows=[{"user_id": "user_123"}],
).to_dict()
```

---

## Label Design

### Churn label example

```sql
-- Define churn: no activity for 30 days after a 90-day observation window
-- Label date = the date we're making the prediction
-- We look 30 days forward from label_date to determine if they churned

SELECT
    user_id,
    label_date,
    CASE
        WHEN MAX(event_date) < DATEADD(day, 30, label_date) THEN 1
        ELSE 0
    END AS churned
FROM (
    -- Generate label dates (one per user per week)
    SELECT DISTINCT user_id, date_spine.date AS label_date
    FROM active_users
    CROSS JOIN date_spine
    WHERE date_spine.date BETWEEN '2023-01-01' AND '2023-12-31'
) label_candidates
LEFT JOIN events e ON label_candidates.user_id = e.user_id
    AND e.event_date BETWEEN label_date AND DATEADD(day, 30, label_date)
GROUP BY 1, 2
```

Important: label generation requires future data. The label for a given `label_date` can only be computed 30 days later. Don't use recent dates as labels in training — the churned flag won't be set yet.

---

## Feature Documentation for ML Teams

When data engineers hand features to ML teams, document the grain, computation window, and known caveats:

```yaml
# feature_catalog/user_engagement.yaml
feature_group: user_engagement
entity: user_id
grain: "One row per (user_id, feature_date)"
update_frequency: daily
owner: data-engineering@company.com

features:
  - name: events_last_30d
    type: integer
    description: "Count of all events in the 30 days prior to feature_date"
    computation_window: "feature_date - 30 days to feature_date (exclusive)"
    known_caveats: "Null for users who signed up < 30 days before feature_date"

  - name: days_since_last_event
    type: integer
    description: "Days between feature_date and the most recent event before feature_date"
    computation_window: "All history up to feature_date"
    known_caveats: "Null for users with no recorded events"
```

---

## Checklist: Before Handing Features to the ML Team

- [ ] Point-in-time correctness verified — all feature windows are bounded by `< label_date`
- [ ] Feature computation function is shared between training pipeline and serving code
- [ ] Null values documented — what does a null mean for each feature?
- [ ] Feature distribution checked after backfill (spot-check means and percentiles per month)
- [ ] Label leakage audit — no feature references data that would only be known after the label event
- [ ] Features versioned — if computation logic changes, old features remain accessible
- [ ] Feature documentation written (grain, window, caveats, owner)
- [ ] Integration test confirms offline and online paths produce identical output for same input
