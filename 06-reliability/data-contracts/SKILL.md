---
name: data-contracts
description: Define and enforce schema contracts between producer and consumer teams — field types, nullability, allowed values, versioning, breaking vs. non-breaking changes, and change detection patterns. Use this skill whenever two teams or services share a dataset and upstream changes keep breaking the downstream silently, when a team wants to formalize what a dataset "promises" to its consumers, or when setting up schema validation at pipeline boundaries. Also trigger when the user asks about schema evolution, backward/forward compatibility, schema registries, Great Expectations for inter-team contracts, or when a producer is about to make a "small" schema change and you want to assess its downstream impact. If upstream and downstream are owned by different people, this skill should be active.
---

# Data Contracts

## Why Contracts Exist

Without contracts, schema changes are discovered by broken dashboards or failed pipeline runs — often hours or days after the change shipped. A data contract is a formal agreement between the team that produces a dataset (producer) and the teams that consume it (consumers). It specifies exactly what the dataset promises, and gives consumers a way to know when that promise is broken.

Contracts aren't bureaucracy — they're the interface boundary for data, the same way an API contract is the interface boundary for a service.

---

## Contract Structure

A minimal contract covers:

1. **Schema** — fields, types, nullability, allowed values
2. **Grain** — what one row represents (e.g., "one row per customer per day")
3. **Freshness** — how recent the data is guaranteed to be
4. **SLA** — when the data is available by (e.g., "available by 6am UTC daily")
5. **Owner** — who to contact when something breaks
6. **Version** — the contract version, so consumers can pin to a stable version

### Example contract in YAML

```yaml
# contracts/churn_features_v2.yaml
contract:
  name: churn_features
  version: "2.1.0"
  owner: data-engineering@company.com
  consumers:
    - ml-team@company.com
    - analytics@company.com
  grain: "One row per customer_id per feature_date"
  sla:
    available_by: "06:00 UTC"
    freshness_max_lag_hours: 24

schema:
  - name: customer_id
    type: STRING
    nullable: false
    description: "FK to dim_customers"
  - name: feature_date
    type: DATE
    nullable: false
    description: "The date the features were computed for"
  - name: days_since_last_login
    type: INTEGER
    nullable: true
    description: "Null if customer has never logged in"
  - name: subscription_tier
    type: STRING
    nullable: false
    allowed_values: ["free", "pro", "enterprise"]
  - name: predicted_churn_score
    type: FLOAT
    nullable: true
    range:
      min: 0.0
      max: 1.0
```

---

## Breaking vs. Non-Breaking Changes

Not all schema changes are equal. The producer team needs to know which changes require consumer sign-off before shipping.

| Change | Breaking? | Why |
|---|---|---|
| Add a new nullable column | No | Consumers can ignore it |
| Add a new non-null column with no default | Yes | Existing queries may fail if they `SELECT *` or expect fixed column count |
| Remove a column | Yes | Any consumer using it breaks |
| Rename a column | Yes | Same as remove + add |
| Change type from INT to FLOAT | Usually no | Implicit upcast |
| Change type from FLOAT to INT | Yes | Data loss for fractional values |
| Narrow `allowed_values` (remove a valid value) | Yes | Existing data may violate the new constraint |
| Expand `allowed_values` (add a new value) | No | Consumers may need to handle the new case, but won't break immediately |
| Change grain (e.g., daily → hourly) | Yes | All downstream aggregations are now wrong |
| Change freshness SLA | Depends | If consumers depend on the old window, their pipelines may run against stale data |

The rule of thumb: if removing the old behavior would cause any consumer's code or pipeline to produce wrong results or errors, it's breaking.

---

## Versioning

Use semantic versioning: `MAJOR.MINOR.PATCH`.

- **MAJOR** — breaking changes. Consumers must update before the old version is retired.
- **MINOR** — non-breaking additions (new nullable columns, new allowed values).
- **PATCH** — documentation fixes, description changes, no schema change.

When shipping a breaking change:
1. Release the new version alongside the old one (v1 and v2 both exist)
2. Notify all registered consumers with a migration timeline
3. Give consumers at least N sprints (agree on N with stakeholders) to migrate
4. Deprecate the old version — mark it in the contract as `status: deprecated`
5. Remove the old version after the sunset date

---

## Schema Validation at Pipeline Boundaries

Contracts only work if they're enforced. Add validation at the point where data crosses a team boundary.

### Using Great Expectations

```python
import great_expectations as gx

context = gx.get_context()
ds = context.sources.add_pandas("churn_features")
da = ds.add_dataframe_asset("batch")
batch = da.build_batch_request(dataframe=df)

suite = context.add_expectation_suite("churn_features_v2_contract")
suite.add_expectation(gx.expectations.ExpectColumnToExist(column="customer_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column="subscription_tier", value_set=["free", "pro", "enterprise"]
))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="predicted_churn_score", min_value=0.0, max_value=1.0
))

results = context.run_validation_operator("action_list_operator", assets_to_validate=[batch])
if not results["success"]:
    raise ValueError("Contract validation failed — aborting load")
```

Run this check in the consumer's ingestion pipeline, not just in the producer's — the consumer owns detecting when the data doesn't match what was promised.

### Schema Registry (Kafka / event-driven)

For streaming pipelines, use Confluent Schema Registry to enforce Avro or Protobuf schemas at the topic level.

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

schema_registry_conf = {"url": "http://schema-registry:8081"}
sr_client = SchemaRegistryClient(schema_registry_conf)

# Deserializer enforces that the message matches the registered schema
# — rejects messages that don't conform
avro_deserializer = AvroDeserializer(sr_client)
```

Schema Registry enforces compatibility rules (BACKWARD, FORWARD, FULL) automatically — a producer that tries to register a breaking schema change is rejected.

---

## Change Detection Pattern

The producer should run a diff check before deploying any schema change to a shared dataset. This catches accidental breaking changes.

```python
import pandas as pd

def check_schema_diff(old_schema: dict, new_schema: dict) -> list[str]:
    issues = []
    old_fields = {f["name"]: f for f in old_schema["schema"]}
    new_fields = {f["name"]: f for f in new_schema["schema"]}

    # Removed columns
    for name in old_fields:
        if name not in new_fields:
            issues.append(f"BREAKING: column '{name}' removed")

    # Type changes
    for name, field in new_fields.items():
        if name in old_fields and field["type"] != old_fields[name]["type"]:
            issues.append(f"BREAKING: column '{name}' type changed from {old_fields[name]['type']} to {field['type']}")

    # Nullable changed to non-nullable
    for name, field in new_fields.items():
        if name in old_fields:
            if not field.get("nullable", True) and old_fields[name].get("nullable", True):
                issues.append(f"BREAKING: column '{name}' changed from nullable to non-null")

    return issues
```

Add this check to CI so schema changes are flagged before they reach production.

---

## Consumer Registration

Consumers should be registered in the contract so producers know who to notify when making changes.

```yaml
consumers:
  - team: ml-team
    contact: ml-oncall@company.com
    uses:
      - customer_id
      - feature_date
      - predicted_churn_score
    pipeline: s3://ml-pipelines/churn/ingest.py
    registered: 2024-03-01
    sla_dependency: "needs data by 05:00 UTC for morning model training run"
```

Knowing which columns each consumer uses means you can safely remove columns that nobody is consuming — and you know exactly who to call when a breaking change is unavoidable.

---

## Contracts in dbt

dbt's `contracts` block (available in dbt Core 1.5+) enforces schema contracts at model build time:

```yaml
# models/marts/schema.yml
models:
  - name: fct_churn_features
    config:
      contract:
        enforced: true  # dbt will fail if the model doesn't match the declared schema
    columns:
      - name: customer_id
        data_type: varchar
        constraints:
          - type: not_null
      - name: feature_date
        data_type: date
        constraints:
          - type: not_null
```

When `enforced: true`, dbt compares the model's actual output schema against the declared one and fails the build on mismatch — catching drift before it reaches the warehouse.

---

## Contract Lifecycle Checklist

When shipping a change to a shared dataset:
- [ ] Identified all registered consumers from the contract file
- [ ] Classified the change as breaking or non-breaking
- [ ] For breaking changes: new version deployed alongside old, not replacing it
- [ ] Consumers notified with migration deadline
- [ ] Contract YAML updated with new version number and changelog entry
- [ ] Schema validation checks updated to reflect the new contract
- [ ] Old version marked `status: deprecated` with sunset date
- [ ] CI enforces schema diff check on the contract file
