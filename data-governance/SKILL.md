---
name: data-governance
description: Data lineage tracking, PII tagging, access control policies, data catalog metadata standards, retention policies, and audit logging for regulatory compliance. Use this skill whenever the company is subject to PDPA, GDPR, HIPAA, or any data privacy regulation, when an audit requires proof of who accesses what data, when PII fields need to be identified and classified in a dataset, when setting up column-level access control, or when building a data catalog. Also trigger when someone asks about data masking, anonymization, right-to-erasure workflows, role-based data access, or data lineage from source to BI tool. If the word "compliance", "audit", "PII", "sensitive data", or "regulation" appears, this skill should be active.
---

# Data Governance for Data Pipelines

## Why Governance Matters Now, Not Later

Governance work done retroactively costs 10x what it costs upfront. When an audit arrives or a breach happens, you need to answer three questions fast: *What data do we have? Where does it come from? Who can see it?* If you can't answer these from a system, you'll answer them from a spreadsheet — under pressure, with incomplete information.

---

## PII Classification

### Sensitivity tiers

Classify every field that touches personal data before it enters the warehouse.

| Tier | Examples | Default access |
|---|---|---|
| **Public** | Country, product tier, aggregated metrics | All authenticated users |
| **Internal** | User ID, subscription status, behavioral events | Analysts and engineers |
| **Confidential** | Email, full name, phone number | Restricted to specific roles |
| **Restricted** | Payment card data, government ID, health data | Named individuals only, logged |

### Tagging in dbt

```yaml
# models/staging/schema.yml
models:
  - name: stg_users
    columns:
      - name: user_id
        meta:
          pii_tier: internal
      - name: email
        meta:
          pii_tier: confidential
          pii_type: email_address
          regulation: [GDPR, PDPA]
      - name: full_name
        meta:
          pii_tier: confidential
          pii_type: name
```

### Tagging in BigQuery (policy tags)

```sql
-- Assign a policy tag to restrict column-level access
ALTER TABLE `project.dataset.users`
ALTER COLUMN email
SET OPTIONS (
  description = 'User email address — PII confidential',
  policy_tags = ['projects/my-project/locations/us/taxonomies/123/policyTags/456']
);
```

With BigQuery column-level security, queries that touch `email` fail unless the caller has `datacatalog.categoryFineGrainedReader` on that policy tag. This is enforced at the engine level — no application code needed.

---

## Lineage Tracking

Lineage answers: "If source table X changes, what downstream reports break?" and "This dashboard shows wrong numbers — where did the data come from?"

### dbt lineage (automatic)

dbt builds a lineage graph automatically from `{{ ref() }}` and `{{ source() }}` calls. Run `dbt docs generate` to make it browsable.

```bash
dbt docs generate
dbt docs serve  # opens browser with full lineage DAG
```

For cross-system lineage (Airflow → dbt → BI tool), use OpenLineage or Marquez to emit lineage events from each system into a shared graph.

### OpenLineage integration

```python
from openlineage.airflow import OpenLineagePlugin
# Add the plugin — Airflow auto-emits START/COMPLETE/FAIL events
# with input/output dataset metadata for every task run

# Result: Marquez or DataHub shows the full lineage:
# S3 raw file → Airflow extract task → BigQuery staging → dbt model → Looker dashboard
```

---

## Access Control Patterns

### Role-based access (RBAC)

Define roles at the business level, not at the individual level. Granting access to people directly means access is never revoked when someone changes teams.

```sql
-- Snowflake RBAC example
CREATE ROLE analyst_read;
GRANT USAGE ON DATABASE analytics TO ROLE analyst_read;
GRANT USAGE ON SCHEMA analytics.marts TO ROLE analyst_read;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics.marts TO ROLE analyst_read;

-- Restricted role — only for data engineering
CREATE ROLE data_eng_pii;
GRANT SELECT ON TABLE analytics.raw.users TO ROLE data_eng_pii;

-- Assign roles to groups, not individuals
GRANT ROLE analyst_read TO ROLE sysadmin;  -- inherited by all analysts via AD group
```

### Row-level security

When different teams should see different subsets of the same table (e.g., regional sales teams):

```sql
-- BigQuery row-level access policy
CREATE ROW ACCESS POLICY region_filter
ON `project.dataset.sales`
GRANT TO ("group:apac-team@company.com")
FILTER USING (region = 'APAC');
```

### Minimum access principle in practice

- Pipelines get read-only access to their source, write access to their specific output schema only
- Analysts get read-only access to marts, never to raw or staging
- No individual should have write access to production tables — only service accounts
- All privileged access (PII tiers Confidential/Restricted) requires manager approval and is time-limited

---

## Audit Logging

Every access to sensitive data should be logged. Audit logs are what you show regulators.

### BigQuery audit logs

BigQuery writes all data access to Cloud Audit Logs automatically. Query them in Log Explorer or export to BigQuery for analysis:

```sql
-- Who queried the users table in the last 30 days?
SELECT
  protopayload_auditlog.authenticationInfo.principalEmail AS user,
  protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query AS query_text,
  timestamp
FROM `project.dataset.cloudaudit_googleapis_com_data_access`
WHERE
  protopayload_auditlog.resourceName LIKE '%users%'
  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
ORDER BY timestamp DESC;
```

### Application-level logging for PII access

When PII is accessed by an application (not just a query), log it explicitly:

```python
import logging
from dataclasses import dataclass

audit_logger = logging.getLogger("audit")

def get_user_email(user_id: str, accessed_by: str, reason: str) -> str:
    audit_logger.info({
        "event": "pii_access",
        "field": "email",
        "user_id": user_id,
        "accessed_by": accessed_by,
        "reason": reason,
        "timestamp": datetime.utcnow().isoformat(),
    })
    return db.query("SELECT email FROM users WHERE user_id = %s", user_id)
```

---

## Data Retention and Right to Erasure

### Retention policy table

```yaml
# governance/retention_policy.yaml
datasets:
  - name: raw_events
    retention_days: 365
    action_on_expiry: delete
    regulation: PDPA

  - name: user_profiles
    retention_days: 1825  # 5 years
    action_on_expiry: anonymize  # replace PII with hashed/null values
    regulation: [GDPR, PDPA]

  - name: audit_logs
    retention_days: 2555  # 7 years — legal hold
    action_on_expiry: archive_to_cold_storage
```

### Right-to-erasure workflow

When a user requests deletion (GDPR Article 17 / PDPA):

```python
def process_erasure_request(user_id: str):
    # 1. Find all tables that contain this user
    tables_with_user = catalog.find_tables_containing(user_id)

    # 2. Anonymize or delete in each table
    for table in tables_with_user:
        if table.retention_policy.action == "anonymize":
            db.execute(f"""
                UPDATE {table.name}
                SET email = SHA256(email),
                    full_name = 'REDACTED',
                    phone = NULL
                WHERE user_id = %s
            """, user_id)
        elif table.retention_policy.action == "delete":
            db.execute(f"DELETE FROM {table.name} WHERE user_id = %s", user_id)

    # 3. Log the erasure for proof of compliance
    audit_logger.info({"event": "erasure_completed", "user_id": user_id, "tables": tables_with_user})
```

---

## Data Catalog

A data catalog makes datasets discoverable and trustworthy. At minimum, every mart-level table should have:

```yaml
# dbt schema.yml — catalog metadata
models:
  - name: fct_subscriptions
    description: >
      One row per customer per subscription month. Source of truth for MRR reporting.
      Owned by: Data Engineering. Consumers: Finance, Analytics, ML team.
    meta:
      owner: data-engineering@company.com
      sla: "Available by 06:00 UTC daily"
      sensitivity: internal
      status: production  # or: experimental, deprecated
    columns:
      - name: mrr_usd
        description: Monthly recurring revenue in USD. Null for churned months.
        meta:
          pii_tier: internal
```

Run `dbt docs generate` to push this into dbt's built-in catalog, or integrate with DataHub/Amundsen/Atlan for cross-system discovery.

---

## Governance Checklist

Before a dataset is considered "governed":
- [ ] All PII fields identified and tagged with sensitivity tier and regulation scope
- [ ] Column-level access controls applied to Confidential/Restricted fields
- [ ] Lineage documented from source system to BI layer
- [ ] Audit logging enabled for all PII access
- [ ] Retention policy defined — days to keep, action on expiry (delete vs. anonymize)
- [ ] Right-to-erasure procedure tested end-to-end
- [ ] Role-based access defined at the group level, not individual level
- [ ] Data catalog entry created for all mart/production tables (owner, SLA, sensitivity)
- [ ] Governance policy reviewed with legal/compliance team
