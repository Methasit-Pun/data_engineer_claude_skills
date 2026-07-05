---
name: data-reliability
description: Umbrella skill for making data correct, trustworthy, and compliant — validation and quality checks (Great Expectations, dbt tests, anomaly/null/range/referential assertions), producer↔consumer schema contracts (versioning, breaking-change detection), and governance (PII tagging, lineage, access control, retention, audit for PDPA/GDPR/HIPAA). Use this whenever the concern is "is this data right, safe, and allowed" rather than moving or shaping it. This skill ROUTES to the focused sub-skills (data-quality, data-contracts, data-governance) and pulls in more than one when a task spans them. Trigger on: bad data in a pipeline, silent upstream schema changes breaking downstream, PII/compliance/audit, or "how do I make sure my data is correct".
origin: grouped
---

# Data Reliability, Contracts & Governance (Router)

This is a **router skill**. It groups the three skills that make data trustworthy: correctness checks, cross-team schema guarantees, and regulatory/access governance. Diagnose which sub-area(s) the task touches, then **invoke the matching sub-skill(s) with the Skill tool**.

## How to route

| If the task is about… | Invoke sub-skill |
|---|---|
| Validation rules, Great Expectations suites, dbt tests, anomaly detection, null/type/range/referential assertions, monitoring bad data in a pipeline | `data-quality` |
| Formalizing what a dataset promises across team boundaries: field types, nullability, allowed values, versioning, breaking vs. non-breaking changes, schema registries, downstream-impact assessment | `data-contracts` |
| PII tagging/classification, column-level access control, data catalog metadata, lineage, retention, right-to-erasure, audit logging, PDPA/GDPR/HIPAA compliance | `data-governance` |

## Routing rules

- **Quality vs. contracts overlap** (both validate schema). Same-owner pipeline correctness → `data-quality`. Two teams/services sharing a dataset where upstream keeps breaking downstream → `data-contracts`. When both apply (a contract enforced by quality tests), invoke both.
- **Any mention of PII, compliance, audit, or regulation** → `data-governance` immediately, often alongside `data-quality`.
- **"Small" upstream schema change about to ship** → `data-contracts` to assess downstream impact before it lands.
- Invoke via the Skill tool by name, e.g. `Skill(skill="data-quality")`. Combine outputs; don't paraphrase from memory.

## Related groups

- Where these checks run in the pipeline → [[data-pipelines]]
- The models being validated → [[data-modeling]]
