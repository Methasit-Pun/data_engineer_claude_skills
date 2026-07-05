---
name: cloud-data-infra
description: Umbrella skill for where data pipelines run and what they cost — AWS/GCP/Azure infrastructure (S3/GCS/ADLS layout, BigQuery/Redshift/Snowflake selection, IAM, managed-service choice, performance tuning) and cost control (bytes scanned, partition pruning, slot/credit management, storage tiering, budgets and alerts). Use this whenever the topic is cloud data infrastructure or the cloud data bill. This skill ROUTES to the focused sub-skills (cloud-infra-data, cost-optimization-data) and pulls in both when a task spans architecture and cost. Trigger on: deploying a pipeline to cloud, choosing between managed warehouses, bucket/partition layout, IAM for data access, "our BigQuery bill jumped", expensive queries, or slot/credit utilization.
origin: grouped
---

# Cloud Data Infrastructure & Cost (Router)

This is a **router skill**. It groups the two skills covering cloud data infrastructure and its cost. Diagnose which sub-area(s) the task touches, then **invoke the matching sub-skill(s) with the Skill tool**.

## How to route

| If the task is about… | Invoke sub-skill |
|---|---|
| Provisioning and architecture: S3/GCS/ADLS partitioning, BigQuery slots, Redshift Spectrum, Snowflake warehouses, IAM/cross-account access, managed-service selection, cloud performance tuning | `cloud-infra-data` |
| The bill: query cost analysis, bytes scanned, partition pruning, slot reservation vs. on-demand, storage tiering, Snowflake credits, cost alerts and budgets | `cost-optimization-data` |

## Routing rules

- **These two almost always travel together.** A "slow BigQuery query" is both a performance (`cloud-infra-data`) and a cost (`cost-optimization-data`) problem — invoke both.
- **"Our bill jumped" or a named expensive query** → lead with `cost-optimization-data` (it starts by finding where the money goes), then `cloud-infra-data` for the structural fix.
- **Choosing/standing up a warehouse or storage layout** → lead with `cloud-infra-data`, then `cost-optimization-data` to price the choice.
- Invoke via the Skill tool by name, e.g. `Skill(skill="cost-optimization-data")`. Combine outputs; don't paraphrase from memory.

## Related groups

- Pipelines deployed onto this infra → [[data-pipelines]]
- Query patterns that drive cost → [[data-modeling]]
