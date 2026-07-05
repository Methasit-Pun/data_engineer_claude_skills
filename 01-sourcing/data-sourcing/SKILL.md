---
name: data-sourcing
description: Data collection and readiness — the step BEFORE a pipeline exists. Catalog candidate data sources for an objective, rank each by importance/impact, and record where to get it (internal system vs. open/public dataset), how to access it, its refresh cadence, licensing, and readiness blockers. Use this skill whenever a project is starting and the sources aren't decided yet, when the user asks "what data do I need for X and where do I get it", when weighing internal vs. open-source data, or when assessing whether a source is accessible/usable before committing to a build. This sits upstream of pipeline-design — it decides WHAT to ingest before deciding HOW.
origin: grouped
---

# Data Sourcing & Readiness

The step before you build anything. You cannot design a pipeline for data you haven't located, priced, or confirmed you're allowed to use. This skill produces a **source inventory** the rest of the lifecycle consumes.

## Start from the objective, not the data

Ask first: *what question or product does this data serve?* Every source is judged by how much it moves that objective — not by how easy it is to grab.

1. **Objective** — BI dashboard? ML model? Regulatory report? One-off analysis?
2. **Decision the data drives** — what changes based on this data?
3. **Minimum viable sources** — the smallest set that answers the objective. Resist collecting "nice to have" data no one will use.

## Build the source inventory

For every candidate source, fill one row. This table is the deliverable.

| Column | What to capture |
|---|---|
| Source name | Human name (e.g. "Salesforce Opportunities", "OpenStreetMap") |
| Type | Internal system / SaaS API / public dataset / file drop / scrape |
| Importance | Critical / important / enrichment — impact on the objective |
| Where to get it | Exact system, endpoint, URL, or dataset repository |
| Access method | DB credential, API key, OAuth, S3 bucket, download, request-to-owner |
| Owner | Team or person who controls access |
| Refresh cadence | Real-time / daily / monthly / static |
| Volume | Rough row/byte scale — feeds architecture sizing |
| Licensing | Internal-only / open license (name it) / restricted / PII-bearing |
| Readiness | Ready / needs-access / needs-approval / blocked |

## Internal vs. open-source sourcing

**Internal first for anything proprietary or objective-critical** — it's authoritative and you control it. Common internal sources: transactional DBs (Postgres/MySQL), SaaS tools (Salesforce, HubSpot, Stripe), event streams, data warehouse tables, internal APIs, file exports.

**Open/public data for enrichment, benchmarks, or when you lack the internal equivalent.** Name the license explicitly — it decides whether you can use it commercially.

Common open sources by need:
- **General/tabular:** Kaggle Datasets, Hugging Face Datasets, data.gov, Google Dataset Search, Awesome Public Datasets.
- **Geospatial:** OpenStreetMap, Natural Earth, government GIS portals.
- **Economic/financial:** World Bank, FRED, IMF, SEC EDGAR.
- **Text/ML corpora:** Hugging Face Hub, Common Crawl, Wikipedia dumps.
- **APIs:** check the provider's public API before scraping — scraping is a licensing and reliability risk of last resort.

## Readiness gate — pass before designing

A source is **not ready** to build on until:

- [ ] Access is confirmed *obtainable* (you have or can get the credential/permission)
- [ ] Licensing permits your use (commercial? redistribution? PII?)
- [ ] Refresh cadence meets the objective's freshness need
- [ ] An owner is identified for when it breaks
- [ ] Rough volume is known (feeds [[data-architecture]] sizing)
- [ ] A PII/sensitivity flag is set (hands off to [[data-reliability]] governance)

Sources stuck at `needs-approval` or `blocked` are risks — surface them now, not mid-build.

## Hand-off

The finished inventory feeds:
- Profiling the actual data → [[data-profiling]]
- Sizing infra and cost → [[data-architecture]]
- Building the ingestion → [[data-pipelines]]
- Governance/PII handling → [[data-reliability]]

See the lifecycle overview in [[data-lifecycle]].
