---
name: stakeholder-reporting
description: Translate pipeline metrics, SLA breaches, data quality failures, and incidents into clear non-technical summaries for business stakeholders. Use this skill whenever data was late, wrong, or missing and someone needs to communicate what happened to a manager, director, or business team. Also trigger when writing incident reports, SLA breach notifications, data quality summaries, pipeline health updates, or any communication where the audience doesn't know what Airflow or BigQuery is. If the user needs to explain a technical data failure to a non-technical person, this skill should be active.
---

# Stakeholder Reporting for Data Incidents

## The Communication Problem

When data is late or wrong, two conversations need to happen simultaneously: the technical one (what broke and how to fix it) and the business one (what does this mean for decisions being made right now?). Data engineers are trained for the first conversation and often skip the second — which is how a 2-hour pipeline delay turns into a 2-day trust crisis.

The goal of stakeholder communication is not to explain the technology. It's to answer: *What can stakeholders rely on right now? What should they not rely on? When will this be resolved?*

---

## Incident Communication Structure

Every data incident notification should answer these four questions, in this order:

1. **What is affected?** — Which reports, dashboards, or decisions are impacted
2. **What do we know?** — The current state in plain language, no jargon
3. **What are you doing?** — The action being taken (not the technical details)
4. **When will it be resolved?** — A specific time estimate or next update time

### Template: Initial incident notification

```
Subject: [Data Alert] [Dashboard/Report Name] — Data delayed as of [TIME]

Hi [team],

We're experiencing a delay with [specific dashboard/report]. Data that should 
reflect activity through [expected time] is currently showing data through 
[actual time] — approximately [N hours] behind.

Impact: [Sales dashboard / Churn model / Finance report] is showing incomplete 
numbers for today. Any decisions based on today's figures should be treated as 
preliminary.

We're actively investigating and expect to have this resolved by [specific time].
We'll send an update at [time] regardless of status.

— [Name], Data Engineering
```

---

## Translating Technical Failures

### Pipeline failure → business language

| What happened (technical) | What to say to stakeholders |
|---|---|
| Airflow DAG failed at 3am | The overnight data refresh didn't complete |
| API rate limit hit on Salesforce connector | Data from Salesforce is delayed — we're re-syncing now |
| BigQuery out-of-memory error | A large query took longer than expected; we're splitting it into smaller runs |
| dbt model test failed: 2.3% null rate in `revenue` | Revenue figures in the Finance dashboard may be understated by approximately 2% |
| Partition filter missing — query scanned all historical data | A query ran unexpectedly slowly; it's been fixed and will run normally going forward |
| Source schema changed — `subscription_status` column renamed | A change in our upstream system caused our pipeline to stop recognizing subscription data |

The test: read your message aloud. If you'd need to explain what any word means, replace it.

### Data quality failure → business impact framing

When data is wrong (not just late), quantify the impact immediately:

```
Bad: "The dbt test failed with a 3.2% null rate on the mrr_usd column."

Good: "The revenue dashboard is understating MRR by approximately 3.2% — 
      equivalent to ~$42,000 in the current month view. The affected records 
      are customers who upgraded their subscription after the 15th of the month. 
      We're reprocessing now; corrected figures will be available by 2pm."
```

Stakeholders make decisions from numbers. Give them the number that's wrong and the approximate magnitude — don't make them calculate it themselves.

---

## SLA Breach Notification

When a data SLA is missed (data promised by 6am isn't ready until 9am):

```
Subject: [SLA Update] Morning data refresh — 3-hour delay

Hi [team],

The morning data refresh that normally completes by 6:00am UTC ran until 9:15am 
today. All dashboards are now current.

What happened: Our data provider (Stripe) experienced intermittent delays in 
delivering transaction data between 1am and 4am. Our pipeline retried 
automatically and completed successfully once the upstream delay cleared.

Impact: Reports that team members checked before 9:15am this morning showed 
data from yesterday only. All reports now reflect today's activity.

What we're doing to prevent recurrence: We're adding an early-warning alert 
so we can notify affected teams by 6:30am if the refresh is running behind, 
rather than after it completes.

— [Name], Data Engineering
```

Notice: the message explains what happened, what was affected, what's fixed, and what's changing — without mentioning Airflow, DAGs, sensors, or retry strategies.

---

## Incident Report (Post-Mortem)

After a significant incident, write a brief post-mortem within 48 hours. Keep it under one page.

### Post-mortem template

```
# Data Incident Report — [Date]

## Summary
[One sentence: what happened, how long, what was affected]

Example: "The daily churn dashboard showed incorrect data for 6 hours on 
[date], caused by a schema change in our CRM system that our pipeline wasn't 
expecting."

## Timeline
[Times in local timezone, plain language]
- 6:00am — Morning refresh started (normal)
- 6:45am — [Team] flagged that churn numbers looked unusual
- 7:00am — Data engineering began investigation
- 8:30am — Root cause identified (column rename in CRM)
- 9:15am — Pipeline updated and reprocessing started
- 10:45am — All data correct and verified

## Impact
- [Dashboard name]: showed incorrect data for 6 hours
- Estimated magnitude: churn rate appeared 12% higher than actual
- Teams affected: Customer Success, Finance (morning standup)

## Root Cause
A change in [system name] renamed a field our pipeline depended on. 
The pipeline continued running but populated the field with zeros rather 
than failing visibly.

## What We're Doing
1. [Immediate] Added an alert that fires when key revenue/churn metrics 
   change by more than 10% day-over-day (catches anomalies faster)
2. [This week] Added a data contract check between CRM and our pipeline 
   so future schema changes fail loudly instead of silently
3. [Next sprint] Improve the morning SLA alert to surface issues by 
   6:30am rather than waiting for someone to notice

## Questions?
Contact [name] at [email] or in [Slack channel].
```

---

## Regular Pipeline Health Updates

For ongoing stakeholder communication (weekly or monthly), keep it short:

```
## Data Pipeline Health — Week of [Date]

✅ All pipelines ran on schedule this week
✅ No SLA breaches
⚠️  One data quality alert: customer age field showed 0.8% nulls on 
    Wednesday (resolved same day, no dashboard impact)

Upcoming changes:
- [Date]: Migrating churn pipeline to new infrastructure (no expected downtime)
- [Date]: New "days since last login" feature will appear in the Customer 
  dashboard

Questions? Reply to this email or ping us in #data-engineering.
```

Use symbols (✅ ⚠️ ❌) for quick visual scanning. Keep the whole update under 150 words. Stakeholders who want more can ask; don't front-load detail they didn't request.

---

## Communication Checklist

Before sending any stakeholder message about a data issue:
- [ ] Identified specifically which dashboards/reports/decisions are affected
- [ ] Quantified the impact (rows affected, metric magnitude, time window)
- [ ] Removed all technical jargon (Airflow, DAG, dbt, partition, schema)
- [ ] Included a specific resolution time or a "next update by" time
- [ ] Stated clearly what stakeholders should and should not rely on right now
- [ ] For incidents: sent within 30 minutes of discovery, not after it's fixed
- [ ] For post-mortems: completed within 48 hours, reviewed by one peer before sending
