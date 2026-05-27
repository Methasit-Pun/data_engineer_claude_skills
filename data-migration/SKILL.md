---
name: data-migration
description: Moving data between systems safely — cutover planning, backfill strategies, dual-write patterns, validation, rollback procedures, and zero-downtime migration techniques. Use this skill whenever the team is migrating from one database or warehouse to another (MySQL → Snowflake, Redshift → BigQuery, on-prem → cloud), replacing a legacy pipeline, doing a major schema change on a live table, or planning a cutover that cannot have downtime. Also trigger when the user asks about dual-write, shadow reads, data validation across systems, incremental vs. full migration, or how to safely retire an old system. If the phrase "migrate", "move data", "cutover", "legacy system", or "replace the old pipeline" appears, this skill should be active.
---

# Data Migration Patterns

## The Core Risk

Migrations fail in two ways: data loss (records that existed in the old system don't appear in the new one) and data corruption (records appear but with wrong values). Both are subtle and can go undetected for weeks if you don't build explicit validation into the migration plan.

The second risk is business disruption — users or downstream systems depend on the old system and can't tolerate a gap in availability.

Treat a migration like a deployment: plan for rollback from day one, validate at every stage, and don't cut over until you've proven the new system matches the old one.

---

## Migration Strategies

### Big bang vs. incremental

| Approach | When to use | Risk |
|---|---|---|
| **Big bang** | Small datasets (<1M rows), can tolerate downtime, low-stakes system | If something is wrong, you find out post-cutover with users affected |
| **Incremental** | Large datasets, live production systems, zero-downtime requirement | More complex, but problems are caught before cutover |
| **Dual-write** | Can't afford any data loss, need rollback capability | Double writes for a period; reconciliation required |

For anything that users or downstream pipelines depend on, default to incremental + dual-write.

---

## Phase 1: Backfill Historical Data

Load historical data into the new system before switching live traffic. This is the bulk of the work — do it offline, under no time pressure.

```python
def backfill_in_chunks(source_conn, target_conn, table: str, date_col: str, 
                        start_date: str, end_date: str, chunk_days: int = 7):
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)

        rows = source_conn.execute(f"""
            SELECT * FROM {table}
            WHERE {date_col} >= '{current.date()}' AND {date_col} < '{chunk_end.date()}'
        """).fetchall()

        if rows:
            # Upsert — safe to rerun if a chunk fails
            target_conn.execute_many(f"""
                INSERT INTO {table} VALUES (...)
                ON CONFLICT (id) DO UPDATE SET ...
            """, rows)

        current = chunk_end
        print(f"Backfilled through {chunk_end.date()} — {len(rows)} rows")
```

Run chunks in order, smallest first. Log each chunk's row count so you can verify totals match. Make chunks idempotent — if one fails and you rerun it, you shouldn't get duplicates.

---

## Phase 2: Dual-Write (Live Traffic)

Once the backfill is complete, write new records to both the old and new system simultaneously. This keeps the new system current while the old system remains authoritative.

```python
class DualWriteRepository:
    def __init__(self, old_repo, new_repo):
        self.old = old_repo
        self.new = new_repo

    def insert(self, record):
        # Old system is primary — it must succeed
        result = self.old.insert(record)

        # New system is secondary — log failures but don't raise
        try:
            self.new.insert(record)
        except Exception as e:
            logger.error(f"Dual-write to new system failed: {e}", extra={"record_id": record.id})
            # Alert but don't fail the request — old system is still authoritative

        return result
```

During dual-write, the old system is still the source of truth. The new system is in "catch-up" mode and may have gaps from failures.

---

## Phase 3: Validation

Before cutting over, prove the new system matches the old one. Run validation continuously during dual-write — gaps caught early are easy to fix.

### Row count validation

```python
def validate_row_counts(old_conn, new_conn, table: str, date_col: str, date: str) -> bool:
    old_count = old_conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE DATE({date_col}) = '{date}'"
    ).scalar()
    new_count = new_conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE DATE({date_col}) = '{date}'"
    ).scalar()

    if old_count != new_count:
        logger.error(f"Row count mismatch on {date}: old={old_count}, new={new_count}")
        return False
    return True
```

### Field-level checksum validation

```sql
-- Run on both systems — hashes must match
SELECT
    DATE(created_at) AS day,
    COUNT(*) AS row_count,
    SUM(amount) AS total_amount,
    MD5(STRING_AGG(CAST(id AS STRING) ORDER BY id)) AS row_hash
FROM orders
WHERE created_at >= '2024-01-01'
GROUP BY 1
ORDER BY 1;
```

Compare output row by row. Any day with a different hash or count has a data discrepancy that must be investigated before cutover.

### Shadow reads

Before switching reads to the new system, route a fraction of read traffic to it and compare results to the old system's response:

```python
def get_user_features(user_id: str) -> dict:
    old_result = old_repo.get_features(user_id)

    # Shadow read — run against new system but don't return its result
    if random.random() < 0.05:  # 5% shadow traffic
        try:
            new_result = new_repo.get_features(user_id)
            if old_result != new_result:
                logger.warning("Shadow read mismatch", extra={
                    "user_id": user_id,
                    "old": old_result,
                    "new": new_result
                })
        except Exception as e:
            logger.error(f"Shadow read error: {e}")

    return old_result  # old system still authoritative
```

Increase shadow traffic percentage gradually. When mismatch rate reaches zero, you're ready to cut over.

---

## Phase 4: Cutover

### Zero-downtime cutover sequence

1. **Stop writes to old system** — put the application in read-only mode or drain the write queue
2. **Let dual-write drain** — wait for any in-flight writes to complete
3. **Run final validation** — row counts and checksums must match
4. **Switch read traffic** — update the connection string / feature flag to point at new system
5. **Switch write traffic** — new system becomes primary
6. **Monitor for 30 minutes** — watch error rates, query latency, and downstream pipeline health
7. **Remove old system writes** — stop dual-writing to old system

```python
# Feature flag pattern for cutover
def get_connection():
    if feature_flags.is_enabled("use_new_warehouse"):
        return new_conn
    return old_conn
```

Feature flags let you cut over and roll back instantly without a code deploy.

### Cutover checklist

- [ ] Final validation run passed (row counts + checksums)
- [ ] Shadow read mismatch rate is zero for at least 24 hours
- [ ] Rollback procedure tested and ready (verified it works, not just documented)
- [ ] On-call engineer available during cutover window
- [ ] Downstream teams notified of cutover time
- [ ] Monitoring dashboards open for old system AND new system during transition
- [ ] Feature flag or config change ready to flip

---

## Phase 5: Rollback Plan

Define the rollback before you start — not when something breaks.

```
Rollback trigger conditions:
- Error rate > 1% on new system for > 5 minutes
- Query latency p99 > 2x baseline on new system
- Any data validation failure post-cutover

Rollback steps:
1. Flip feature flag back to old system (< 1 minute)
2. Verify reads are back on old system (check logs)
3. Investigate new system failures
4. File incident report with timeline

Old system will remain operational for 30 days post-cutover before decommissioning.
```

---

## Decommissioning the Old System

Don't decommission the old system immediately after cutover. Keep it running in read-only mode for at least 2–4 weeks:

- Provides a rollback option if a subtle data issue surfaces post-cutover
- Allows comparison queries if anyone questions the new system's numbers
- Gives downstream teams time to update any hardcoded references

Before decommissioning:
- [ ] All downstream pipelines confirmed pointing at new system
- [ ] BI tool connections updated
- [ ] No queries hitting old system for at least 7 days (check access logs)
- [ ] Archive a snapshot of old system data to cold storage
- [ ] Old system access revoked from service accounts

---

## Common Migration Pitfalls

| Pitfall | Fix |
|---|---|
| Skipping validation until cutover | Run validation continuously during dual-write phase |
| Backfill without idempotency | Make each chunk a delete-then-insert on the date partition |
| Cutting over on a Friday | Always cut over Monday–Wednesday with full team available |
| No rollback plan | Define rollback triggers and test the procedure before cutover day |
| Decommissioning old system same day | Keep old system alive for 30 days post-cutover |
| Forgetting downstream consumers | Audit all pipelines, BI tools, and applications that read from old system |
