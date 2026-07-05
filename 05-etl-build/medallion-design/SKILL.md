---
name: medallion-design
description: Design a medallion (bronze/silver/gold) ETL architecture interactively, objective-first. List the available data, confirm the objective, then design GOLD first to match the objective and get the user to review it before moving down to silver, then bronze (top-down default) — or bronze-up if the user asks. Asks the user to confirm at each layer boundary rather than designing all three in one shot. Wraps the reusable utils/ library (bronze.py, silver.py, scd.py, watermark.py, metadata.py, quality.py). Use this skill whenever the user wants a bronze/silver/gold or medallion/lakehouse layout, is building a layered Delta/warehouse pipeline, or wants an objective-driven top-down layer design. For extraction/idempotency mechanics reference pipeline-design; this owns the layered design conversation.
origin: grouped
---

# Medallion Design (Interactive, Objective-First)

Medallion = **bronze** (raw, as-ingested) → **silver** (cleaned, conformed) → **gold** (business-ready, objective-shaped). This skill designs it as a **guided conversation**, one layer at a time, so the gold layer actually matches what the user needs — not a generic three-tier that misses the goal.

## Core principle: design gold first (top-down default)

Most medallion designs fail because they build bronze→silver→gold and *discover* at the end that gold doesn't answer the question. Invert it. **Start from the objective, define gold, then derive what silver and bronze must contain to feed it.**

Bottom-up (bronze→gold) is available too — use it when the user explicitly asks, or when the sources are fixed/unknown-purpose and you're exploring what gold *could* be.

## The interactive protocol — ask at every boundary

Do **not** design all three layers in one response. Walk the user through it:

1. **Inventory + objective.** List every dataset available (pull from [[data-sourcing]] / [[data-profiling]] if present). Confirm the objective in one sentence: *"Gold exists to answer ___."*
2. **Design GOLD.** Propose the gold tables — grain, columns, metrics, dimensions, SCD needs — shaped directly to the objective. **Stop and ask the user to review before continuing.**
3. **On approval, design SILVER.** Derive the cleaned/conformed tables that gold requires: standardized names, cast types, dedup keys, validated rows. **Stop and ask the user to review.**
4. **On approval, design BRONZE.** Derive the raw landing tables that silver requires: which raw sources, ingestion metadata, quarantine for malformed rows. **Stop and confirm.**
5. **Trace end-to-end.** Show the full bronze→silver→gold lineage in one diagram and confirm every gold field traces back to a real bronze source.

If the user requests bottom-up, run 2–4 in reverse (bronze → silver → gold) and confirm at each boundary the same way.

## What each layer owns

| Layer | Owns | Typical operations | utils module |
|---|---|---|---|
| **Bronze** | Exact raw copy, never mutated | ingest, quarantine malformed, stamp ingestion metadata, exact-dedup | `utils/bronze.py` |
| **Silver** | Clean & conform | standardize names, cast types, trim, dedup to key, fill nulls, quarantine invalid | `utils/silver.py` |
| **Gold** | Business-ready, objective-shaped | incremental watermark, SCD1/SCD2, metadata stamping, marts | `utils/watermark.py`, `utils/scd.py`, `utils/metadata.py` |

## Wrap the utils library — don't re-implement plumbing

The building blocks already exist in `utils/`. Compose them rather than hand-writing merges and watermarks. Typical flow (see `utils/example_pipeline.py` and `utils/README.md`):

```python
from utils.bronze import read_raw, quarantine_malformed, stamp_ingestion_metadata
from utils.silver import standardize_column_names, cast_columns, dedup_to_key, quarantine_invalid_rows
from utils.watermark import get_last_watermark, update_watermark
from utils.metadata import stamp_metadata, new_run_id
from utils.scd import apply_scd1, apply_scd2

# BRONZE
raw = read_raw(spark, path)
clean_raw, bad = quarantine_malformed(raw, required_cols)
bronze = stamp_ingestion_metadata(clean_raw, source_system="salesforce")

# SILVER
s = standardize_column_names(bronze)
s = cast_columns(s, {"signup_date": "date", "customer_id": "int"})
silver = dedup_to_key(s, keys=["customer_id"], order_by="load_ts")

# GOLD (incremental + SCD2 history)
wm = get_last_watermark("dim_customer")
delta = silver.filter(silver.load_ts > wm)
delta = stamp_metadata(delta, run_id=new_run_id(), pipeline_name="dim_customer")
apply_scd2(target="gold.dim_customer", source=delta, key="customer_id")
update_watermark("dim_customer", new_high=delta.agg(...))  # only AFTER write commits
```

> `scd.py` assumes Delta Lake. Always call `update_watermark` *after* the write commits, so a failed run safely reprocesses the window (see `utils/README.md` notes).

## Design checklist

- [ ] Objective stated in one sentence before any table is drawn
- [ ] Gold designed and **user-reviewed** first (or bronze-first if user chose bottom-up)
- [ ] Silver designed only after gold approved; bronze after silver approved
- [ ] Every gold field traces to a bronze source (end-to-end lineage shown)
- [ ] SCD1 vs SCD2 decided per gold dimension (history needed or not)
- [ ] Idempotency + watermark placement correct (mechanics: [[data-pipelines]])
- [ ] Quality gates chosen per layer (validation: [[data-reliability]])
- [ ] utils modules reused instead of re-implemented

## Hand-off

Mechanics of extraction/scheduling → [[data-pipelines]]; the dimensional model itself → [[data-modeling]]; layer-by-layer validation → [[data-reliability]]. Lifecycle overview: [[data-lifecycle]].
