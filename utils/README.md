# utils/

A copy-paste-able library of data engineering building blocks: incremental
load tracking, SCD merges, lineage stamping, key discovery, and data
quality profiling. Pipeline code should call these rather than re-implement
plumbing inline.

## Files

| File | Layer | Purpose |
|---|---|---|
| [bronze.py](bronze.py) | Bronze | Raw ingestion: `read_raw`, `quarantine_malformed`, `stamp_ingestion_metadata`, `dedupe_exact_raw`, `row_count_by_file` |
| [silver.py](silver.py) | Silver | Clean/conform: `standardize_column_names`, `cast_columns`, `trim_strings`, `dedup_to_key`, `fill_nulls`, `quarantine_invalid_rows` |
| [watermark.py](watermark.py) | Gold | Track incremental load progress per pipeline via a control table |
| [scd.py](scd.py) | Gold | SCD1 (overwrite) and SCD2 (expire + insert history) Delta merges |
| [metadata.py](metadata.py) | Gold | Stamp rows with `load_ts`, `run_id`, `pipeline_name`, `source_system` |
| [keys.py](keys.py) | Any | Discover composite/unique keys on an unfamiliar table; find duplicate rows |
| [quality.py](quality.py) | Any | Null profiling, schema diff, row count diff, distinct counts, stale-row detection |
| [control_table_ddl.sql](control_table_ddl.sql) | Gold | DDL for the watermark control table |
| [example_pipeline.py](example_pipeline.py) | — | Shows the gold-layer set (watermark/scd/metadata) composed into one pipeline |

Each file's functions are independent and individually importable — call
only what you need, no need to pull in a whole layer's module to use one
function.

## How to import

### Case A — `utils/` lives inside your project (sibling to your pipeline code)

```
my_project/
├── utils/
│   ├── __init__.py
│   ├── watermark.py
│   ├── scd.py
│   ├── metadata.py
│   ├── keys.py
│   └── quality.py
└── pipelines/
    └── gold_nqm.py
```

As long as you run things from `my_project/` (so it's on `sys.path`), plain
package imports work:

```python
from utils.bronze import read_raw, quarantine_malformed, stamp_ingestion_metadata
from utils.silver import standardize_column_names, cast_columns, dedup_to_key
from utils.watermark import get_last_watermark, update_watermark
from utils.scd import apply_scd1, apply_scd2
from utils.metadata import stamp_metadata, new_run_id
from utils.keys import find_unique_key, get_duplicate_rows
from utils.quality import profile_nulls, compare_schemas
```

### Case B — `utils/` is copied into a *different* repo, not at the root

If you drop the whole `utils/` folder somewhere other than your project
root (e.g. `src/utils/` or `lib/utils/`), import using that path:

```python
from src.utils.watermark import get_last_watermark
```

Just make sure every parent directory between your project root and
`utils/` has an `__init__.py` (or is a namespace package), otherwise the
import will fail.

### Case C — `utils/` is external and NOT importable as a package

If you can't put `utils/` under your project root (e.g. notebook
environment, ad-hoc script, or it lives in a shared location), add its
*parent* directory to `sys.path` before importing:

```python
import sys
sys.path.insert(0, "/path/to/folder/containing/utils")

from utils.watermark import get_last_watermark
from utils.scd import apply_scd1
```

### Case D — Databricks notebooks

Upload `utils/` as a Workspace Files folder, then either:

```python
import sys
sys.path.append("/Workspace/Repos/<you>/<repo>")
from utils.watermark import get_last_watermark
```

or, if using Databricks Repos, the repo root is already on `sys.path` —
just import directly as in Case A.

### Case E — install as a proper package (multiple repos reuse it)

If several projects need `utils/`, the durable fix is turning it into an
installable package (add a minimal `pyproject.toml` next to it) and:

```bash
pip install -e /path/to/utils_package
```

Then import normally — `from utils.watermark import get_last_watermark` —
from any environment with that package installed.

## Notes

- `scd.py` assumes Delta Lake (`delta-spark`) is available — `MERGE INTO`
  and `DeltaTable` are Delta-specific, not plain Spark SQL.
- `watermark.py` only advances the watermark on explicit call —
  always place `update_watermark(...)` *after* your write has committed,
  never before, so a failed run safely re-processes the same window on retry.
- `keys.py`'s `find_unique_key` is brute-force (`combinations` over your
  candidate columns) — fine for profiling at the console, but cap
  `max_combination_size` on wide tables or it gets slow fast.
- Typical medallion flow using these modules:
  `bronze.read_raw` → `bronze.quarantine_malformed` → `bronze.stamp_ingestion_metadata`
  → `silver.standardize_column_names` → `silver.cast_columns` → `silver.dedup_to_key`
  → `silver.quarantine_invalid_rows` → (gold) `watermark.get_last_watermark` →
  `metadata.stamp_metadata` → `scd.apply_scd1`/`apply_scd2` → `watermark.update_watermark`.
