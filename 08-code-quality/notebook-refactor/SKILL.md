---
name: notebook-refactor
description: Refactor a Jupyter/.ipynb notebook from top-to-bottom exploratory sprawl into small, named, reviewable functions with clear inputs and outputs. Extract subfunctions, separate config/params from logic, remove hidden cell-order dependencies and global-state leakage, add docstrings and light tests, and make the notebook re-runnable top-to-bottom and diff-friendly. Use this skill whenever the user has a messy notebook they want cleaned up, wants notebook code turned into functions or a module, is preparing a notebook for review/handoff/production, or asks to make an .ipynb readable or testable. This is about code STRUCTURE and reviewability; performance and correctness of data code is python-data-patterns' job.
origin: grouped
---

# Notebook Refactor

Exploratory notebooks grow by appending cells. That's fine for discovery and terrible for review, reuse, and reproduction. This skill restructures a notebook into functions a reviewer can read and a machine can re-run top-to-bottom.

## Where this sits (boundary)

- **vs. [[data-modeling]] (python-data-patterns):** that skill fixes *performance and correctness* (memory, vectorization, Polars/Spark). This skill fixes *structure and reviewability*. Refactor structure here; if a function is also slow or buggy, hand the internals to python-data-patterns.

## The smells to fix

- One giant cell doing five things
- Hidden order dependency — cell 12 only works if cell 7 ran, with no signal
- Globals mutated across cells; variables reused for different meanings
- Magic numbers and paths hard-coded mid-logic
- Copy-pasted blocks that differ by one value
- Output/plots interleaved so logic can't be read straight through
- No way to test anything without running the whole notebook

## Refactor procedure

1. **Make it re-runnable first.** Restart kernel, run all. Fix anything that breaks on a clean top-to-bottom pass. You can't refactor what you can't reproduce.
2. **Separate the four kinds of cell.** Sort content into: **config/params** (paths, dates, constants) → top; **pure logic** (transformations) → functions; **side effects** (I/O, writes); **presentation** (plots, displays) → bottom. 
3. **Extract subfunctions.** Turn each coherent block into a named function with explicit args and a return value — no reliance on globals. One function does one thing; name it after that thing.
4. **Parameterize.** Lift hard-coded values into a config cell (or `papermill`-style parameter cell) so the notebook runs for different dates/inputs without edits.
5. **De-duplicate.** Fold copy-pasted variants into one parameterized function.
6. **Document lightly.** One-line docstring per function: what it takes, what it returns. Keep markdown cells for *why*, not *what*.
7. **Add smoke tests.** For the core functions, a couple of `assert`s on a tiny fixture — enough that a reviewer trusts them without rerunning everything.
8. **Consider extraction to a module.** If functions are reused or production-bound, move them to a `.py` beside the notebook and `import` them — the notebook becomes a thin driver.

## Before / after

```python
# BEFORE — one cell, globals, hard-coded, not reusable
df = pd.read_csv("/data/2024-03-15/orders.csv")
df = df[df.status == "complete"]
df["total"] = df.qty * df.price
df.groupby("customer_id")["total"].sum().to_csv("/out/rev.csv")
```

```python
# AFTER — params + pure functions + thin driver

# --- params cell ---
INPUT_PATH = "/data/2024-03-15/orders.csv"
OUTPUT_PATH = "/out/rev.csv"

# --- functions cell ---
def load_orders(path: str) -> pd.DataFrame:
    """Read raw orders CSV."""
    return pd.read_csv(path)

def revenue_by_customer(orders: pd.DataFrame) -> pd.DataFrame:
    """Sum completed-order revenue per customer."""
    completed = orders[orders.status == "complete"].copy()
    completed["total"] = completed.qty * completed.price
    return completed.groupby("customer_id", as_index=False)["total"].sum()

# --- smoke test ---
_fixture = pd.DataFrame({"status":["complete"],"qty":[2],"price":[3.0],"customer_id":[1]})
assert revenue_by_customer(_fixture)["total"].iloc[0] == 6.0

# --- driver cell ---
orders = load_orders(INPUT_PATH)
revenue_by_customer(orders).to_csv(OUTPUT_PATH, index=False)
```

## Done when

- [ ] Restart-and-run-all passes clean, top to bottom
- [ ] Params/config live in one place, not scattered in logic
- [ ] Each function is named, single-purpose, no global reliance, has a docstring
- [ ] No copy-pasted logic blocks remain
- [ ] Core functions have at least a smoke assert
- [ ] Reusable/production code extracted to a `.py` module (if applicable)
- [ ] Notebook diffs cleanly (cleared bulky outputs before commit)

## Hand-off

Slow or memory-heavy function internals → [[data-modeling]] (python-data-patterns). Lifecycle overview: [[data-lifecycle]].
