---
name: python-data-patterns
description: Pandas, Polars, and PySpark idioms for production data engineering — chunked reads, memory-safe transforms, vectorized operations, type optimization, and performance patterns. Use this skill whenever the user is writing a Python data transformation script and running into memory issues, slow performance, or correctness bugs with large datasets. Also trigger when the user asks how to handle large CSV/Parquet files, process data in batches, use Polars instead of Pandas, optimize a PySpark job, or reduce DataFrame memory usage. If you see someone iterating row-by-row over a DataFrame, this skill should trigger immediately.
---

# Python Data Patterns

## The Root Cause of Most Python Data Performance Problems

Row-by-row iteration (`for index, row in df.iterrows()`) is almost always the culprit. DataFrames are columnar data structures — they're designed for batch column operations, not row-by-row Python loops. A 1M-row DataFrame that takes 10 minutes with `iterrows` typically runs in under a second with a vectorized equivalent.

---

## Pandas

### Vectorized operations — always prefer over loops

```python
# Bad: iterrows is 100-1000x slower
for i, row in df.iterrows():
    df.at[i, "margin"] = row["revenue"] - row["cost"]

# Good: vectorized
df["margin"] = df["revenue"] - df["cost"]

# Good: apply only when vectorized isn't possible
df["label"] = df["score"].apply(lambda x: "high" if x > 0.8 else "low")

# Better: use np.where for simple conditionals
import numpy as np
df["label"] = np.where(df["score"] > 0.8, "high", "low")

# Best for complex conditionals: np.select
conditions = [df["score"] > 0.8, df["score"] > 0.5]
choices    = ["high", "medium"]
df["label"] = np.select(conditions, choices, default="low")
```

### Memory optimization — reduce types early

```python
def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes("object"):
        if df[col].nunique() / len(df) < 0.5:  # low cardinality → category
            df[col] = df[col].astype("category")

    for col in df.select_dtypes("int64"):
        df[col] = pd.to_numeric(df[col], downcast="integer")

    for col in df.select_dtypes("float64"):
        df[col] = pd.to_numeric(df[col], downcast="float")

    return df
```

Reducing dtypes on a typical analytics DataFrame cuts memory 50-70%.

### Chunked reads for large files

```python
CHUNK_SIZE = 100_000

results = []
for chunk in pd.read_csv("large_file.csv", chunksize=CHUNK_SIZE):
    chunk = optimize_dtypes(chunk)
    # process chunk
    aggregated = chunk.groupby("customer_id")["revenue"].sum()
    results.append(aggregated)

final = pd.concat(results).groupby(level=0).sum()
```

**Key insight:** Process each chunk independently and accumulate only the aggregated result, not the raw rows. If you're storing every chunk in `results`, you haven't actually saved memory.

### Efficient merges

```python
# Sort before merge on large DataFrames — enables merge-sort algorithm
left = df1.sort_values("customer_id")
right = df2.sort_values("customer_id")
merged = pd.merge(left, right, on="customer_id", how="left")

# Use category dtype on join keys for faster hashing
df1["customer_id"] = df1["customer_id"].astype("category")
df2["customer_id"] = df2["customer_id"].astype("category")
```

---

## Polars — When to Switch from Pandas

Switch to Polars when:
- Dataset > 1GB and Pandas is hitting memory limits
- Multi-core parallelism would help (Polars uses all cores automatically)
- You need lazy evaluation to optimize query plans before executing

### Core Polars idioms

```python
import polars as pl

# Lazy evaluation — Polars optimizes the entire query plan before running
result = (
    pl.scan_csv("large_file.csv")          # lazy — nothing runs yet
    .filter(pl.col("revenue") > 0)
    .with_columns([
        (pl.col("revenue") - pl.col("cost")).alias("margin"),
        pl.col("created_at").str.to_datetime().alias("created_at"),
    ])
    .group_by("customer_id")
    .agg([
        pl.col("revenue").sum().alias("total_revenue"),
        pl.col("order_id").count().alias("order_count"),
    ])
    .collect()                              # execute here
)
```

### Polars expression patterns

```python
# Conditional column (equivalent to np.where)
df.with_columns(
    pl.when(pl.col("score") > 0.8).then("high")
      .when(pl.col("score") > 0.5).then("medium")
      .otherwise("low")
      .alias("label")
)

# Window functions
df.with_columns(
    pl.col("revenue").sum().over("customer_id").alias("customer_total_revenue")
)

# String operations
df.with_columns(
    pl.col("email").str.to_lowercase(),
    pl.col("name").str.strip_chars(),
)
```

### Reading Parquet efficiently

```python
# Read only needed columns — massive speedup on wide tables
df = pl.read_parquet("data.parquet", columns=["customer_id", "revenue", "order_date"])

# Filter at read time (predicate pushdown)
df = pl.scan_parquet("data.parquet").filter(
    pl.col("order_date") >= pl.lit("2024-01-01").str.to_date()
).collect()
```

---

## PySpark — Patterns for Distributed Transforms

### Avoid UDFs when you can

Python UDFs are slow because Spark must serialize data to Python, process it, and serialize back. Use Spark SQL built-in functions instead.

```python
from pyspark.sql import functions as F

# Bad: Python UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def upper_udf(s):
    return s.upper() if s else None

df = df.withColumn("name_upper", upper_udf("name"))

# Good: built-in function
df = df.withColumn("name_upper", F.upper("name"))
```

### Repartitioning and coalescing

```python
# Repartition before a wide transformation (shuffle) — set to ~ 2x cores
df = df.repartition(200, "customer_id")

# Coalesce before writing small output — reduces file count
result.coalesce(10).write.parquet("output/")
```

### Broadcast joins for small dimension tables

```python
from pyspark.sql.functions import broadcast

# When one table fits in memory (~< 100MB), broadcast it
result = large_df.join(broadcast(small_dim_df), "customer_id", "left")
```

### Caching — only when a DataFrame is used multiple times

```python
# Cache only if this DataFrame is referenced in multiple actions
customer_features = (
    df.groupBy("customer_id")
    .agg(F.sum("revenue").alias("total_revenue"), F.count("*").alias("order_count"))
    .cache()
)

# Use it multiple times
count = customer_features.count()
top_customers = customer_features.filter(F.col("total_revenue") > 10000)

customer_features.unpersist()  # release when done
```

### Write partitioned output

```python
result.write.partitionBy("order_date").mode("overwrite").parquet("s3://bucket/orders/")
```

---

## Common Performance Checklist

- [ ] No `iterrows` — replaced with vectorized operations or `np.where`/`np.select`
- [ ] Large files read in chunks or with lazy evaluation
- [ ] dtypes optimized (category for low-cardinality strings, int32/float32 where sufficient)
- [ ] Parquet preferred over CSV for large files (10x smaller, typed, columnar)
- [ ] Polars used for files > 1GB where Pandas struggles
- [ ] PySpark UDFs replaced with built-in `functions` module
- [ ] Spark DataFrames cached only when reused; unpersisted after use
- [ ] Joins on sorted/partitioned keys where possible
- [ ] Column selection done at read time, not after loading the full file
