"""
Quick data-quality profiling utilities — the checks you run first when
looking at a table you don't trust yet.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def profile_nulls(df: DataFrame) -> DataFrame:
    """Null count and null % per column, sorted worst-first."""
    total = df.count()
    if total == 0:
        return df.sparkSession.createDataFrame([], "column STRING, null_count LONG, null_pct DOUBLE")

    agg_exprs = [F.sum(F.col(c).isNull().cast("long")).alias(c) for c in df.columns]
    counts_row = df.agg(*agg_exprs).collect()[0]

    rows = [(c, counts_row[c], round(counts_row[c] / total * 100, 2)) for c in df.columns]
    return df.sparkSession.createDataFrame(
        rows, "column STRING, null_count LONG, null_pct DOUBLE"
    ).orderBy(F.col("null_pct").desc())


def compare_schemas(df1: DataFrame, df2: DataFrame) -> dict:
    """Diff two dataframe schemas. Returns columns only in df1, only in
    df2, and columns present in both but with a different type.
    """
    s1 = {f.name: f.dataType.simpleString() for f in df1.schema.fields}
    s2 = {f.name: f.dataType.simpleString() for f in df2.schema.fields}

    only_in_1 = sorted(set(s1) - set(s2))
    only_in_2 = sorted(set(s2) - set(s1))
    type_mismatches = {
        c: (s1[c], s2[c]) for c in set(s1) & set(s2) if s1[c] != s2[c]
    }

    return {
        "only_in_first": only_in_1,
        "only_in_second": only_in_2,
        "type_mismatches": type_mismatches,
    }


def row_count_diff(df1: DataFrame, df2: DataFrame) -> dict:
    """Row count comparison — quick smoke test after a migration or
    rewrite to confirm nothing was silently dropped/duplicated.
    """
    c1, c2 = df1.count(), df2.count()
    return {"first": c1, "second": c2, "diff": c1 - c2}


def get_distinct_counts(df: DataFrame, columns=None) -> DataFrame:
    """Distinct value count per column — fast way to spot near-constant
    columns or unexpectedly high-cardinality ones.
    """
    columns = columns or df.columns
    agg_exprs = [F.countDistinct(F.col(c)).alias(c) for c in columns]
    result = df.agg(*agg_exprs).collect()[0]
    rows = [(c, result[c]) for c in columns]
    return df.sparkSession.createDataFrame(rows, "column STRING, distinct_count LONG")


def find_outdated_rows(df: DataFrame, key_cols, order_col: str) -> DataFrame:
    """Given a table with multiple versions per key (e.g. before applying
    SCD logic), return all rows EXCEPT the latest version per key. Useful
    for spotting stale rows that should have been superseded.
    """
    from pyspark.sql import Window

    w = Window.partitionBy(key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") > 1)
        .drop("_rn")
    )
