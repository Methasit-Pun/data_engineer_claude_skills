"""
Silver-layer utilities — clean and conform bronze data into something
trustworthy enough to build gold tables on: consistent names/types,
one row per business key, nulls handled deliberately, bad rows quarantined
rather than silently dropped.
"""

import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


def standardize_column_names(df: DataFrame) -> DataFrame:
    """Lowercase + snake_case every column name. Run this first so every
    downstream function can rely on a consistent naming convention
    regardless of how the source system styled its columns.
    """
    def to_snake(name: str) -> str:
        name = re.sub(r"[\s\-]+", "_", name.strip())
        name = re.sub(r"(?<!^)(?=[A-Z])", "_", name)
        return name.lower()

    for c in df.columns:
        df = df.withColumnRenamed(c, to_snake(c))
    return df


def cast_columns(df: DataFrame, type_map: dict) -> DataFrame:
    """Cast columns to expected types. type_map = {"col": "int", ...}.
    Casts are non-throwing in Spark (bad values become null) — pair this
    with quarantine_invalid_rows if you need to catch what failed to cast.
    """
    for col_name, dtype in type_map.items():
        df = df.withColumn(col_name, F.col(col_name).cast(dtype))
    return df


def trim_strings(df: DataFrame, columns: list = None) -> DataFrame:
    """Trim leading/trailing whitespace on string columns. Defaults to
    every string-typed column if none specified.
    """
    columns = columns or [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
    for c in columns:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df


def dedup_to_key(df: DataFrame, key_cols: list, order_col: str, keep: str = "latest") -> DataFrame:
    """Collapse multiple raw versions of the same business key down to
    one row — the one and only place "which version wins" gets decided
    before gold-layer SCD logic ever runs.

    keep: "latest" keeps the max order_col per key, "earliest" keeps the min.
    """
    desc = keep == "latest"
    w = Window.partitionBy(key_cols).orderBy(F.col(order_col).desc() if desc else F.col(order_col).asc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def fill_nulls(df: DataFrame, fill_map: dict) -> DataFrame:
    """Deliberate null handling: fill_map = {"col": default_value}.
    Forces an explicit decision per column instead of leaving nulls to
    propagate silently into gold aggregations.
    """
    return df.fillna(fill_map)


def quarantine_invalid_rows(df: DataFrame, validation_exprs: dict):
    """Split rows into (valid_df, invalid_df) based on named boolean
    conditions, e.g. {"positive_amount": "amount > 0", "has_id": "id IS NOT NULL"}.

    invalid_df gets an extra `failed_checks` column listing which named
    checks failed, so quarantined data is debuggable rather than just discarded.
    """
    check_cols = []
    for name, expr in validation_exprs.items():
        df = df.withColumn(f"_check_{name}", F.expr(expr))
        check_cols.append(f"_check_{name}")

    all_pass = " AND ".join(check_cols)
    failed_list = F.array(
        *[F.when(~F.col(c), F.lit(c.replace("_check_", ""))) for c in check_cols]
    )

    valid = df.filter(F.expr(all_pass)).drop(*check_cols)
    invalid = (
        df.filter(~F.expr(all_pass))
        .withColumn("failed_checks", F.array_except(failed_list, F.array(F.lit(None).cast("string"))))
        .drop(*check_cols)
    )
    return valid, invalid
