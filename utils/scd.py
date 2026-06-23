"""
Slowly Changing Dimension (SCD) merge utilities.

SCD1 — overwrite in place. Use for "current state" facts/KPIs where you
       don't need history (e.g. a grid's current rate).
SCD2 — expire the old row, insert a new one. Use when you need to answer
       "what did this look like as of date X" (audit/history requirements).

Both assume a Delta Lake target table (spark.sql MERGE INTO).
"""

from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable


def apply_scd1(
    spark: SparkSession,
    target: str,
    source: DataFrame,
    merge_keys: List[str],
    update_cols: List[str] = None,
) -> None:
    """Merge source into target, overwriting matched rows and inserting new ones.

    update_cols: columns to overwrite on match. If None, overwrites all
    source columns.
    """
    delta_target = DeltaTable.forName(spark, target)

    merge_condition = " AND ".join(
        f"target.{k} = source.{k}" for k in merge_keys
    )

    if update_cols is None:
        update_cols = [c for c in source.columns if c not in merge_keys]

    update_set = {c: f"source.{c}" for c in update_cols}
    insert_set = {c: f"source.{c}" for c in source.columns}

    (
        delta_target.alias("target")
        .merge(source.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_set)
        .execute()
    )


def apply_scd2(
    spark: SparkSession,
    target: str,
    source: DataFrame,
    merge_keys: List[str],
    tracked_cols: List[str],
    effective_col: str = "effective_ts",
    expiry_col: str = "expiry_ts",
    current_flag_col: str = "is_current",
    load_ts=None,
) -> None:
    """Type-2 merge: expire changed rows, insert new versions.

    A row is considered "changed" if any tracked_col differs from the
    current version. Unchanged rows are left untouched (no new version
    created — avoids history bloat from no-op upserts).
    """
    load_ts = load_ts or F.current_timestamp()
    delta_target = DeltaTable.forName(spark, target)
    target_df = delta_target.toDF().filter(F.col(current_flag_col) == True)  # noqa: E712

    merge_condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    change_condition = " OR ".join(
        f"t.{c} <> s.{c} OR (t.{c} IS NULL) <> (s.{c} IS NULL)" for c in tracked_cols
    )

    changed_keys = (
        target_df.alias("t")
        .join(source.alias("s"), on=merge_keys, how="inner")
        .where(change_condition)
        .select(*[f"t.{k}" for k in merge_keys])
        .distinct()
    )

    new_or_changed = source.join(changed_keys, on=merge_keys, how="left_semi").union(
        source.join(target_df.select(*merge_keys), on=merge_keys, how="left_anti")
    )

    if new_or_changed.rdd.isEmpty():
        return

    # Step 1: expire current rows that have a changed/new version incoming
    (
        delta_target.alias("t")
        .merge(
            new_or_changed.alias("s"),
            merge_condition.replace("target.", "t.").replace("source.", "s."),
        )
        .whenMatchedUpdate(
            condition=f"t.{current_flag_col} = true",
            set={
                expiry_col: load_ts,
                current_flag_col: F.lit(False),
            },
        )
        .execute()
    )

    # Step 2: insert new current versions
    insert_df = new_or_changed.withColumn(effective_col, load_ts).withColumn(
        expiry_col, F.lit(None).cast("timestamp")
    ).withColumn(current_flag_col, F.lit(True))

    insert_df.write.format("delta").mode("append").saveAsTable(target)
