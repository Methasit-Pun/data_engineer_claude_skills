"""
Key-discovery utilities — figure out what actually uniquely identifies a
row in a table you didn't design (very common when onboarding a new source).
"""

from itertools import combinations
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def find_unique_key(df: DataFrame, columns, max_combination_size: int = 3, sample_limit: int = 5):
    """Search column combinations (smallest first) for one that uniquely
    identifies each row. Stops and returns the first unique combo found.

    Prints progress as it goes (NOT UNIQUE / UNIQUE KEY FOUND) since this
    is normally run interactively while profiling an unfamiliar table.

    Returns the list of columns forming the key, or None if nothing up to
    max_combination_size is unique.
    """
    for r in range(1, max_combination_size + 1):
        for combo in combinations(columns, r):
            combo_cols = list(combo)

            dup_df = df.groupBy(combo_cols).count().filter(col("count") > 1)

            if dup_df.limit(1).count() > 0:
                print(f"NOT UNIQUE: {combo_cols}")
                dup_df.show(sample_limit, truncate=False)
            else:
                print(f"UNIQUE KEY FOUND: {combo_cols}")
                return combo_cols

    print(f"No unique key found within {max_combination_size} columns")
    return None


def find_all_non_unique_combos(df: DataFrame, columns, max_combination_size: int = 3):
    """Like find_unique_key, but doesn't stop early — returns every
    combination (up to max_combination_size) that is NOT unique. Useful
    for understanding the full shape of duplication rather than just
    grabbing the first valid key.
    """
    non_unique = []
    for r in range(1, max_combination_size + 1):
        for combo in combinations(columns, r):
            combo_cols = list(combo)
            dup_df = df.groupBy(combo_cols).count().filter(col("count") > 1)
            if dup_df.limit(1).count() > 0:
                non_unique.append(combo_cols)
    return non_unique


def get_duplicate_rows(df: DataFrame, key_cols, sample_limit: int = None) -> DataFrame:
    """Return the full rows (not just keys) that violate uniqueness on
    key_cols — i.e. every row belonging to a key value that appears more
    than once. Handy for inspecting *why* a candidate key fails.
    """
    dup_keys = (
        df.groupBy(key_cols).count().filter(col("count") > 1).drop("count")
    )
    dup_rows = df.join(dup_keys, on=key_cols, how="inner")
    return dup_rows.limit(sample_limit) if sample_limit else dup_rows


def key_uniqueness_ratio(df: DataFrame, key_cols) -> float:
    """Fraction of rows that are uniquely identified by key_cols.
    1.0 = perfectly unique key. Cheaper sanity check than the full
    find_unique_key search when you already have a candidate.
    """
    total = df.count()
    if total == 0:
        return 1.0
    distinct = df.select(key_cols).distinct().count()
    return distinct / total
