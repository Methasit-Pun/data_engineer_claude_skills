"""
Bronze-layer utilities — land raw data with minimal transformation, but
enough structure to know what came in, when, and whether any of it was
malformed.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_raw(
    spark: SparkSession,
    path: str,
    fmt: str = "json",
    schema=None,
    options: dict = None,
) -> DataFrame:
    """Read raw source files with PERMISSIVE mode so malformed records land
    in a _corrupt_record column instead of failing the job. Use
    quarantine_malformed() afterwards to split good rows from bad ones.
    """
    options = options or {}
    reader = spark.read.format(fmt).option("mode", "PERMISSIVE")
    if fmt in ("json", "csv") and "columnNameOfCorruptRecord" not in options:
        reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")
    for k, v in options.items():
        reader = reader.option(k, v)
    if schema is not None:
        reader = reader.schema(schema)
    return reader.load(path)


def quarantine_malformed(df: DataFrame, corrupt_col: str = "_corrupt_record"):
    """Split a raw dataframe into (clean_df, quarantined_df) based on the
    corrupt-record marker column left by read_raw(). Returns clean rows
    with the marker column dropped, and the quarantined rows as-is for
    inspection/replay.
    """
    if corrupt_col not in df.columns:
        return df, df.sparkSession.createDataFrame([], df.schema)

    quarantined = df.filter(F.col(corrupt_col).isNotNull())
    clean = df.filter(F.col(corrupt_col).isNull()).drop(corrupt_col)
    return clean, quarantined


def stamp_ingestion_metadata(
    df: DataFrame,
    source_path: str,
    file_name_col: bool = True,
) -> DataFrame:
    """Add raw-ingestion lineage: ingest_ts, source_path, and (optionally)
    the originating file name per row via input_file_name(). Do this BEFORE
    any cleaning so you always know exactly where a raw row came from.
    """
    out = df.withColumn("ingest_ts", F.current_timestamp()).withColumn(
        "source_path", F.lit(source_path)
    )
    if file_name_col:
        out = out.withColumn("source_file", F.input_file_name())
    return out


def dedupe_exact_raw(df: DataFrame) -> DataFrame:
    """Drop fully-duplicate raw rows. Common in landing zones when the
    same file gets re-dropped or a producer retries a send. This is a
    blunt exact-match dedup, not a business-key dedup — that belongs in
    silver.py (dedup_to_key) since it requires knowing which version to keep.
    """
    return df.dropDuplicates()


def row_count_by_file(df: DataFrame, file_name_col: str = "source_file") -> DataFrame:
    """Row count per source file — quick sanity check that every expected
    file actually landed rows (catches silently-empty or truncated drops).
    """
    return df.groupBy(file_name_col).count().orderBy(file_name_col)
