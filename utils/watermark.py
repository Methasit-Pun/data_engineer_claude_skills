"""
Watermark utilities — track incremental load progress per pipeline.

Backed by a control table (see control_table_ddl.sql). Each pipeline run
reads its last watermark, pulls only rows newer than that, and on success
advances the watermark. Never advance on failure — that's how you avoid
silently skipping data on the next retry.
"""

from datetime import datetime, timezone
from pyspark.sql import SparkSession


DEFAULT_WATERMARK = "1970-01-01T00:00:00"


def get_last_watermark(
    spark: SparkSession,
    control_table: str,
    pipeline_name: str,
    default: str = DEFAULT_WATERMARK,
) -> str:
    """Return the last successfully processed watermark for a pipeline.

    Returns `default` if the pipeline has never run before.
    """
    rows = (
        spark.table(control_table)
        .filter(f"pipeline_name = '{pipeline_name}'")
        .select("last_watermark")
        .collect()
    )
    if not rows:
        return default
    return rows[0]["last_watermark"]


def update_watermark(
    spark: SparkSession,
    control_table: str,
    pipeline_name: str,
    new_watermark: str,
) -> None:
    """Upsert the new high-water mark after a successful load.

    Call this ONLY after the downstream write (merge/insert) has committed.
    If the job fails after this point but before truly finishing, the next
    run will re-process from new_watermark forward — so place this call as
    close to "end of pipeline" as possible.
    """
    updated_at = datetime.now(timezone.utc).isoformat()

    spark.sql(f"""
        MERGE INTO {control_table} AS target
        USING (
            SELECT
                '{pipeline_name}' AS pipeline_name,
                '{new_watermark}' AS last_watermark,
                '{updated_at}' AS updated_at
        ) AS source
        ON target.pipeline_name = source.pipeline_name
        WHEN MATCHED THEN UPDATE SET
            target.last_watermark = source.last_watermark,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (pipeline_name, last_watermark, updated_at)
            VALUES (source.pipeline_name, source.last_watermark, source.updated_at)
    """)


def get_max_column_value(df, column: str) -> str:
    """Helper: pull the max value of a timestamp/date column from a source df
    to use as the new watermark after a successful load.
    """
    result = df.selectExpr(f"max({column}) as max_val").collect()
    max_val = result[0]["max_val"]
    return str(max_val) if max_val is not None else DEFAULT_WATERMARK
