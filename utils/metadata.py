"""
Metadata/lineage stamping utilities — every row written to gold should be
traceable back to the run that produced it.
"""

import uuid
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def stamp_metadata(
    df: DataFrame,
    pipeline_name: str,
    batch_id: str = None,
    source_system: str = None,
) -> DataFrame:
    """Add standard lineage columns to a dataframe before writing to gold.

    Columns added:
      load_ts        — UTC timestamp this batch was processed
      run_id         — unique id for this execution (generated if not passed via batch_id)
      pipeline_name  — name of the pipeline that produced the row
      source_system  — origin system, if known
    """
    batch_id = batch_id or str(uuid.uuid4())

    return (
        df.withColumn("load_ts", F.current_timestamp())
        .withColumn("run_id", F.lit(batch_id))
        .withColumn("pipeline_name", F.lit(pipeline_name))
        .withColumn("source_system", F.lit(source_system))
    )


def new_run_id() -> str:
    """Generate a fresh run_id at the start of a pipeline, so the same id
    can be threaded through stamp_metadata(), logging, and watermark updates.
    """
    return str(uuid.uuid4())
