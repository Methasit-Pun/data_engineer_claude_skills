"""
Example showing how watermark.py, scd.py, and metadata.py compose into a
gold-layer pipeline. Not meant to be run as-is — adapt table names to your
environment.
"""

from pyspark.sql import SparkSession

from utils.watermark import get_last_watermark, update_watermark, get_max_column_value
from utils.scd import apply_scd1
from utils.metadata import stamp_metadata, new_run_id

CONTROL_TABLE = "control.watermarks"
PIPELINE_NAME = "nqm_grid"
SOURCE_TABLE = "silver.nqm"
TARGET_TABLE = "gold.nqm_grid"


def run(spark: SparkSession) -> None:
    run_id = new_run_id()

    wm = get_last_watermark(spark, CONTROL_TABLE, PIPELINE_NAME)

    source = spark.table(SOURCE_TABLE).where(f"updated_at > '{wm}'")
    if source.rdd.isEmpty():
        return

    new_wm = get_max_column_value(source, "updated_at")
    source = stamp_metadata(source, PIPELINE_NAME, batch_id=run_id, source_system="nqm")

    apply_scd1(spark, target=TARGET_TABLE, source=source, merge_keys=["grid_id"])

    # Only advance the watermark after the merge above has committed.
    update_watermark(spark, CONTROL_TABLE, PIPELINE_NAME, new_wm)


if __name__ == "__main__":
    spark = SparkSession.builder.appName(PIPELINE_NAME).getOrCreate()
    run(spark)
