-- Control table backing watermark.py
-- One row per pipeline, holding the last successfully processed watermark.

CREATE TABLE IF NOT EXISTS control.watermarks (
    pipeline_name   STRING NOT NULL,
    last_watermark  STRING NOT NULL,
    updated_at      STRING NOT NULL
)
USING DELTA;

-- pipeline_name should be unique; enforce via application logic (MERGE) since
-- Delta does not support PK constraints natively.
