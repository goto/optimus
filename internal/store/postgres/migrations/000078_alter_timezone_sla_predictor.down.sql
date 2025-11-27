ALTER TABLE sla_predictor
ALTER COLUMN created_at
TYPE timestamp
USING created_at AT TIME ZONE 'Asia/Jakarta',

ALTER COLUMN reference_time
TYPE timestamp
USING reference_time AT TIME ZONE 'UTC';