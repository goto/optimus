ALTER TABLE sla_predictor
ALTER COLUMN created_at
TYPE timestamptz
USING created_at AT TIME ZONE 'Asia/Jakarta',

ALTER COLUMN reference_time
TYPE timestamptz
USING reference_time AT TIME ZONE 'UTC';