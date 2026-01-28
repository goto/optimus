ALTER TABLE job
ADD COLUMN if not EXISTS dex_sensor boolean DEFAULT FALSE;