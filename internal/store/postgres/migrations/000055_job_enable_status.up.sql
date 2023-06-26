CREATE TYPE VALID_JOB_STATE AS ENUM ('enabled', 'disabled');

ALTER TABLE JOB ADD COLUMN IF NOT EXISTS STATE VALID_JOB_STATE DEFAULT 'enabled', ADD COLUMN IF NOT EXISTS  REMARK TEXT;