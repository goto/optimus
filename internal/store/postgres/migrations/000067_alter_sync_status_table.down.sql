ALTER TABLE sync_status
DROP COLUMN IF EXISTS last_sync_attempt,
DROP COLUMN IF EXISTS remarks,
ALTER COLUMN last_update_time SET NOT NULL;