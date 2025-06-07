ALTER TABLE sync_status DROP COLUMN if exists last_synced_revision;

COMMENT ON COLUMN sync_status.identifier IS NULL;
COMMENT ON COLUMN sync_status.entity_type IS NULL;
COMMENT ON COLUMN sync_status.last_modified IS NULL;
COMMENT ON COLUMN sync_status.last_sync_attempt IS NULL;
COMMENT ON COLUMN sync_status.remarks IS NULL;