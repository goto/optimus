-- Add last_synced_revision column to support revision-based entities
ALTER TABLE sync_status
    ADD COLUMN if not exists last_synced_revision integer;

COMMENT ON COLUMN sync_status.last_synced_revision IS 'Source Revision number of the last successful sync for revision-tracked entities';

COMMENT ON COLUMN sync_status.identifier IS 'Unique identifier for the specific entity instance';
COMMENT ON COLUMN sync_status.entity_type IS 'Type/category of the entity being synchronized (e.g., external_table, external_table_lark)';
COMMENT ON COLUMN sync_status.last_modified IS 'Timestamp indicating when the entity was last updated';
COMMENT ON COLUMN sync_status.last_sync_attempt IS 'Timestamp of the last sync attempt for all entity types';
COMMENT ON COLUMN sync_status.remarks IS 'JSON field to store additional sync metadata such as error messages, sync details, or logs';
