-- Drop the indexes created in the up migration
DROP INDEX IF EXISTS sync_status_id_idx;
DROP INDEX IF EXISTS sync_status_project_name_entity_type_identifier_idx;

-- Drop the table sync_status
DROP TABLE IF EXISTS sync_status;