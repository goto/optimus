CREATE TABLE IF NOT EXISTS sync_status (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    project_name        VARCHAR(100) NOT NULL REFERENCES project   (name),
    entity_type         VARCHAR(250),
    identifier          VARCHAR(250),
    last_update_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE (project_name, identifier)
);

CREATE INDEX IF NOT EXISTS sync_status_id_idx ON sync_status USING btree (id);
CREATE INDEX IF NOT EXISTS sync_status_project_name_entity_type_identifier_idx ON sync_status USING btree (project_name, entity_type, identifier);