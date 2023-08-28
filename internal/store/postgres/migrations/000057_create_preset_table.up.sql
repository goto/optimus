CREATE TABLE IF NOT EXISTS preset (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    project_name    VARCHAR(100) NOT NULL REFERENCES project (name),
    name            VARCHAR(100) NOT NULL,
    description     TEXT NOT NULL,

    truncate_to     VARCHAR(10),
    offset          VARCHAR(10),
    size            VARCHAR(10),

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

    UNIQUE (project_name, name)
);

CREATE INDEX IF NOT EXISTS preset_id_idx ON preset USING btree (id);
CREATE INDEX IF NOT EXISTS preset_project_name_name_idx ON preset USING btree (project_name, name);
