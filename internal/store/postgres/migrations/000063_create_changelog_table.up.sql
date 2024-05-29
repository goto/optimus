CREATE TYPE ENTITY_TYPE AS ENUM ('job', 'resource');
CREATE TYPE CHANGE_TYPE AS ENUM ('create', 'update', 'delete');

CREATE TABLE IF NOT EXISTS changelog (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    entity_type   ENTITY_TYPE NOT NULL,
    name          VARCHAR(100)  NOT NULL,
    project_name  VARCHAR(100)  NOT NULL,
    change_type   CHANGE_TYPE   NOT NULL,
    changes       JSONB         NOT NULL,
    created_at    TIMESTAMP     WITH TIME ZONE NOT NULL,

    UNIQUE (project_name, name)
);

CREATE INDEX IF NOT EXISTS changelog_project_name_name_idx ON preset USING btree (project_name, name);
