CREATE TABLE IF NOT EXISTS job_third_party_upstream (
    job_id                           UUID NOT NULL,
    job_name                         VARCHAR(220) NOT NULL,
    project_name                     VARCHAR(100) NOT NULL,
    upstream_third_party_type        TEXT NOT NULL,
    upstream_third_party_identifier  TEXT NOT NULL,
    upstream_third_party_config      JSONB,
    created_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, project_name, upstream_third_party_identifier),
    CONSTRAINT job_third_party_upstream_job_id_fkey
        FOREIGN KEY(job_id)
        REFERENCES job(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS job_third_party_upstream_job_name_idx ON job_third_party_upstream (job_name);
CREATE INDEX IF NOT EXISTS job_third_party_upstream_upstream_third_party_type_idx ON job_third_party_upstream (upstream_third_party_type);
CREATE INDEX IF NOT EXISTS job_third_party_upstream_upstream_third_party_identifier_idx ON job_third_party_upstream (upstream_third_party_identifier);