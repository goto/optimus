CREATE TABLE IF NOT EXISTS sla_predictor (
    id                   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_name             TEXT NOT NULL,
    job_scheduled_at     TIMESTAMP NOT NULL,
    job_cause_name       TEXT NOT NULL,
    cause                TEXT NOT NULL,
    reference_time       TIMESTAMP NOT NULL,
    config               JSONB,
    lineages             JSONB,
    created_at           TIMESTAMP NOT NULL DEFAULT NOW()
);