CREATE TABLE IF NOT EXISTS alert_logs (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_name   TEXT NOT NULL,
    template_name  TEXT NOT NULL,
    data           JSONB NOT NULL,
    labels         JSONB NOT NULL,
    endpoint       TEXT,
    status         TEXT NOT NULL DEFAULT 'PENDING',
    message        TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);