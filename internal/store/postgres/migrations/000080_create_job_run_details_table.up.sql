CREATE TABLE job_run_details (
    project_name TEXT NOT NULL,
    job_name TEXT NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    estimated_finish_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_name, job_name, scheduled_at)
);