CREATE TABLE IF NOT EXISTS job_run_expectation_details (
    project_name TEXT NOT NULL,
    job_name TEXT NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    expected_finish_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_name, job_name, scheduled_at)
);