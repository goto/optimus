CREATE TABLE IF NOT EXISTS airflow_sync_state (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    project_name   VARCHAR(100) NOT NULL,
    start_time     TIMESTAMPTZ NOT NULL,
    end_time       TIMESTAMPTZ NOT NULL,

    status         VARCHAR(30) NOT NULL,
    attempt_count  INT NOT NULL DEFAULT 0,
    last_error     TEXT,

    worker_id      UUID,
    locked_until   TIMESTAMPTZ,

    -- highest Airflow eventLogs `event_log_id` applied in this window, used to
    -- de-duplicate the small overlap between adjacent windows
    max_processed_log_id BIGINT,
    events_matched        INT,
    runs_reconciled       INT,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT airflow_sync_state_project_window_unique UNIQUE (project_name, start_time, end_time)
);

CREATE INDEX IF NOT EXISTS airflow_sync_state_project_end_time_idx
    ON airflow_sync_state (project_name, end_time DESC);
