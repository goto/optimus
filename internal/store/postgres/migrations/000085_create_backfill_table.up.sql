CREATE TABLE IF NOT EXISTS backfill (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    scheduler_run_id VARCHAR,

    project_name    VARCHAR NOT NULL,
    namespace_name  VARCHAR NOT NULL,
    job_name        VARCHAR NOT NULL,

    description     VARCHAR,
    category        VARCHAR(100) NOT NULL,
    approval_id     VARCHAR NOT NULL,
    user_id         VARCHAR NOT NULL,
    
    start_time  TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time    TIMESTAMP WITH TIME ZONE,
    status VARCHAR(30) NOT NULL,
    message TEXT,

    dstart      TIMESTAMP WITH TIME ZONE NOT NULL,
    dend        TIMESTAMP WITH TIME ZONE NOT NULL,
    custom_assets JSONB,
    job_config JSONB NOT NULL,

    canceled_by VARCHAR,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

    CONSTRAINT backfill_approval_id_unique UNIQUE (approval_id)
);

CREATE INDEX IF NOT EXISTS backfill_project_name_idx ON backfill(project_name);
CREATE INDEX IF NOT EXISTS backfill_job_name_idx ON backfill(job_name);
CREATE INDEX IF NOT EXISTS backfill_scheduler_run_id_idx ON backfill(scheduler_run_id);
CREATE INDEX IF NOT EXISTS backfill_status_idx ON backfill(status);
CREATE INDEX IF NOT EXISTS backfill_approval_id_idx ON backfill(approval_id);
CREATE INDEX IF NOT EXISTS backfill_user_id_idx ON backfill(user_id);