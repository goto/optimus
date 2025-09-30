DO $$ BEGIN
    CREATE TYPE operator_type AS ENUM ('sensor', 'task', 'hook', 'job');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS operator_sla (
     id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
     project_name   TEXT NOT NULL,
     job_name       TEXT NOT NULL,
     operator_name  TEXT NOT NULL,
     run_id         TEXT NOT NULL,
     alert_tag      TEXT NOT NULL,
     operator_type  operator_type NOT NULL,
     
     sla_time            TIMESTAMPTZ NOT NULL,
     scheduled_at        TIMESTAMPTZ NOT NULL,
     operator_start_time TIMESTAMPTZ NOT NULL,

     worker_signature  UUID,
     worker_lock_until TIMESTAMPTZ,

     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);