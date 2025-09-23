DO $$ BEGIN
    CREATE TYPE operator_type AS ENUM ('sensor', 'task', 'hook', 'job');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS sla (
     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
     operator_type operator_type NOT NULL,
     run_id UUID NOT NULL,
     sla_time TIMESTAMPTZ NOT NULL,
     description TEXT,
     worker_signature UUID,
     worker_lock_until TIMESTAMPTZ,
     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);