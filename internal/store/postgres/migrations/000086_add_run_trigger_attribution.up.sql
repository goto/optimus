-- Capture the Airflow dag run id on job_run so a run row can be joined back to a specific
-- Airflow dag run. Value comes from the existing event payload field
-- event_context.dag_run.run_id, which is already parsed but was never persisted.
ALTER TABLE job_run ADD COLUMN IF NOT EXISTS scheduler_run_id VARCHAR(255);
CREATE INDEX IF NOT EXISTS job_run_scheduler_run_id_idx ON job_run (scheduler_run_id);

-- run_type      : scheduled | replay | backfill | manual
-- triggered_by  : 'scheduler' | '<username>' | 'unidentified_user'
-- attempt       : Airflow task instance try_number, from event_context.task_instance.attempt
--
-- Defaults keep every pre-existing and newly ingested row reading as a plain scheduled
-- run, so behaviour is unchanged until attribution is enabled.
ALTER TABLE task_run ADD COLUMN IF NOT EXISTS run_type VARCHAR(20) NOT NULL DEFAULT 'scheduled';
ALTER TABLE task_run ADD COLUMN IF NOT EXISTS triggered_by VARCHAR(255) NOT NULL DEFAULT 'scheduler';
ALTER TABLE task_run ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS task_run_job_run_id_name_attempt_idx ON task_run (job_run_id, name, attempt);

ALTER TABLE hook_run ADD COLUMN IF NOT EXISTS run_type VARCHAR(20) NOT NULL DEFAULT 'scheduled';
ALTER TABLE hook_run ADD COLUMN IF NOT EXISTS triggered_by VARCHAR(255) NOT NULL DEFAULT 'scheduler';
ALTER TABLE hook_run ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS hook_run_job_run_id_name_attempt_idx ON hook_run (job_run_id, name, attempt);

-- sensor_run is included even though the requirement names only task/hook: all three tables
-- share a single Go struct and a single set of SQL strings switched by operatorTypeToTableName,
-- so a divergent schema would force per-table branching in the query builder.
ALTER TABLE sensor_run ADD COLUMN IF NOT EXISTS run_type VARCHAR(20) NOT NULL DEFAULT 'scheduled';
ALTER TABLE sensor_run ADD COLUMN IF NOT EXISTS triggered_by VARCHAR(255) NOT NULL DEFAULT 'scheduler';
ALTER TABLE sensor_run ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS sensor_run_job_run_id_name_attempt_idx ON sensor_run (job_run_id, name, attempt);

-- Links a task_run / hook_run / sensor_run back to the cause of the run: an Optimus
-- replay_request, an Optimus backfill, or a manual action taken directly in Airflow.
--
-- Rows are written only for non-scheduled runs, so this table stays small relative to task_run.
-- operator_run_id is polymorphic across the three *_run tables, hence no foreign key on it.
CREATE TABLE IF NOT EXISTS operator_run_trigger_source (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    operator_run_id  UUID         NOT NULL,
    operator_type    VARCHAR(10)  NOT NULL,
    job_run_id       UUID         NOT NULL,
    scheduler_run_id VARCHAR(255),

    source_type      VARCHAR(20)  NOT NULL,
    replay_id        UUID         REFERENCES replay_request (id) ON DELETE SET NULL,
    backfill_id      UUID         REFERENCES backfill (id) ON DELETE SET NULL,
    triggered_by     VARCHAR(255) NOT NULL,

    -- How the attribution was decided, so a consumer can tell an exact match from a guess:
    -- optimus_replay | optimus_backfill | airflow_audit_run_id | inherited  -- exact
    -- airflow_audit_heuristic                                              -- correlated by time only
    -- unidentified | pending                                               -- actor not established
    attribution      VARCHAR(30)  NOT NULL,
    resolve_attempts INTEGER      NOT NULL DEFAULT 0,

    audit_event      VARCHAR(100),
    audit_event_id   BIGINT,
    audit_extra      JSONB,

    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS operator_run_trigger_source_run_idx ON operator_run_trigger_source (operator_run_id);
CREATE INDEX IF NOT EXISTS operator_run_trigger_source_job_run_idx ON operator_run_trigger_source (job_run_id);
CREATE INDEX IF NOT EXISTS operator_run_trigger_source_replay_idx ON operator_run_trigger_source (replay_id);
CREATE INDEX IF NOT EXISTS operator_run_trigger_source_backfill_idx ON operator_run_trigger_source (backfill_id);
CREATE INDEX IF NOT EXISTS operator_run_trigger_source_pending_idx ON operator_run_trigger_source (updated_at)
    WHERE attribution = 'pending';
