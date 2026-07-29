DROP TABLE IF EXISTS operator_run_trigger_source;

DROP INDEX IF EXISTS sensor_run_job_run_id_name_attempt_idx;
ALTER TABLE sensor_run DROP COLUMN IF EXISTS attempt;
ALTER TABLE sensor_run DROP COLUMN IF EXISTS triggered_by;
ALTER TABLE sensor_run DROP COLUMN IF EXISTS run_type;

DROP INDEX IF EXISTS hook_run_job_run_id_name_attempt_idx;
ALTER TABLE hook_run DROP COLUMN IF EXISTS attempt;
ALTER TABLE hook_run DROP COLUMN IF EXISTS triggered_by;
ALTER TABLE hook_run DROP COLUMN IF EXISTS run_type;

DROP INDEX IF EXISTS task_run_job_run_id_name_attempt_idx;
ALTER TABLE task_run DROP COLUMN IF EXISTS attempt;
ALTER TABLE task_run DROP COLUMN IF EXISTS triggered_by;
ALTER TABLE task_run DROP COLUMN IF EXISTS run_type;

DROP INDEX IF EXISTS job_run_scheduler_run_id_idx;
ALTER TABLE job_run DROP COLUMN IF EXISTS scheduler_run_id;
