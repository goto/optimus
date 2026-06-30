DROP INDEX IF EXISTS backfill_project_name_idx;
DROP INDEX IF EXISTS backfill_job_name_idx;
DROP INDEX IF EXISTS backfill_scheduler_run_id_idx;
DROP INDEX IF EXISTS backfill_status_idx;
DROP INDEX IF EXISTS backfill_approval_id_idx;
DROP INDEX IF EXISTS backfill_user_id_idx;

DROP TABLE IF EXISTS backfill;