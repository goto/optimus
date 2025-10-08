BEGIN;

ALTER TABLE job_run
    DROP CONSTRAINT IF EXISTS pk_constraint_job_name_scheduled_at;

ALTER TABLE job_run
    ADD CONSTRAINT pk_constraint_project_job_scheduled_at
    UNIQUE (project_name, job_name, scheduled_at);

COMMIT;