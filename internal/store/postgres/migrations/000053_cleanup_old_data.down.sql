ALTER TABLE hook_run DROP CONSTRAINT IF EXISTS  hook_run_job_id_fkey;

ALTER TABLE hook_run ADD CONSTRAINT hook_run_job_id_fkey
FOREIGN KEY (job_run_id) REFERENCES job_run(id) ON DELETE CASCADE;


ALTER TABLE sensor_run DROP CONSTRAINT sensor_run_job_id_fkey;

ALTER TABLE sensor_run ADD CONSTRAINT sensor_run_job_id_fkey
FOREIGN KEY (job_run_id) REFERENCES job_run(id) ON DELETE CASCADE;


ALTER TABLE task_run DROP CONSTRAINT task_run_job_id_fkey;

ALTER TABLE task_run ADD CONSTRAINT task_run_job_id_fkey
FOREIGN KEY (job_run_id) REFERENCES job_run(id) ON DELETE CASCADE;