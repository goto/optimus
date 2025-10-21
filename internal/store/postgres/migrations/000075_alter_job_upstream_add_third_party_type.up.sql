ALTER TABLE job_upstream
ADD COLUMN if not exists upstream_third_party_type VARCHAR(50) DEFAULT NULL;
