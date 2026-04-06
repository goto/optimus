DROP INDEX IF EXISTS idx_replay_request_approval_id_idx;
ALTER TABLE replay_request DROP COLUMN IF EXISTS approval_id;
ALTER TABLE replay_request DROP COLUMN IF EXISTS user_id;
