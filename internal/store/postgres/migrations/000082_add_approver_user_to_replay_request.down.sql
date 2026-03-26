DROP INDEX IF EXISTS idx_replay_request_approver_id_unique;
DROP INDEX IF EXISTS idx_replay_request_user_id_idx;
ALTER TABLE replay_request DROP COLUMN IF EXISTS approver_id;
ALTER TABLE replay_request DROP COLUMN IF EXISTS user_id;
