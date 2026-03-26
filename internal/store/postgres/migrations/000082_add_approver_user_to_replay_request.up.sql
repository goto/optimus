ALTER TABLE replay_request ADD COLUMN IF NOT EXISTS approver_id TEXT;
ALTER TABLE replay_request ADD COLUMN IF NOT EXISTS user_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_request_approver_id_unique ON replay_request (approver_id) WHERE approver_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_replay_request_user_id_idx ON replay_request (user_id);
