ALTER TABLE replay_request ADD COLUMN IF NOT EXISTS approval_id TEXT;
ALTER TABLE replay_request ADD COLUMN IF NOT EXISTS user_id TEXT;

CREATE INDEX IF NOT EXISTS idx_replay_request_approval_id_idx ON replay_request (approval_id);