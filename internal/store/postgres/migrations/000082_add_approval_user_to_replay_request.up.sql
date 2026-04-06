ALTER TABLE replay_request ADD COLUMN IF NOT EXISTS approval_id TEXT NOT NULL DEFAULT '';
ALTER TABLE replay_request ADD COLUMN IF NOT EXISTS user_id TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_replay_request_approval_id_idx ON replay_request (approval_id);