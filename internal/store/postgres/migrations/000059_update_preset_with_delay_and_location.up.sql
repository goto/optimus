ALTER TABLE preset
    ADD COLUMN IF NOT EXISTS window_location VARCHAR(10);
ALTER TABLE preset RENAME COLUMN window_offset TO window_delay;