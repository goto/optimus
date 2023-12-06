ALTER TABLE preset
    ADD COLUMN IF NOT EXISTS window_location VARCHAR(50) default '';
ALTER TABLE preset RENAME COLUMN window_offset TO window_delay;