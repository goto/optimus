ALTER TABLE preset DROP COLUMN IF EXISTS window_location;
ALTER TABLE preset RENAME COLUMN window_delay TO window_offset;