UPDATE preset
SET window_shift_by = CASE
    WHEN
        window_shift_by IS NULL OR
        window_shift_by = '' OR
        window_shift_by = 'None' OR
        LENGTH(window_shift_by) < 2 THEN
            window_shift_by
    WHEN
        window_shift_by LIKE '-%' THEN
            RIGHT(window_shift_by, LENGTH(window_shift_by) - 1)
    ELSE
        CONCAT('-', window_shift_by)
END;

ALTER TABLE preset RENAME COLUMN window_shift_by TO window_delay;
