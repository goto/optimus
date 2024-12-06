-- Add the deprecation column to the resource table
ALTER TABLE resource 
ADD COLUMN deprecation jsonb;