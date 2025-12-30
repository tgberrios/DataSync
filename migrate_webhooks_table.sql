-- Migration script to add log_levels and log_categories columns to webhooks table
-- Run this script in your PostgreSQL database

BEGIN;

-- Add new columns if they don't exist
ALTER TABLE metadata.webhooks 
ADD COLUMN IF NOT EXISTS log_levels JSONB DEFAULT '[]'::jsonb;

ALTER TABLE metadata.webhooks 
ADD COLUMN IF NOT EXISTS log_categories JSONB DEFAULT '[]'::jsonb;

-- Update existing rows to have empty arrays if they are NULL
UPDATE metadata.webhooks 
SET log_levels = '[]'::jsonb 
WHERE log_levels IS NULL;

UPDATE metadata.webhooks 
SET log_categories = '[]'::jsonb 
WHERE log_categories IS NULL;

-- Optional: Drop old columns (uncomment if you want to remove them)
-- ALTER TABLE metadata.webhooks DROP COLUMN IF EXISTS event_types;
-- ALTER TABLE metadata.webhooks DROP COLUMN IF EXISTS severities;

COMMIT;

