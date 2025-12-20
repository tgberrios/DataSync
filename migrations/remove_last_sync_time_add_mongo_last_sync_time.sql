-- Migration: Remove last_sync_time and add mongo_last_sync_time
-- Date: 2024
-- Description: 
--   - Removes last_sync_time column from metadata.catalog (no longer needed for CDC)
--   - Adds mongo_last_sync_time column for MongoDB-specific sync tracking
--   - Keeps last_sync_time in metadata.api_catalog (separate table, still needed)

BEGIN;

-- Add mongo_last_sync_time column for MongoDB
ALTER TABLE metadata.catalog 
ADD COLUMN IF NOT EXISTS mongo_last_sync_time TIMESTAMP;

-- Create index for MongoDB queries
CREATE INDEX IF NOT EXISTS idx_catalog_mongo_last_sync_time 
ON metadata.catalog (mongo_last_sync_time) 
WHERE db_engine = 'MongoDB';

-- Migrate existing MongoDB last_sync_time data to mongo_last_sync_time
UPDATE metadata.catalog 
SET mongo_last_sync_time = last_sync_time 
WHERE db_engine = 'MongoDB' AND last_sync_time IS NOT NULL;

-- Drop last_sync_time column from metadata.catalog
-- Note: This will fail if there are views or other dependencies
-- You may need to drop those first or update them
ALTER TABLE metadata.catalog 
DROP COLUMN IF EXISTS last_sync_time;

COMMIT;

