-- Migration: Update pk_strategy default and comment
-- Date: 2024
-- Description: 
--   - Changes pk_strategy default from 'OFFSET' to 'CDC'
--   - Updates comment to reflect that only CDC strategy is used now

BEGIN;

-- Update default value for pk_strategy column
ALTER TABLE metadata.catalog 
ALTER COLUMN pk_strategy SET DEFAULT 'CDC'::character varying;

-- Update comment for pk_strategy column
COMMENT ON COLUMN metadata.catalog.pk_strategy IS 'Synchronization strategy: CDC (Change Data Capture) - monitors database changes in real-time using transaction logs';

COMMIT;

