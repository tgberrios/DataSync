-- Migration: Remove last_processed_pk and has_pk columns
-- Date: 2024
-- Description: 
--   - Removes last_processed_pk column (no longer needed with CDC)
--   - Removes has_pk column (can be derived from pk_columns)
--   - Both columns are redundant with the current CDC-based architecture
--   - Uses CASCADE to automatically drop dependent objects (triggers, etc.)

BEGIN;

-- Drop trigger that depends on last_processed_pk (if it exists)
DROP TRIGGER IF EXISTS catalog_processing_trigger ON metadata.catalog CASCADE;

-- Drop last_processed_pk column with CASCADE to remove all dependencies
ALTER TABLE metadata.catalog 
DROP COLUMN IF EXISTS last_processed_pk CASCADE;

-- Drop has_pk column
ALTER TABLE metadata.catalog 
DROP COLUMN IF EXISTS has_pk;

COMMIT;

