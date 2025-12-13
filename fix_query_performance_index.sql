-- Fix for: ERROR: index row requires 24432 bytes, maximum size is 8191
-- The query_fingerprint column can contain very long values that exceed PostgreSQL's
-- index size limit of 8191 bytes. This script fixes the index by using a functional
-- index that only indexes the first 100 characters.

-- Drop the existing index if it exists
DROP INDEX IF EXISTS metadata.idx_qp_fingerprint;

-- Create a new functional index that only indexes the first 100 characters
-- This ensures the index entry size stays well below the 8191 byte limit
CREATE INDEX idx_qp_fingerprint ON metadata.query_performance USING btree (LEFT(query_fingerprint, 100));

-- Verify the index was created correctly
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'query_performance' 
  AND schemaname = 'metadata' 
  AND indexname = 'idx_qp_fingerprint';

