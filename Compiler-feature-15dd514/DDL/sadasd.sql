UPDATE metadata.catalog
SET status = 'FULL_LOAD', last_offset = 0
where active = true