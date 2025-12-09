ALTER TABLE metadata.maintenance_control
DROP CONSTRAINT IF EXISTS maintenance_control_unique;

ALTER TABLE metadata.maintenance_control
ADD CONSTRAINT maintenance_control_unique 
UNIQUE (db_engine, maintenance_type, schema_name, object_name, object_type);

COMMENT ON CONSTRAINT maintenance_control_unique ON metadata.maintenance_control IS 
'Ensures uniqueness per database engine, allowing same object in different engines';

