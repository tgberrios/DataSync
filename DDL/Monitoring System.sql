-- Trigger que se ejecute en UPDATE de metadata.catalog
CREATE OR REPLACE FUNCTION track_processing_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- Solo si cambió last_offset o last_processed_pk
    IF OLD.last_offset != NEW.last_offset OR OLD.last_processed_pk != NEW.last_processed_pk THEN
        -- Insertar en tabla de monitoreo
        INSERT INTO metadata.processing_log (
            schema_name, 
            table_name, 
            db_engine,
            old_offset, 
            new_offset,
            old_pk, 
            new_pk,
            status,
            processed_at
        ) VALUES (
            NEW.schema_name,
            NEW.table_name, 
            NEW.db_engine,
            OLD.last_offset,
            NEW.last_offset,
            OLD.last_processed_pk,
            NEW.last_processed_pk,
            NEW.status,
            NOW()
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Aplicar trigger
CREATE TRIGGER catalog_processing_trigger
    AFTER UPDATE ON metadata.catalog
    FOR EACH ROW
    EXECUTE FUNCTION track_processing_changes();


CREATE TABLE metadata.processing_log (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    db_engine VARCHAR NOT NULL,
    old_offset INTEGER,
    new_offset INTEGER,
    old_pk TEXT,
    new_pk TEXT,
    status VARCHAR,
    processed_at TIMESTAMP DEFAULT NOW()
);