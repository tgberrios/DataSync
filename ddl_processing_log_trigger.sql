-- Trigger mejorado para capturar cambios en tablas con status PROCESSING
-- Este trigger registra información cuando:
-- 1. El status cambia a 'PROCESSING'
-- 2. El last_processed_pk cambia mientras status = 'PROCESSING'
-- 3. El status cambia desde 'PROCESSING' a otro estado

-- Primero, agregar columna record_count si no existe (opcional)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'metadata' 
        AND table_name = 'processing_log' 
        AND column_name = 'record_count'
    ) THEN
        ALTER TABLE metadata.processing_log ADD COLUMN record_count BIGINT;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION track_processing_changes() 
RETURNS TRIGGER
LANGUAGE plpgsql
AS
$$
DECLARE
    record_count BIGINT := NULL;
BEGIN
    -- Caso 1: Status cambió a PROCESSING
    IF OLD.status != 'PROCESSING' AND NEW.status = 'PROCESSING' THEN
        -- Intentar obtener el count de registros de la tabla (opcional, puede ser costoso)
        BEGIN
            EXECUTE format('SELECT COUNT(*) FROM %I.%I', NEW.schema_name, NEW.table_name) INTO record_count;
        EXCEPTION
            WHEN OTHERS THEN
                record_count := NULL;
        END;
        
        INSERT INTO metadata.processing_log (
            schema_name,
            table_name,
            db_engine,
            old_offset,
            new_offset,
            old_pk,
            new_pk,
            status,
            processed_at,
            record_count
        ) VALUES (
            NEW.schema_name,
            NEW.table_name,
            NEW.db_engine,
            NULL,
            NULL,
            OLD.last_processed_pk,
            NEW.last_processed_pk,
            NEW.status,
            NOW(),
            record_count
        );
    
    -- Caso 2: Status es PROCESSING y last_processed_pk cambió (progreso durante procesamiento)
    ELSIF NEW.status = 'PROCESSING' AND OLD.last_processed_pk IS DISTINCT FROM NEW.last_processed_pk THEN
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
            NULL,
            NULL,
            OLD.last_processed_pk,
            NEW.last_processed_pk,
            NEW.status,
            NOW()
        );
    
    -- Caso 3: Status cambió desde PROCESSING a otro estado (completado o error)
    ELSIF OLD.status = 'PROCESSING' AND NEW.status != 'PROCESSING' THEN
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
            NULL,
            NULL,
            OLD.last_processed_pk,
            NEW.last_processed_pk,
            NEW.status,
            NOW()
        );
    END IF;
    
    RETURN NEW;
END;
$$;

-- Asegurar que el trigger existe y está configurado correctamente
DROP TRIGGER IF EXISTS catalog_processing_trigger ON metadata.catalog;

CREATE TRIGGER catalog_processing_trigger
    AFTER UPDATE ON metadata.catalog
    FOR EACH ROW
    WHEN (
        -- Solo ejecutar si cambió status o last_processed_pk
        (OLD.status IS DISTINCT FROM NEW.status) OR
        (OLD.last_processed_pk IS DISTINCT FROM NEW.last_processed_pk)
    )
    EXECUTE FUNCTION track_processing_changes();

-- Comentario sobre el trigger
COMMENT ON FUNCTION track_processing_changes() IS 
'Captura cambios en metadata.catalog cuando el status es PROCESSING o cambia a/desde PROCESSING. Registra información en metadata.processing_log para monitoreo en tiempo real.';
