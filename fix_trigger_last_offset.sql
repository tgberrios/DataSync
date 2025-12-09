CREATE OR REPLACE FUNCTION metadata.track_processing_changes()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF OLD.last_processed_pk != NEW.last_processed_pk THEN
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
$function$;

