# Configuración de pg_stat_statements

La extensión `pg_stat_statements` requiere configuración adicional en PostgreSQL:

## Pasos para habilitar:

1. Editar `postgresql.conf`:
   ```
   shared_preload_libraries = 'pg_stat_statements'
   pg_stat_statements.max = 10000
   pg_stat_statements.track = all
   ```

2. Reiniciar PostgreSQL:
   ```bash
   sudo systemctl restart postgresql
   # o
   sudo service postgresql restart
   ```

3. Ejecutar el script SQL:
   ```bash
   psql -d DataLake -f schema_migrations/006_enable_pg_stat_statements.sql
   ```

## Nota:

- `QueryActivityLogger` funciona sin esta configuración (usa `pg_stat_activity`)
- `QueryStoreCollector` requiere `pg_stat_statements` habilitado
- Si `pg_stat_statements` no está disponible, `QueryStoreCollector` solo mostrará un warning
