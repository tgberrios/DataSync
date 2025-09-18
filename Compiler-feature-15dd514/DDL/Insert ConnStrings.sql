--MSQL
INSERT INTO metadata.catalog
(schema_name, table_name, cluster_name, db_engine, connection_string, last_sync_time, last_sync_column, status, last_offset, active)
VALUES ('dbo', 'RegistrosPrueba', '', 'MSSQL', 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=10.12.240.66;DATABASE=PruebaDB1;UID=Datalake_User;PWD=keepprofessional;TrustServerCertificate=yes;', NOW(), '', 'FULL_LOAD', '0', true);