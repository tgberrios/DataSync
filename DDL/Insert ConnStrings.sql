--MSQL
INSERT INTO metadata.catalog
(schema_name, table_name, cluster_name, db_engine, connection_string, last_sync_time, last_sync_column, status, last_offset, active)
VALUES ('dbo', 'RegistrosPrueba', '', 'MSSQL', 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=10.12.240.66;DATABASE=PruebaDB1;UID=Datalake_User;PWD=keepprofessional;TrustServerCertificate=yes;', NOW(), '', 'FULL_LOAD', '0', true);


-- MariaDB Test Table
INSERT INTO metadata.catalog
(schema_name, table_name, cluster_name, db_engine, connection_string, last_sync_time, last_sync_column, status, last_offset, active)
VALUES ('SportBook', 'productos', '', 'MariaDB', 'host=localhost;user=root;password=Yucaquemada1;db=SportBook', NOW(), 'fecha_creacion', 'FULL_LOAD', '0', true);

-- PostgreSQL Test Table
INSERT INTO metadata.catalog
(schema_name, table_name, cluster_name, db_engine, connection_string, last_sync_time, last_sync_column, status, last_offset, active)
VALUES ('test_schema', 'test_table', '', 'PostgreSQL', 'host=localhost user=tomy.berrios password=Yucaquemada1 dbname=postgres', NOW(), 'created_at', 'FULL_LOAD', '0', true);

-- MongoDB Test Table
INSERT INTO metadata.catalog
(schema_name, table_name, cluster_name, db_engine, connection_string, last_sync_time, last_sync_column, status, last_offset, active)
VALUES ('mydatabase', 'usuarios', '', 'MongoDB', 'mongodb://localhost:27017', NOW(), '', 'FULL_LOAD', '0', true);

-- MSSQL Test Table
INSERT INTO metadata.catalog
(schema_name, table_name, cluster_name, db_engine, connection_string, last_sync_time, last_sync_column, status, last_offset, active)
VALUES ('dbo', 'customers', '', 'MSSQL', 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=master;UID=sa;PWD=Yucaquemada1;TrustServerCertificate=yes;', NOW(), 'created_at', 'FULL_LOAD', '0', true);


UPDATE metadata.catalog
SET status = 'RESET', last_offset = 0
WHERE active = true;



