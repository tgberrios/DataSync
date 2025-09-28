-- Script seguro y específico
CREATE LOGIN [Datalake_User] WITH PASSWORD = 'keepprofessional'
GO

USE [Comm100.Site10000]
GO
CREATE USER [Datalake_User] FOR LOGIN [Datalake_User]
GO

-- Solo lo necesario
ALTER ROLE db_datareader ADD MEMBER [Datalake_User]
ALTER ROLE db_denydatawriter ADD MEMBER [Datalake_User]
GO

GRANT VIEW ANY DEFINITION TO [Datalake_User]
GO

-- Conectarte a SQL Server y verificar
SELECT name FROM sys.databases WHERE name LIKE '%Comm100%';


-- Solo dar permisos necesarios para la base de datos específica
USE [Comm100.Site10000];
CREATE USER [Datalake_User] FOR LOGIN [Datalake_User];
ALTER ROLE db_datareader ADD MEMBER [Datalake_User];
ALTER ROLE db_datawriter ADD MEMBER [Datalake_User];