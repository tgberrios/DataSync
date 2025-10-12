                      ddl                      
-----------------------------------------------
 CREATE SCHEMA IF NOT EXISTS metadata;
 
 -- ==========================================
 -- TABLES
 -- ==========================================
(5 rows)

                                                                               List of relations
  Schema  |          Name           | Type  |    Owner     | Persistence | Access method |  Size   |                                Description                                 
----------+-------------------------+-------+--------------+-------------+---------------+---------+----------------------------------------------------------------------------
 metadata | catalog                 | table | tomy.berrios | permanent   | heap          | 360 kB  | Metadata catalog for all tables managed by DataSync system
 metadata | catalog_locks           | table | tomy.berrios | permanent   | heap          | 32 kB   | Distributed locks for catalog operations to prevent race conditions
 metadata | config                  | table | tomy.berrios | permanent   | heap          | 16 kB   | 
 metadata | data_governance_catalog | table | tomy.berrios | permanent   | heap          | 88 kB   | Comprehensive metadata catalog for all tables in the DataLake
 metadata | data_quality            | table | tomy.berrios | permanent   | heap          | 14 MB   | Stores data quality metrics and validation results for synchronized tables
 metadata | logs                    | table | tomy.berrios | permanent   | heap          | 280 kB  | 
 metadata | processing_log          | table | tomy.berrios | permanent   | heap          | 2208 kB | 
 metadata | transfer_metrics        | table | tomy.berrios | permanent   | heap          | 160 kB  | Real database metrics for data transfer operations
(8 rows)

