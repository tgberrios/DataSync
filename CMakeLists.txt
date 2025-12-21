cmake_minimum_required(VERSION 3.16)
project(DataSync)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR NOT CMAKE_BUILD_TYPE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -Wall -Wextra -fsanitize=address -fsanitize=undefined")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")
  message(STATUS "Building in DEBUG mode with AddressSanitizer")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")
  message(STATUS "Building in RELEASE mode")
endif()

include_directories(
    ./include
    ./include/catalog
    ./include/engines
    ./include/sync
    ./include/utils
    ./include/core
    ./include/governance
    ./include/metrics
    ./include/third_party
    /usr/local/include
    /usr/include/mariadb
    /usr/include/mariadb/mysql
    /opt/microsoft/msodbcsql/include
    /usr/include/libmongoc-1.0
    /usr/include/libbson-1.0
    /usr/include
)

link_directories(
    /usr/local/lib
    /usr/lib64
    /usr/pgsql-17/lib
    /usr/pgsql-13/lib
    /usr/lib64/mysql
    /opt/microsoft/msodbcsql/lib64
    /usr/lib
)

add_executable(DataSync
    src/main.cpp
    src/core/database_config.cpp
    src/core/sync_config.cpp
    src/core/logger.cpp
    src/core/database_log_writer.cpp
    src/governance/DataGovernance.cpp
    src/governance/DataGovernanceMSSQL.cpp
    src/governance/DataGovernanceMariaDB.cpp
    src/governance/DataGovernanceMongoDB.cpp
    src/governance/DataGovernanceOracle.cpp
    src/governance/DataQuality.cpp
    src/governance/data_classifier.cpp
    src/governance/QueryStoreCollector.cpp
    src/governance/QueryActivityLogger.cpp
    src/governance/MaintenanceManager.cpp
    src/governance/LineageExtractorMSSQL.cpp
    src/governance/LineageExtractorMariaDB.cpp
    src/governance/LineageExtractorMongoDB.cpp
    src/governance/LineageExtractorOracle.cpp
    src/governance/ColumnCatalogCollector.cpp
    src/governance/ComplianceManager.cpp
    src/governance/AccessControlManager.cpp
    src/governance/DataRetentionManager.cpp
    src/governance/BusinessGlossaryManager.cpp
    src/governance/AlertingManager.cpp
    src/metrics/MetricsCollector.cpp
    src/utils/connection_utils.cpp
    src/utils/cluster_name_resolver.cpp
    src/utils/table_utils.cpp
    src/utils/HostnamePatternMatcher.cpp
    src/utils/MariaDBClusterNameProvider.cpp
    src/utils/MSSQLClusterNameProvider.cpp
    src/utils/PostgreSQLClusterNameProvider.cpp
    src/engines/mariadb_engine.cpp
    src/engines/mssql_engine.cpp
    src/engines/postgres_engine.cpp
    src/engines/mongodb_engine.cpp
    src/engines/oracle_engine.cpp
    src/catalog/metadata_repository.cpp
    src/catalog/catalog_cleaner.cpp
    src/catalog/catalog_manager.cpp
    src/catalog/catalog_lock.cpp
    src/sync/DatabaseToPostgresSync.cpp
    src/sync/MariaDBToPostgres.cpp
    src/sync/MSSQLToPostgres.cpp
    src/sync/MongoDBToPostgres.cpp
    src/sync/OracleToPostgres.cpp
    src/sync/SchemaSync.cpp
    src/sync/StreamingData.cpp
    src/sync/TableProcessorThreadPool.cpp
    src/sync/APIToDatabaseSync.cpp
    src/catalog/api_catalog_repository.cpp
    src/engines/api_engine.cpp
    src/catalog/custom_jobs_repository.cpp
    src/sync/CustomJobExecutor.cpp
)

# Configure executable output directory to project root
set_target_properties(DataSync PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Add JSON library
add_definitions(-DJSON_USE_IMPLICIT_CONVERSIONS=1)

target_link_libraries(DataSync
    mariadb
    mysqlclient
    pqxx
    pq
    pthread
    odbc
    mongoc-1.0
    bson-1.0
    clntsh
    stdc++fs
    curl
)

add_executable(test_api_catalog_repository
    test/test_api_catalog_repository.cpp
    src/catalog/api_catalog_repository.cpp
    src/core/logger.cpp
    src/core/database_log_writer.cpp
    src/core/database_config.cpp
)

target_link_libraries(test_api_catalog_repository
    pqxx
    pq
    pthread
)

set_target_properties(test_api_catalog_repository PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_executable(test_catalog_cleaner
    test/test_catalog_cleaner.cpp
    src/catalog/catalog_cleaner.cpp
    src/catalog/metadata_repository.cpp
    src/core/logger.cpp
    src/core/database_log_writer.cpp
    src/core/database_config.cpp
    src/core/sync_config.cpp
    src/engines/postgres_engine.cpp
    src/engines/mariadb_engine.cpp
    src/engines/mssql_engine.cpp
    src/engines/oracle_engine.cpp
    src/engines/mongodb_engine.cpp
    src/sync/DatabaseToPostgresSync.cpp
    src/sync/MariaDBToPostgres.cpp
    src/sync/MSSQLToPostgres.cpp
    src/sync/OracleToPostgres.cpp
    src/sync/MongoDBToPostgres.cpp
    src/sync/SchemaSync.cpp
    src/utils/table_utils.cpp
    src/utils/connection_utils.cpp
)

target_link_libraries(test_catalog_cleaner
    mariadb
    mysqlclient
    pqxx
    pq
    pthread
    odbc
    mongoc-1.0
    bson-1.0
    clntsh
    curl
)

set_target_properties(test_catalog_cleaner PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_executable(test_catalog_lock
    test/test_catalog_lock.cpp
    src/catalog/catalog_lock.cpp
    src/core/logger.cpp
    src/core/database_log_writer.cpp
    src/core/database_config.cpp
)

target_link_libraries(test_catalog_lock
    pqxx
    pq
    pthread
)

set_target_properties(test_catalog_lock PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_executable(test_catalog_manager
    test/test_catalog_manager.cpp
    src/catalog/catalog_manager.cpp
    src/catalog/catalog_cleaner.cpp
    src/catalog/metadata_repository.cpp
    src/catalog/catalog_lock.cpp
    src/core/logger.cpp
    src/core/database_log_writer.cpp
    src/core/database_config.cpp
    src/core/sync_config.cpp
    src/engines/postgres_engine.cpp
    src/engines/mariadb_engine.cpp
    src/engines/mssql_engine.cpp
    src/engines/oracle_engine.cpp
    src/engines/mongodb_engine.cpp
    src/sync/MariaDBToPostgres.cpp
    src/sync/MSSQLToPostgres.cpp
    src/sync/OracleToPostgres.cpp
    src/sync/DatabaseToPostgresSync.cpp
    src/sync/SchemaSync.cpp
    src/utils/cluster_name_resolver.cpp
    src/utils/MariaDBClusterNameProvider.cpp
    src/utils/MSSQLClusterNameProvider.cpp
    src/utils/PostgreSQLClusterNameProvider.cpp
    src/utils/HostnamePatternMatcher.cpp
    src/utils/connection_utils.cpp
)

target_link_libraries(test_catalog_manager
    mariadb
    mysqlclient
    pqxx
    pq
    pthread
    odbc
    mongoc-1.0
    bson-1.0
    clntsh
    curl
)

set_target_properties(test_catalog_manager PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_executable(test_custom_jobs_repository
    test/test_custom_jobs_repository.cpp
    src/catalog/custom_jobs_repository.cpp
    src/core/logger.cpp
    src/core/database_log_writer.cpp
    src/core/database_config.cpp
)

target_link_libraries(test_custom_jobs_repository
    pqxx
    pq
)

set_target_properties(test_custom_jobs_repository PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_executable(test_metadata_repository
    test/test_metadata_repository.cpp
    src/catalog/metadata_repository.cpp
    src/core/logger.cpp
    src/core/database_log_writer.cpp
    src/core/database_config.cpp
)

target_link_libraries(test_metadata_repository
    pqxx
    pq
)

set_target_properties(test_metadata_repository PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)
