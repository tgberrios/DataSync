cmake_minimum_required(VERSION 3.16)
project(DataSync)

include(CheckIncludeFile)

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

set(ORACLE_FILES "")

set(ORACLE_INCLUDE_PATHS
    /usr/include/oracle/21/client64
    /usr/include/oracle/19/client64
    /usr/include/oracle/18/client64
    /opt/oracle/instantclient_21_1/sdk/include
    /opt/oracle/instantclient_19_8/sdk/include
    /opt/oracle/instantclient_18_5/sdk/include
)

set(HAVE_ORACLE_HEADER FALSE)
foreach(path ${ORACLE_INCLUDE_PATHS})
  if(EXISTS "${path}/oci.h")
    set(HAVE_ORACLE_HEADER TRUE)
    break()
  endif()
endforeach()

if(HAVE_ORACLE_HEADER)
  add_definitions(-DHAVE_ORACLE)
  message(STATUS "Oracle support: ENABLED")
  set(ORACLE_FILES
    src/governance/DataGovernanceOracle.cpp
    src/governance/LineageExtractorOracle.cpp
    src/engines/oracle_engine.cpp
    src/sync/OracleToPostgres.cpp
  )
else()
  message(STATUS "Oracle support: DISABLED (oci.h not found)")
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
    ./include/backup
    ./include/transformations
    ./include/third_party
    /usr/local/include
    /usr/include/mariadb
    /usr/include/mariadb/mysql
    /opt/microsoft/msodbcsql/include
    /usr/include/libmongoc-1.0
    /usr/include/libbson-1.0
    /usr/include/oracle/21/client64
    /usr/include
)

link_directories(
    /usr/local/lib
    /usr/lib64
    /usr/pgsql-17/lib
    /usr/pgsql-13/lib
    /usr/lib64/mysql
    /opt/microsoft/msodbcsql/lib64
    /usr/lib/oracle/21/client64/lib
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
    ${ORACLE_FILES}
    src/governance/DataQuality.cpp
    src/governance/data_classifier.cpp
    src/governance/QueryStoreCollector.cpp
    src/governance/QueryActivityLogger.cpp
    src/governance/MaintenanceManager.cpp
    src/governance/LineageExtractorMSSQL.cpp
    src/governance/LineageExtractorMariaDB.cpp
    src/governance/LineageExtractorMongoDB.cpp
    src/governance/ColumnCatalogCollector.cpp
    src/governance/ComplianceManager.cpp
    src/governance/AccessControlManager.cpp
    src/governance/DataRetentionManager.cpp
    src/governance/BusinessGlossaryManager.cpp
    src/governance/AlertingManager.cpp
    src/governance/WebhookManager.cpp
    src/metrics/MetricsCollector.cpp
    src/utils/connection_utils.cpp
    src/utils/cluster_name_resolver.cpp
    src/utils/table_utils.cpp
    src/utils/HostnamePatternMatcher.cpp
    src/utils/MariaDBClusterNameProvider.cpp
    src/utils/MSSQLClusterNameProvider.cpp
    src/utils/PostgreSQLClusterNameProvider.cpp
    src/backup/backup_manager.cpp
    src/backup/backup_scheduler.cpp
    src/transformations/transformation_engine.cpp
    src/transformations/lookup_transformation.cpp
    src/transformations/aggregate_transformation.cpp
    src/transformations/join_transformation.cpp
    src/transformations/router_transformation.cpp
    src/transformations/union_transformation.cpp
    src/transformations/sorter_transformation.cpp
    src/transformations/expression_transformation.cpp
    src/transformations/data_cleansing_transformation.cpp
    src/transformations/rank_transformation.cpp
    src/transformations/sequence_generator_transformation.cpp
    src/transformations/window_functions_transformation.cpp
    src/transformations/normalizer_transformation.cpp
    src/transformations/json_parser_transformation.cpp
    src/transformations/geolocation_transformation.cpp
    src/transformations/data_validation_transformation.cpp
    src/transformations/deduplication_transformation.cpp
    src/engines/mariadb_engine.cpp
    src/engines/mssql_engine.cpp
    src/engines/postgres_engine.cpp
    src/engines/mongodb_engine.cpp
    src/engines/db2_engine.cpp
    src/catalog/metadata_repository.cpp
    src/catalog/catalog_cleaner.cpp
    src/catalog/catalog_manager.cpp
    src/catalog/catalog_lock.cpp
    src/sync/DatabaseToPostgresSync.cpp
    src/sync/MariaDBToPostgres.cpp
    src/sync/MSSQLToPostgres.cpp
    src/sync/MongoDBToPostgres.cpp
    src/sync/DB2ToPostgres.cpp
    src/sync/SchemaSync.cpp
    src/sync/StreamingData.cpp
    src/sync/TableProcessorThreadPool.cpp
    src/sync/APIToDatabaseSync.cpp
    src/catalog/api_catalog_repository.cpp
    src/catalog/csv_catalog_repository.cpp
    src/catalog/google_sheets_catalog_repository.cpp
    src/engines/api_engine.cpp
    src/catalog/custom_jobs_repository.cpp
    src/catalog/data_warehouse_repository.cpp
    src/catalog/data_vault_repository.cpp
    src/catalog/workflow_repository.cpp
    src/catalog/dbt_repository.cpp
    src/sync/DBTExecutor.cpp
    src/sync/CustomJobExecutor.cpp
    src/sync/DataWarehouseBuilder.cpp
    src/sync/DataVaultBuilder.cpp
    src/sync/WorkflowExecutor.cpp
    src/sync/EventTriggerManager.cpp
    src/sync/DataDrivenScheduler.cpp
    src/sync/BackfillManager.cpp
    src/sync/TaskQueue.cpp
    src/catalog/WorkflowVersionManager.cpp
    src/engines/csv_engine.cpp
    src/sync/CSVToDatabaseSync.cpp
    src/engines/google_sheets_engine.cpp
    src/sync/GoogleSheetsToDatabaseSync.cpp
    src/sync/PostgreSQLToPostgres.cpp
    src/engines/redshift_engine.cpp
    src/engines/snowflake_engine.cpp
    src/engines/bigquery_engine.cpp
    src/engines/postgres_warehouse_engine.cpp
    src/engines/salesforce_engine.cpp
    src/engines/sap_engine.cpp
    src/engines/teradata_engine.cpp
    src/engines/netezza_engine.cpp
    src/engines/hive_engine.cpp
    src/engines/cassandra_engine.cpp
    src/engines/dynamodb_engine.cpp
    src/engines/as400_engine.cpp
    src/engines/s3_engine.cpp
    src/engines/azure_blob_engine.cpp
    src/engines/gcs_engine.cpp
    src/engines/ftp_engine.cpp
    src/engines/email_engine.cpp
    src/engines/soap_engine.cpp
    src/engines/graphql_engine.cpp
    src/engines/excel_engine.cpp
    src/engines/fixed_width_engine.cpp
    src/engines/ebcdic_engine.cpp
    src/engines/xml_engine.cpp
    src/engines/avro_engine.cpp
    src/engines/parquet_engine.cpp
    src/engines/orc_engine.cpp
    src/engines/compressed_file_engine.cpp
)

# Configure executable output directory to project root
set_target_properties(DataSync PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Add JSON library
add_definitions(-DJSON_USE_IMPLICIT_CONVERSIONS=1)

set(ORACLE_LIBS "")
if(HAVE_ORACLE_HEADER)
    set(ORACLE_LIBS clntsh)
endif()

# Optional libraries for new connectors
# These will be enabled when the libraries are installed
set(OPTIONAL_LIBS "")

# Compression libraries
find_library(ZLIB_LIB z)
if(ZLIB_LIB)
    list(APPEND OPTIONAL_LIBS ${ZLIB_LIB})
    message(STATUS "Found zlib: ${ZLIB_LIB}")
endif()

find_library(BZIP2_LIB bz2)
if(BZIP2_LIB)
    list(APPEND OPTIONAL_LIBS ${BZIP2_LIB})
    message(STATUS "Found bzip2: ${BZIP2_LIB}")
endif()

find_library(LZ4_LIB lz4)
if(LZ4_LIB)
    list(APPEND OPTIONAL_LIBS ${LZ4_LIB})
    message(STATUS "Found lz4: ${LZ4_LIB}")
endif()

# Network and protocol libraries
find_library(SSH2_LIB ssh2)
if(SSH2_LIB)
    list(APPEND OPTIONAL_LIBS ${SSH2_LIB})
    message(STATUS "Found libssh2: ${SSH2_LIB}")
endif()

# File format libraries
find_library(XLSXWRITER_LIB xlsxwriter)
if(XLSXWRITER_LIB)
    list(APPEND OPTIONAL_LIBS ${XLSXWRITER_LIB})
    message(STATUS "Found libxlsxwriter: ${XLSXWRITER_LIB}")
endif()

find_library(PUGIXML_LIB pugixml)
if(PUGIXML_LIB)
    list(APPEND OPTIONAL_LIBS ${PUGIXML_LIB})
    message(STATUS "Found pugixml: ${PUGIXML_LIB}")
endif()

# Data format libraries
find_library(AVRO_LIB avrocpp)
if(AVRO_LIB)
    list(APPEND OPTIONAL_LIBS ${AVRO_LIB})
    add_definitions(-DHAVE_AVRO_CPP)
    message(STATUS "Found avro-cpp: ${AVRO_LIB}")
endif()

find_library(PARQUET_LIB parquet)
if(PARQUET_LIB)
    list(APPEND OPTIONAL_LIBS ${PARQUET_LIB})
    message(STATUS "Found parquet-cpp: ${PARQUET_LIB}")
endif()

# Cloud SDKs
# AWS SDK
find_package(aws-sdk-cpp QUIET)
if(aws-sdk-cpp_FOUND)
    list(APPEND OPTIONAL_LIBS aws-cpp-sdk-core aws-cpp-sdk-s3)
    add_definitions(-DHAVE_AWS_SDK)
    message(STATUS "Found AWS SDK")
    # Check for DynamoDB SDK
    find_library(AWS_SDK_DYNAMODB_LIB aws-cpp-sdk-dynamodb)
    if(AWS_SDK_DYNAMODB_LIB)
        list(APPEND OPTIONAL_LIBS ${AWS_SDK_DYNAMODB_LIB})
        add_definitions(-DHAVE_DYNAMODB_SDK)
        message(STATUS "Found AWS DynamoDB SDK")
    else()
        message(STATUS "AWS DynamoDB SDK not found - DynamoDBEngine will have limited functionality")
    endif()
else()
    # Try finding libraries directly
    find_library(AWS_SDK_CORE_LIB aws-cpp-sdk-core)
    find_library(AWS_SDK_S3_LIB aws-cpp-sdk-s3)
    if(AWS_SDK_CORE_LIB AND AWS_SDK_S3_LIB)
        list(APPEND OPTIONAL_LIBS ${AWS_SDK_CORE_LIB} ${AWS_SDK_S3_LIB})
        add_definitions(-DHAVE_AWS_SDK)
        message(STATUS "Found AWS SDK libraries")
    endif()
    # Also check for DynamoDB SDK
    find_library(AWS_SDK_DYNAMODB_LIB aws-cpp-sdk-dynamodb)
    if(AWS_SDK_DYNAMODB_LIB)
        list(APPEND OPTIONAL_LIBS ${AWS_SDK_DYNAMODB_LIB})
        add_definitions(-DHAVE_DYNAMODB_SDK)
        message(STATUS "Found AWS DynamoDB SDK")
    else()
        message(STATUS "AWS DynamoDB SDK not found - DynamoDBEngine will have limited functionality")
    endif()
endif()

# Azure SDK - try to find azure-storage-cpp
find_library(AZURE_STORAGE_LIB azure-storage)
if(AZURE_STORAGE_LIB)
    list(APPEND OPTIONAL_LIBS ${AZURE_STORAGE_LIB})
    message(STATUS "Found Azure Storage SDK: ${AZURE_STORAGE_LIB}")
endif()

# Google Cloud SDK
find_package(google_cloud_cpp_storage QUIET)
if(google_cloud_cpp_storage_FOUND)
    list(APPEND OPTIONAL_LIBS google-cloud-cpp::storage)
    message(STATUS "Found Google Cloud Storage SDK")
else()
    find_library(GCS_LIB google-cloud-cpp)
    if(GCS_LIB)
        list(APPEND OPTIONAL_LIBS ${GCS_LIB})
        message(STATUS "Found Google Cloud SDK: ${GCS_LIB}")
    endif()
endif()

target_link_libraries(DataSync
    mariadb
    mysqlclient
    pqxx
    pq
    pthread
    odbc
    mongoc-1.0
    bson-1.0
    ${ORACLE_LIBS}
    stdc++fs
    curl
    crypto
    ${OPTIONAL_LIBS}
)
