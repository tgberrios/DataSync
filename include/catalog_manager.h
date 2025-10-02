#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H

#include "Config.h"
#include "logger.h"
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <set>
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <string>
#include <vector>

enum class DBEngine { MARIADB, MSSQL, POSTGRES };

enum class CleanupType { NON_EXISTENT, ORPHANED, INCONSISTENT_PAGINATION };

struct ODBCHandles {
  SQLHENV env;
  SQLHDBC dbc;
};

struct CatalogTableInfo {
  std::string schemaName;
  std::string tableName;
  std::string dbEngine;
  std::string connectionString;
  std::string status;
};

struct ValidationResults {
  int totalTables = 0;
  int validatedTables = 0;
  int resetTables = 0;
};

struct MariaDBConnectionInfo {
  std::string host;
  std::string user;
  std::string password;
  std::string database;
  std::string port;
  unsigned int portNumber = 3306;
};

struct CatalogTableMetadata {
  std::string schemaName;
  std::string tableName;
  std::string timeColumn;
  std::vector<std::string> pkColumns;
  std::vector<std::string> candidateColumns;
  std::string pkStrategy;
  bool hasPK = false;
  int64_t tableSize = 0;
};

struct SyncResults {
  int totalConnections = 0;
  int processedConnections = 0;
  int totalTables = 0;
  int updatedTables = 0;
  int newTables = 0;
};

struct MSSQLConnectionInfo {
  std::string server;
  std::string database;
  std::string uid;
  std::string pwd;
  std::string driver;
  std::string port;
  std::string trustedConnection;
};

struct PostgresConnectionInfo {
  std::string host;
  std::string port;
  std::string dbname;
  std::string user;
  std::string password;
  std::string sslmode;
};

class CatalogManager {
public:
  CatalogManager() = default;
  ~CatalogManager() = default;

  // Main catalog operations
  void cleanCatalog();
  void deactivateNoDataTables();
  void updateClusterNames();
  void validateSchemaConsistency();

  // Database-specific sync operations
  void syncCatalogMariaDBToPostgres();
  void syncCatalogMSSQLToPostgres();
  void syncCatalogPostgresToPostgres();

private:
  // Primary key detection functions
  std::vector<std::string> detectPrimaryKeyColumns(MYSQL *conn,
                                                   const std::string &schema,
                                                   const std::string &table);
  std::vector<std::string> detectCandidateColumns(MYSQL *conn,
                                                  const std::string &schema,
                                                  const std::string &table);
  std::string
  determinePKStrategy(const std::vector<std::string> &pkColumns,
                      const std::vector<std::string> &candidateColumns);

  // MSSQL-specific functions
  std::vector<std::string>
  detectPrimaryKeyColumnsMSSQL(SQLHDBC conn, const std::string &schema,
                               const std::string &table);
  std::vector<std::string>
  detectCandidateColumnsMSSQL(SQLHDBC conn, const std::string &schema,
                              const std::string &table);

  // PostgreSQL-specific functions
  std::vector<std::string>
  detectPrimaryKeyColumnsPostgres(pqxx::connection &conn,
                                  const std::string &schema,
                                  const std::string &table);
  std::vector<std::string>
  detectCandidateColumnsPostgres(pqxx::connection &conn,
                                 const std::string &schema,
                                 const std::string &table);

  // Time column detection functions
  std::string detectTimeColumnMSSQL(SQLHDBC conn, const std::string &schema,
                                    const std::string &table);
  std::string detectTimeColumnMariaDB(MYSQL *conn, const std::string &schema,
                                      const std::string &table);
  std::string detectTimeColumnPostgres(pqxx::connection &conn,
                                       const std::string &schema,
                                       const std::string &table);

  // Utility functions
  std::string extractDatabaseName(const std::string &connectionString);

  // Cleanup functions
  void cleanNonExistentPostgresTables(pqxx::connection &pgConn);
  void cleanNonExistentMariaDBTables(pqxx::connection &pgConn);
  void cleanNonExistentMSSQLTables(pqxx::connection &pgConn);
  void cleanOrphanedTables(pqxx::connection &pgConn);
  void cleanInconsistentPaginationFields();

  // Schema validation helper functions
  std::vector<CatalogTableInfo>
  getTablesForValidation(pqxx::connection &pgConn);
  ValidationResults
  validateAllTables(pqxx::connection &pgConn,
                    const std::vector<CatalogTableInfo> &tables);
  void logValidationResults(const ValidationResults &results);
  bool validateTableSchema(pqxx::connection &pgConn,
                           const CatalogTableInfo &table);
  void resetTableSchema(pqxx::connection &pgConn,
                        const CatalogTableInfo &table);
  std::pair<int, int>
  getColumnCountsForEngine(const std::string &dbEngine,
                           const std::string &connectionString,
                           const std::string &schema, const std::string &table);

  // MariaDB sync helper functions
  std::vector<std::string> getMariaDBConnections(pqxx::connection &pgConn);
  MariaDBConnectionInfo
  parseMariaDBConnectionString(const std::string &connectionString);
  MYSQL *establishMariaDBConnection(const std::string &connectionString);
  void configureMariaDBTimeouts(MYSQL *conn);
  std::vector<std::vector<std::string>> discoverMariaDBTables(MYSQL *conn);
  CatalogTableMetadata analyzeTableMetadata(MYSQL *conn,
                                            const std::string &schemaName,
                                            const std::string &tableName);
  int64_t getTableSize(pqxx::connection &pgConn, const std::string &schemaName,
                       const std::string &tableName);
  void updateOrInsertTableMetadata(pqxx::connection &pgConn,
                                   const std::string &connectionString,
                                   const CatalogTableMetadata &metadata);
  SyncResults processMariaDBConnection(pqxx::connection &pgConn,
                                       const std::string &connectionString);
  void logSyncResults(const SyncResults &results);

  // MSSQL sync helper functions
  std::vector<std::string> getMSSQLConnections(pqxx::connection &pgConn);
  MSSQLConnectionInfo
  parseMSSQLConnectionString(const std::string &connectionString);
  SQLHDBC establishMSSQLConnection(const std::string &connectionString);
  std::vector<std::vector<std::string>> discoverMSSQLTables(SQLHDBC conn);
  CatalogTableMetadata analyzeMSSQLTableMetadata(SQLHDBC conn,
                                                 const std::string &schemaName,
                                                 const std::string &tableName);
  SyncResults processMSSQLConnection(pqxx::connection &pgConn,
                                     const std::string &connectionString);

  // PostgreSQL sync helper functions
  std::vector<std::string> getPostgresConnections(pqxx::connection &pgConn);
  PostgresConnectionInfo
  parsePostgresConnectionString(const std::string &connectionString);
  std::unique_ptr<pqxx::connection>
  establishPostgresConnection(const PostgresConnectionInfo &connInfo);
  std::vector<std::vector<std::string>>
  discoverPostgresTables(pqxx::connection &conn);
  CatalogTableMetadata
  analyzePostgresTableMetadata(pqxx::connection &conn,
                               const std::string &schemaName,
                               const std::string &tableName);
  SyncResults processPostgresConnection(pqxx::connection &pgConn,
                                        const std::string &connectionString);

  // Unified utility functions
  std::string escapeSQL(const std::string &input);
  std::string columnsToJSON(const std::vector<std::string> &columns);
  std::string
  determinePKStrategy(const std::vector<std::string> &pkColumns,
                      const std::vector<std::string> &candidateColumns,
                      const std::string &timeColumn);

  // Unified detection functions
  std::vector<std::string> detectPrimaryKeyColumns(DBEngine engine,
                                                   void *connection,
                                                   const std::string &schema,
                                                   const std::string &table);
  std::vector<std::string> detectCandidateColumns(DBEngine engine,
                                                  void *connection,
                                                  const std::string &schema,
                                                  const std::string &table);
  std::string detectTimeColumn(DBEngine engine, void *connection,
                               const std::string &schema,
                               const std::string &table);

  // Unified utility functions
  std::pair<int, int> getColumnCounts(DBEngine engine,
                                      const std::string &connectionString,
                                      const std::string &schema,
                                      const std::string &table);
  std::vector<std::vector<std::string>>
  executeQuery(DBEngine engine, void *connection, const std::string &query);

  // Unified cleanup functions
  void cleanCatalogTables(DBEngine engine, pqxx::connection &pgConn,
                          CleanupType type);

  // Unified naming functions
  std::string resolveClusterName(const std::string &connectionString,
                                 DBEngine engine);
  std::string extractHostnameFromConnection(const std::string &connectionString,
                                            DBEngine engine);
  std::string getClusterNameFromHostname(const std::string &hostname);

  // Legacy functions (for backward compatibility)
  std::vector<std::vector<std::string>>
  executeQueryMariaDB(MYSQL *conn, const std::string &query);
  std::vector<std::vector<std::string>>
  executeQueryMSSQL(SQLHDBC conn, const std::string &query);
  std::pair<int, int>
  getColumnCountsMariaDB(const std::string &connectionString,
                         const std::string &schema, const std::string &table);
  std::pair<int, int> getColumnCountsMSSQL(const std::string &connectionString,
                                           const std::string &schema,
                                           const std::string &table);
  std::pair<int, int>
  getColumnCountsPostgres(const std::string &connectionString,
                          const std::string &schema, const std::string &table);
};

#endif // CATALOG_MANAGER_H
