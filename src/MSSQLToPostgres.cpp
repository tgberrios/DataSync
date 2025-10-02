#include "MSSQLToPostgres.h"

// Static member definitions
std::unordered_map<std::string, std::string> MSSQLToPostgres::dataTypeMap = {
    {"int", "INTEGER"},
    {"bigint", "BIGINT"},
    {"smallint", "SMALLINT"},
    {"tinyint", "SMALLINT"},
    {"bit", "BOOLEAN"},
    {"decimal", "NUMERIC"},
    {"numeric", "NUMERIC"},
    {"float", "REAL"},
    {"real", "REAL"},
    {"money", "NUMERIC(19,4)"},
    {"smallmoney", "NUMERIC(10,4)"},
    {"varchar", "VARCHAR"},
    {"nvarchar", "VARCHAR"},
    {"char", "CHAR"},
    {"nchar", "CHAR"},
    {"text", "TEXT"},
    {"ntext", "TEXT"},
    {"datetime", "TIMESTAMP"},
    {"datetime2", "TIMESTAMP"},
    {"smalldatetime", "TIMESTAMP"},
    {"date", "DATE"},
    {"time", "TIME"},
    {"datetimeoffset", "TIMESTAMP WITH TIME ZONE"},
    {"uniqueidentifier", "UUID"},
    {"varbinary", "BYTEA"},
    {"image", "BYTEA"},
    {"binary", "BYTEA"},
    {"xml", "TEXT"},
    {"sql_variant", "TEXT"}};

std::unordered_map<std::string, std::string> MSSQLToPostgres::collationMap = {
    {"SQL_Latin1_General_CP1_CI_AS", "en_US.utf8"},
    {"Latin1_General_CI_AS", "en_US.utf8"},
    {"SQL_Latin1_General_CP1_CS_AS", "C"},
    {"Latin1_General_CS_AS", "C"}};

// Public interface methods - delegate to specialized components

SQLHDBC
MSSQLToPostgres::getMSSQLConnection(const std::string &connectionString) {
  return connectionManager.getMSSQLConnection(connectionString);
}

void MSSQLToPostgres::closeMSSQLConnection(SQLHDBC conn) {
  connectionManager.closeMSSQLConnection(conn);
}

void MSSQLToPostgres::setupTableTargetMSSQLToPostgres() {
  tableSetup.setupTableTargetMSSQLToPostgres();
}

void MSSQLToPostgres::transferDataMSSQLToPostgres() {
  dataTransfer.transferDataMSSQLToPostgres();
}

std::vector<TableInfo>
MSSQLToPostgres::getActiveTables(pqxx::connection &pgConn) {
  return queryExecutor.getActiveTables(pgConn);
}

void MSSQLToPostgres::syncIndexesAndConstraints(
    const std::string &schema_name, const std::string &table_name,
    pqxx::connection &pgConn, const std::string &lowerSchemaName,
    const std::string &connection_string) {
  tableSetup.syncIndexesAndConstraints(schema_name, table_name, pgConn,
                                       lowerSchemaName, connection_string);
}
