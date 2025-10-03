#include "MSSQLTableSetup.h"
#include <algorithm>
#include <sstream>

// Static member definitions
std::unordered_map<std::string, std::string> MSSQLTableSetup::dataTypeMap = {
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

std::unordered_map<std::string, std::string> MSSQLTableSetup::collationMap = {
    {"SQL_Latin1_General_CP1_CI_AS", "en_US.utf8"},
    {"Latin1_General_CI_AS", "en_US.utf8"},
    {"SQL_Latin1_General_CP1_CS_AS", "C"},
    {"Latin1_General_CS_AS", "C"}};

void MSSQLTableSetup::createTableSchema(const TableInfo &table,
                                        pqxx::connection &pgConn,
                                        SQLHDBC mssqlConn) {
  try {
    // Primero cambiar a la base de datos correcta
    std::string databaseName =
        connectionManager.extractDatabaseName(table.connection_string);
    std::string useQuery = "USE [" + databaseName + "];";
    auto useResult = connectionManager.executeQueryMSSQL(mssqlConn, useQuery);

    // Obtener información de columnas
    std::string query =
        "SELECT c.name AS COLUMN_NAME, tp.name AS DATA_TYPE, "
        "CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END as IS_NULLABLE, "
        "CASE WHEN pk.column_id IS NOT NULL THEN 'YES' ELSE 'NO' END as "
        "IS_PRIMARY_KEY, "
        "c.max_length AS CHARACTER_MAXIMUM_LENGTH, "
        "c.precision AS NUMERIC_PRECISION, "
        "c.scale AS NUMERIC_SCALE, "
        "NULL AS COLUMN_DEFAULT "
        "FROM sys.columns c "
        "INNER JOIN sys.tables t ON c.object_id = t.object_id "
        "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "INNER JOIN sys.types tp ON c.user_type_id = tp.user_type_id "
        "LEFT JOIN ( "
        "  SELECT ic.column_id, ic.object_id "
        "  FROM sys.indexes i "
        "  INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id "
        "AND i.index_id = ic.index_id "
        "  WHERE i.is_primary_key = 1 "
        ") pk ON c.column_id = pk.column_id AND t.object_id = pk.object_id "
        "WHERE s.name = '" +
        escapeSQL(table.schema_name) + "' AND t.name = '" +
        escapeSQL(table.table_name) +
        "' "
        "ORDER BY c.column_id;";

    std::vector<std::vector<std::string>> columns =
        connectionManager.executeQueryMSSQL(mssqlConn, query);

    if (columns.empty()) {
      Logger::getInstance().error(LogCategory::TRANSFER,
                                  "No columns found for table " +
                                      table.schema_name + "." +
                                      table.table_name + " - skipping");
      return;
    }

    std::string lowerSchema = table.schema_name;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);

    {
      pqxx::work txn(pgConn);
      txn.exec("CREATE SCHEMA IF NOT EXISTS \"" + lowerSchema + "\";");
      txn.commit();
    }

    std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" + lowerSchema +
                              "\".\"" + table.table_name + "\" (";
    std::vector<std::string> primaryKeys;

    for (const std::vector<std::string> &col : columns) {
      if (col.size() < 8)
        continue;

      std::string colName = col[0];
      std::transform(colName.begin(), colName.end(), colName.begin(),
                     ::tolower);
      std::string dataType = col[1];
      std::string nullable = col[2] == "YES" ? "" : " NOT NULL";
      std::string isPrimaryKey = col[3];
      std::string maxLength = col[4];
      std::string numericPrecision = col[5];
      std::string numericScale = col[6];

      std::string pgType =
          mapDataType(dataType, maxLength, numericPrecision, numericScale);

      createQuery += "\"" + colName + "\" " + pgType + nullable;
      if (isPrimaryKey == "YES")
        primaryKeys.push_back(colName);
      createQuery += ", ";
    }

    if (!primaryKeys.empty()) {
      createQuery += "PRIMARY KEY (";
      for (size_t i = 0; i < primaryKeys.size(); ++i) {
        createQuery += "\"" + primaryKeys[i] + "\"";
        if (i < primaryKeys.size() - 1)
          createQuery += ", ";
      }
      createQuery += ")";
    } else {
      createQuery.erase(createQuery.size() - 2, 2);
    }
    createQuery += ");";

    {
      pqxx::work txn(pgConn);
      txn.exec(createQuery);
      txn.commit();
    }

    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Created table schema for " + table.schema_name +
                                   "." + table.table_name);

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error creating table schema for " +
                                    table.schema_name + "." + table.table_name +
                                    ": " + std::string(e.what()));
  }
}

void MSSQLTableSetup::setupTableTargetMSSQLToPostgres() {
  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Starting MSSQL table target setup");

  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

    if (!pgConn.is_open()) {
      Logger::getInstance().error(
          LogCategory::TRANSFER,
          "CRITICAL ERROR: Cannot establish PostgreSQL connection "
          "for MSSQL table setup");
      return;
    }

    Logger::getInstance().info(
        LogCategory::TRANSFER,
        "PostgreSQL connection established for MSSQL table setup");

    // Get active tables
    pqxx::work txn(pgConn);
    auto results = txn.exec(
        "SELECT schema_name, table_name, cluster_name, db_engine, "
        "connection_string, last_sync_time, last_sync_column, "
        "status, last_offset, last_processed_pk, pk_strategy, "
        "pk_columns, candidate_columns, has_pk, table_size "
        "FROM metadata.catalog "
        "WHERE active=true AND db_engine='MSSQL' AND status != 'NO_DATA' "
        "ORDER BY table_size ASC, schema_name, table_name;");
    txn.commit();

    std::vector<TableInfo> tables;
    for (const auto &row : results) {
      if (row.size() < 15)
        continue;

      TableInfo t;
      t.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
      t.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
      t.cluster_name = row[2].is_null() ? "" : row[2].as<std::string>();
      t.db_engine = row[3].is_null() ? "" : row[3].as<std::string>();
      t.connection_string = row[4].is_null() ? "" : row[4].as<std::string>();
      t.last_sync_time = row[5].is_null() ? "" : row[5].as<std::string>();
      t.last_sync_column = row[6].is_null() ? "" : row[6].as<std::string>();
      t.status = row[7].is_null() ? "" : row[7].as<std::string>();
      t.last_offset = row[8].is_null() ? "" : row[8].as<std::string>();
      t.last_processed_pk = row[9].is_null() ? "" : row[9].as<std::string>();
      t.pk_strategy = row[10].is_null() ? "" : row[10].as<std::string>();
      t.pk_columns = row[11].is_null() ? "" : row[11].as<std::string>();
      t.candidate_columns = row[12].is_null() ? "" : row[12].as<std::string>();
      t.has_pk = row[13].is_null() ? false : row[13].as<bool>();
      tables.push_back(t);
    }

    if (tables.empty()) {
      Logger::getInstance().info(LogCategory::TRANSFER,
                                 "No active MSSQL tables found to setup");
      return;
    }

    // Sort tables by priority
    sortTablesByPriority(tables);

    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Processing " + std::to_string(tables.size()) +
                                   " MSSQL tables in priority order");

    for (const auto &table : tables) {
      if (table.db_engine != "MSSQL") {
        Logger::getInstance().warning(
            LogCategory::TRANSFER,
            "Skipping non-MSSQL table: " + table.db_engine + " - " +
                table.schema_name + "." + table.table_name);
        continue;
      }

      SQLHDBC dbc =
          connectionManager.getMSSQLConnection(table.connection_string);
      if (!dbc) {
        Logger::getInstance().error(
            LogCategory::TRANSFER,
            "CRITICAL ERROR: Failed to get MSSQL connection for table " +
                table.schema_name + "." + table.table_name +
                " - skipping table setup");
        continue;
      }

      createTableSchema(table, pgConn, dbc);
      connectionManager.closeMSSQLConnection(dbc);
    }

    Logger::getInstance().info(LogCategory::TRANSFER,
                               "MSSQL table target setup completed");

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error in setupTableTargetMSSQLToPostgres: " +
                                    std::string(e.what()));
  }
}

void MSSQLTableSetup::syncIndexesAndConstraints(
    const std::string &schema_name, const std::string &table_name,
    pqxx::connection &pgConn, const std::string &lowerSchemaName,
    const std::string &connection_string) {
  // Validate input parameters
  if (schema_name.empty() || table_name.empty() || lowerSchemaName.empty() ||
      connection_string.empty()) {
    Logger::getInstance().error(
        LogCategory::TRANSFER, "syncIndexesAndConstraints",
        "Invalid parameters: schema_name, table_name, "
        "lowerSchemaName, or connection_string is empty");
    return;
  }

  SQLHDBC dbc = connectionManager.getMSSQLConnection(connection_string);
  if (!dbc) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "syncIndexesAndConstraints",
                                "Failed to get MSSQL connection");
    return;
  }

  std::string query = "SELECT i.name AS index_name, "
                      "CASE WHEN i.is_unique = 1 THEN 'UNIQUE' ELSE "
                      "'NON_UNIQUE' END AS uniqueness, "
                      "c.name AS column_name "
                      "FROM sys.indexes i "
                      "INNER JOIN sys.index_columns ic ON i.object_id = "
                      "ic.object_id AND i.index_id = ic.index_id "
                      "INNER JOIN sys.columns c ON ic.object_id = "
                      "c.object_id AND ic.column_id = c.column_id "
                      "INNER JOIN sys.tables t ON i.object_id = t.object_id "
                      "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
                      "WHERE s.name = '" +
                      schema_name + "' AND t.name = '" + table_name +
                      "' "
                      "AND i.name IS NOT NULL AND i.is_primary_key = 0 "
                      "ORDER BY i.name, ic.key_ordinal;";

  std::vector<std::vector<std::string>> results =
      connectionManager.executeQueryMSSQL(dbc, query);

  for (const auto &row : results) {
    if (row.size() < 3)
      continue;

    std::string indexName = row[0];
    std::string uniqueness = row[1];
    std::string columnName = row[2];
    std::transform(columnName.begin(), columnName.end(), columnName.begin(),
                   ::tolower);

    std::string createQuery = "CREATE ";
    if (uniqueness == "UNIQUE")
      createQuery += "UNIQUE ";
    createQuery += "INDEX IF NOT EXISTS \"" + indexName + "\" ON \"" +
                   lowerSchemaName + "\".\"" + table_name + "\" (\"" +
                   columnName + "\");";

    try {
      pqxx::work txn(pgConn);
      txn.exec(createQuery);
      txn.commit();
    } catch (const pqxx::sql_error &e) {
      Logger::getInstance().error(LogCategory::TRANSFER,
                                  "syncIndexesAndConstraints",
                                  "SQL ERROR creating index '" + indexName +
                                      "': " + std::string(e.what()) +
                                      " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::getInstance().error(
          LogCategory::TRANSFER, "syncIndexesAndConstraints",
          "CONNECTION ERROR creating index '" + indexName +
              "': " + std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::getInstance().error(
          LogCategory::TRANSFER, "syncIndexesAndConstraints",
          "ERROR creating index '" + indexName + "': " + std::string(e.what()));
    }
  }

  connectionManager.closeMSSQLConnection(dbc);
}

std::string MSSQLTableSetup::mapDataType(const std::string &mssqlType,
                                         const std::string &maxLength,
                                         const std::string &numericPrecision,
                                         const std::string &numericScale) {
  std::string pgType = "TEXT";

  if (mssqlType == "decimal" || mssqlType == "numeric") {
    // Para decimal/numeric, usar la precisión y escala de MSSQL
    if (!numericPrecision.empty() && numericPrecision != "NULL" &&
        !numericScale.empty() && numericScale != "NULL") {
      pgType = "NUMERIC(" + numericPrecision + "," + numericScale + ")";
    } else {
      pgType = "NUMERIC(18,4)";
    }
  } else if (mssqlType == "varchar" || mssqlType == "nvarchar") {
    pgType = (!maxLength.empty() && maxLength != "NULL" && maxLength != "-1")
                 ? "VARCHAR(" + maxLength + ")"
                 : "VARCHAR";
  } else if (mssqlType == "char" || mssqlType == "nchar") {
    pgType = (!maxLength.empty() && maxLength != "NULL")
                 ? "CHAR(" + maxLength + ")"
                 : "CHAR(1)";
  } else if (dataTypeMap.count(mssqlType)) {
    pgType = dataTypeMap[mssqlType];
  }

  return pgType;
}

void MSSQLTableSetup::sortTablesByPriority(std::vector<TableInfo> &tables) {
  std::sort(
      tables.begin(), tables.end(), [](const TableInfo &a, const TableInfo &b) {
        if (a.status == "FULL_LOAD" && b.status != "FULL_LOAD")
          return true;
        if (a.status != "FULL_LOAD" && b.status == "FULL_LOAD")
          return false;
        if (a.status == "LISTENING_CHANGES" && b.status != "LISTENING_CHANGES")
          return true;
        if (a.status != "LISTENING_CHANGES" && b.status == "LISTENING_CHANGES")
          return false;
        return false; // Keep original order for same priority
      });
}

std::string MSSQLTableSetup::escapeSQL(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}
