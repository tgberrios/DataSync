#include "governance/LineageExtractorMSSQL.h"
#include "core/database_config.h"
#include "core/database_defaults.h"
#include "core/logger.h"
#include "engines/mssql_engine.h"
#include "utils/connection_utils.h"
#include <algorithm>
#include <cstring>
#include <iomanip>
#include <pqxx/pqxx>
#include <sql.h>
#include <sqlext.h>
#include <sstream>

LineageExtractorMSSQL::LineageExtractorMSSQL(
    const std::string &connectionString)
    : connectionString_(connectionString) {
  serverName_ = extractServerName(connectionString);
  databaseName_ = extractDatabaseName(connectionString);
}

LineageExtractorMSSQL::~LineageExtractorMSSQL() {}

std::string
LineageExtractorMSSQL::extractServerName(const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  if (params) {
    return params->host;
  }
  return "UNKNOWN";
}

std::string LineageExtractorMSSQL::extractDatabaseName(
    const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  if (params) {
    return params->db;
  }
  return "master";
}

std::string LineageExtractorMSSQL::escapeSQL(const std::string &str) {
  std::string escaped = str;
  size_t pos = 0;
  while ((pos = escaped.find('\'', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

std::vector<std::vector<std::string>>
LineageExtractorMSSQL::executeQuery(SQLHDBC conn, const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!conn) {
    return results;
  }

  SQLHSTMT stmt;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
  if (ret != SQL_SUCCESS) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                  "SQLAllocHandle(STMT) failed");
    return results;
  }

  ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    SQLCHAR sqlState[6];
    SQLCHAR errorMsg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER nativeError;
    SQLSMALLINT msgLen;

    if (SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError,
                      errorMsg, sizeof(errorMsg), &msgLen) == SQL_SUCCESS) {
      Logger::error(
          LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
          "SQLExecDirect failed - SQLState: " + std::string((char *)sqlState) +
              ", Error: " + std::string((char *)errorMsg));
    }
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return results;
  }

  SQLSMALLINT numCols;
  SQLNumResultCols(stmt, &numCols);

  while (SQLFetch(stmt) == SQL_SUCCESS) {
    std::vector<std::string> row;
    for (SQLSMALLINT i = 1; i <= numCols; i++) {
      char buffer[DatabaseDefaults::BUFFER_SIZE];
      SQLLEN len;
      ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &len);
      if (SQL_SUCCEEDED(ret)) {
        if (len == SQL_NULL_DATA || len < 0) {
          row.push_back("");
        } else if (len > 0 && len < static_cast<SQLLEN>(sizeof(buffer))) {
          row.push_back(std::string(buffer, len));
        } else if (len >= static_cast<SQLLEN>(sizeof(buffer))) {
          std::vector<char> largeBuffer(len + 1);
          SQLLEN actualLen;
          ret = SQLGetData(stmt, i, SQL_C_CHAR, largeBuffer.data(),
                           largeBuffer.size(), &actualLen);
          if (SQL_SUCCEEDED(ret) && actualLen > 0 &&
              actualLen < static_cast<SQLLEN>(largeBuffer.size())) {
            row.push_back(std::string(largeBuffer.data(), actualLen));
          } else {
            row.push_back(std::string(buffer, sizeof(buffer) - 1));
          }
        } else {
          row.push_back("");
        }
      } else {
        row.push_back("");
      }
    }
    results.push_back(std::move(row));
  }

  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return results;
}

std::string LineageExtractorMSSQL::generateEdgeKey(const LineageEdge &edge) {
  auto escapeKeyComponent = [](const std::string &str) -> std::string {
    std::string escaped = str;
    size_t pos = 0;
    while ((pos = escaped.find('|', pos)) != std::string::npos) {
      escaped.replace(pos, 1, "||");
      pos += 2;
    }
    while ((pos = escaped.find('\n', pos)) != std::string::npos) {
      escaped.replace(pos, 1, "\\n");
      pos += 2;
    }
    while ((pos = escaped.find('\r', pos)) != std::string::npos) {
      escaped.replace(pos, 1, "\\r");
      pos += 2;
    }
    return escaped;
  };

  std::stringstream ss;
  ss << escapeKeyComponent(edge.server_name) << "|"
     << escapeKeyComponent(edge.database_name) << "|"
     << escapeKeyComponent(edge.schema_name) << "|"
     << escapeKeyComponent(edge.object_name) << "|"
     << escapeKeyComponent(edge.object_type) << "|"
     << escapeKeyComponent(edge.column_name.empty() ? "" : edge.column_name)
     << "|" << escapeKeyComponent(edge.target_object_name) << "|"
     << escapeKeyComponent(edge.target_object_type) << "|"
     << escapeKeyComponent(
            edge.target_column_name.empty() ? "" : edge.target_column_name)
     << "|" << escapeKeyComponent(edge.relationship_type);
  return ss.str();
}

void LineageExtractorMSSQL::extractLineage() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
               "Starting lineage extraction for " + serverName_ + "/" +
                   databaseName_);

  {
    std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
    lineageEdges_.clear();
  }

  try {
    ODBCConnection conn(connectionString_);
    if (!conn.isValid()) {
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                    "Failed to connect to MSSQL");
      return;
    }

    SQLHDBC hdbc = conn.getDbc();

    extractForeignKeyDependencies();
    extractTableDependencies();
    extractStoredProcedureDependencies();
    extractViewDependencies();
    extractSqlExpressionDependencies();

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                 "Lineage extraction completed. Found " +
                     std::to_string([this]() {
                       std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                       return lineageEdges_.size();
                     }()) +
                     " dependencies");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                  "Error extracting lineage: " + std::string(e.what()));
  }
}

void LineageExtractorMSSQL::extractForeignKeyDependencies() {
  try {
    ODBCConnection conn(connectionString_);
    if (!conn.isValid()) {
      return;
    }

    std::string query = R"(
      SELECT 
        OBJECT_SCHEMA_NAME(fk.parent_object_id) AS parent_schema,
        OBJECT_NAME(fk.parent_object_id) AS parent_table,
        COL_NAME(fk.parent_object_id, fkc.parent_column_id) AS parent_column,
        OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
        OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
        COL_NAME(fk.referenced_object_id, fkc.referenced_column_id) AS referenced_column,
        fk.name AS fk_name
      FROM sys.foreign_keys fk
      INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
      WHERE fk.parent_object_id IN (SELECT object_id FROM sys.tables)
        AND fk.referenced_object_id IN (SELECT object_id FROM sys.tables)
    )";

    auto results = executeQuery(conn.getDbc(), query);

    for (const auto &row : results) {
      if (row.size() >= 7) {
        LineageEdge edge;
        edge.server_name = serverName_;
        edge.database_name = databaseName_;
        edge.schema_name = row[0];
        edge.object_name = row[1];
        edge.object_type = "TABLE";
        edge.column_name = row[2];
        edge.target_object_name = row[4];
        edge.target_object_type = "TABLE";
        edge.target_column_name = row[5];
        edge.relationship_type = "FOREIGN_KEY";
        edge.definition_text = "FK: " + row[6];
        edge.dependency_level = 1;
        edge.discovery_method = "sys.foreign_keys";
        edge.confidence_score = 1.0;
        edge.edge_key = generateEdgeKey(edge);

        {
          std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
          lineageEdges_.push_back(edge);
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                 "Extracted " + std::to_string(results.size()) +
                     " foreign key dependencies");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                  "Error extracting foreign key dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorMSSQL::extractTableDependencies() {
  try {
    ODBCConnection conn(connectionString_);
    if (!conn.isValid()) {
      return;
    }

    std::string query = R"(
      SELECT DISTINCT
        OBJECT_SCHEMA_NAME(d.referencing_id) AS referencing_schema,
        OBJECT_NAME(d.referencing_id) AS referencing_object,
        OBJECT_SCHEMA_NAME(d.referenced_id) AS referenced_schema,
        OBJECT_NAME(d.referenced_id) AS referenced_object,
        NULL AS referenced_column,
        d.referencing_class_desc,
        d.referenced_class_desc
      FROM sys.sql_expression_dependencies d
      WHERE d.referencing_id IN (SELECT object_id FROM sys.tables)
        AND d.referenced_id IN (SELECT object_id FROM sys.tables)
        AND d.referencing_id != d.referenced_id
    )";

    auto results = executeQuery(conn.getDbc(), query);

    for (const auto &row : results) {
      if (row.size() >= 7) {
        LineageEdge edge;
        edge.server_name = serverName_;
        edge.database_name = databaseName_;
        edge.schema_name = row[0];
        edge.object_name = row[1];
        edge.object_type = "TABLE";
        edge.target_object_name = row[3];
        edge.target_object_type = "TABLE";
        edge.target_column_name = row[4].empty() ? "" : row[4];
        edge.relationship_type = "TABLE_DEPENDENCY";
        edge.definition_text = "Table dependency via " + row[5];
        edge.dependency_level = 1;
        edge.discovery_method = "sys.sql_expression_dependencies";
        edge.confidence_score = 0.9;
        edge.edge_key = generateEdgeKey(edge);

        {
          std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
          lineageEdges_.push_back(edge);
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                 "Extracted " + std::to_string(results.size()) +
                     " table dependencies");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                  "Error extracting table dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorMSSQL::extractStoredProcedureDependencies() {
  try {
    ODBCConnection conn(connectionString_);
    if (!conn.isValid()) {
      return;
    }

    std::string query = R"(
      SELECT 
        OBJECT_SCHEMA_NAME(sp.object_id) AS schema_name,
        OBJECT_NAME(sp.object_id) AS sp_name,
        OBJECT_SCHEMA_NAME(d.referenced_id) AS referenced_schema,
        OBJECT_NAME(d.referenced_id) AS referenced_object,
        NULL AS referenced_column,
        d.referenced_class_desc
      FROM sys.procedures sp
      INNER JOIN sys.sql_expression_dependencies d ON sp.object_id = d.referencing_id
      WHERE d.referenced_id IN (SELECT object_id FROM sys.tables)
    )";

    auto results = executeQuery(conn.getDbc(), query);

    for (const auto &row : results) {
      if (row.size() >= 6) {
        LineageEdge edge;
        edge.server_name = serverName_;
        edge.database_name = databaseName_;
        edge.schema_name = row[0];
        edge.object_name = row[1];
        edge.object_type = "STORED_PROCEDURE";
        edge.target_object_name = row[3];
        edge.target_object_type = "TABLE";
        edge.target_column_name = row[4].empty() ? "" : row[4];
        edge.relationship_type = "SP_READS_TABLE";
        edge.definition_text = "Stored procedure reads from table";
        edge.dependency_level = 2;
        edge.discovery_method = "sys.sql_expression_dependencies";
        edge.confidence_score = 0.95;
        edge.edge_key = generateEdgeKey(edge);

        {
          std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
          lineageEdges_.push_back(edge);
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                 "Extracted " + std::to_string(results.size()) +
                     " stored procedure dependencies");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                  "Error extracting stored procedure dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorMSSQL::extractViewDependencies() {
  try {
    ODBCConnection conn(connectionString_);
    if (!conn.isValid()) {
      return;
    }

    std::string query = R"(
      SELECT 
        OBJECT_SCHEMA_NAME(v.object_id) AS schema_name,
        OBJECT_NAME(v.object_id) AS view_name,
        OBJECT_SCHEMA_NAME(d.referenced_id) AS referenced_schema,
        OBJECT_NAME(d.referenced_id) AS referenced_object,
        NULL AS referenced_column,
        d.referenced_class_desc
      FROM sys.views v
      INNER JOIN sys.sql_expression_dependencies d ON v.object_id = d.referencing_id
      WHERE d.referenced_id IN (SELECT object_id FROM sys.tables)
    )";

    auto results = executeQuery(conn.getDbc(), query);

    for (const auto &row : results) {
      if (row.size() >= 6) {
        LineageEdge edge;
        edge.server_name = serverName_;
        edge.database_name = databaseName_;
        edge.schema_name = row[0];
        edge.object_name = row[1];
        edge.object_type = "VIEW";
        edge.target_object_name = row[3];
        edge.target_object_type = "TABLE";
        edge.target_column_name = row[4].empty() ? "" : row[4];
        edge.relationship_type = "VIEW_READS_TABLE";
        edge.definition_text = "View reads from table";
        edge.dependency_level = 1;
        edge.discovery_method = "sys.sql_expression_dependencies";
        edge.confidence_score = 1.0;
        edge.edge_key = generateEdgeKey(edge);

        {
          std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
          lineageEdges_.push_back(edge);
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                 "Extracted " + std::to_string(results.size()) +
                     " view dependencies");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                  "Error extracting view dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorMSSQL::extractSqlExpressionDependencies() {
  try {
    ODBCConnection conn(connectionString_);
    if (!conn.isValid()) {
      return;
    }

    std::string query = R"(
      SELECT 
        OBJECT_SCHEMA_NAME(d.referencing_id) AS referencing_schema,
        OBJECT_NAME(d.referencing_id) AS referencing_object,
        OBJECTPROPERTY(d.referencing_id, 'IsTable') AS is_table,
        OBJECTPROPERTY(d.referencing_id, 'IsView') AS is_view,
        OBJECTPROPERTY(d.referencing_id, 'IsProcedure') AS is_procedure,
        OBJECT_SCHEMA_NAME(d.referenced_id) AS referenced_schema,
        OBJECT_NAME(d.referenced_id) AS referenced_object,
        NULL AS referenced_column,
        d.referencing_class_desc,
        d.referenced_class_desc
      FROM sys.sql_expression_dependencies d
      WHERE d.referencing_id IS NOT NULL
        AND d.referenced_id IS NOT NULL
        AND d.referencing_id != d.referenced_id
    )";

    auto results = executeQuery(conn.getDbc(), query);

    for (const auto &row : results) {
      if (row.size() >= 10) {
        std::string referencingType = "UNKNOWN";
        if (row[2] == "1" || row[2] == "true" || row[2] == "TRUE") {
          referencingType = "TABLE";
        } else if (row[3] == "1" || row[3] == "true" || row[3] == "TRUE") {
          referencingType = "VIEW";
        } else if (row[4] == "1" || row[4] == "true" || row[4] == "TRUE") {
          referencingType = "STORED_PROCEDURE";
        }

        if (referencingType == "UNKNOWN") {
          continue;
        }

        LineageEdge edge;
        edge.server_name = serverName_;
        edge.database_name = databaseName_;
        edge.schema_name = row[0];
        edge.object_name = row[1];
        edge.object_type = referencingType;
        edge.target_object_name = row[6];
        edge.target_object_type = "TABLE";
        edge.target_column_name = row[7].empty() ? "" : row[7];
        edge.relationship_type = "SQL_EXPRESSION_DEPENDENCY";
        edge.definition_text = "SQL expression dependency";
        edge.dependency_level = 1;
        edge.discovery_method = "sys.sql_expression_dependencies";
        edge.confidence_score = 0.85;
        edge.edge_key = generateEdgeKey(edge);

        {
          std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
          lineageEdges_.push_back(edge);
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                 "Extracted " + std::to_string(results.size()) +
                     " SQL expression dependencies");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                  "Error extracting SQL expression dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorMSSQL::storeLineage() {
  std::vector<LineageEdge> edgesCopy;
  {
    std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
    if (lineageEdges_.empty()) {
      Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                   "No lineage data to store");
      return;
    }
    edgesCopy = lineageEdges_;
  }

  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    pqxx::connection conn(connStr);

    int stored = 0;
    for (const auto &edge : edgesCopy) {
      try {
        pqxx::work txn(conn);
        std::string query = R"(
          INSERT INTO metadata.mssql_lineage (
            edge_key, server_name, instance_name, database_name, schema_name,
            object_name, object_type, column_name, target_object_name,
            target_object_type, target_column_name, relationship_type,
            definition_text, dependency_level, discovery_method, discovered_by, confidence_score
          ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
          )
          ON CONFLICT (edge_key) DO UPDATE SET
            last_seen_at = NOW(),
            updated_at = NOW(),
            execution_count = COALESCE(EXCLUDED.execution_count, mssql_lineage.execution_count),
            avg_duration_ms = COALESCE(EXCLUDED.avg_duration_ms, mssql_lineage.avg_duration_ms),
            avg_cpu_ms = COALESCE(EXCLUDED.avg_cpu_ms, mssql_lineage.avg_cpu_ms),
            avg_logical_reads = COALESCE(EXCLUDED.avg_logical_reads, mssql_lineage.avg_logical_reads),
            avg_physical_reads = COALESCE(EXCLUDED.avg_physical_reads, mssql_lineage.avg_physical_reads)
        )";

        txn.exec_params(
            query, edge.edge_key, edge.server_name,
            edge.instance_name.empty() ? nullptr : edge.instance_name.c_str(),
            edge.database_name, edge.schema_name, edge.object_name,
            edge.object_type,
            edge.column_name.empty() ? nullptr : edge.column_name.c_str(),
            edge.target_object_name, edge.target_object_type,
            edge.target_column_name.empty() ? nullptr
                                            : edge.target_column_name.c_str(),
            edge.relationship_type,
            edge.definition_text.empty() ? nullptr
                                         : edge.definition_text.c_str(),
            edge.dependency_level, edge.discovery_method,
            "LineageExtractorMSSQL", edge.confidence_score);
        txn.commit();
        stored++;
      } catch (const std::exception &e) {
        Logger::warning(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                        "Error storing lineage edge: " + std::string(e.what()));
      }
    }
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                 "Stored " + std::to_string(stored) + " lineage edges");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
                  "Error storing lineage: " + std::string(e.what()));
  }
}
