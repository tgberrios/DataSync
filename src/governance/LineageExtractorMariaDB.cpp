#include "governance/LineageExtractorMariaDB.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "engines/mariadb_engine.h"
#include "utils/connection_utils.h"
#include <algorithm>
#include <iomanip>
#include <pqxx/pqxx>
#include <regex>
#include <set>
#include <sstream>

LineageExtractorMariaDB::LineageExtractorMariaDB(
    const std::string &connectionString)
    : connectionString_(connectionString) {
  serverName_ = extractServerName(connectionString);
  databaseName_ = extractDatabaseName(connectionString);
}

LineageExtractorMariaDB::~LineageExtractorMariaDB() {}

std::string LineageExtractorMariaDB::extractServerName(
    const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  if (params) {
    return params->host;
  }
  return "UNKNOWN";
}

std::string LineageExtractorMariaDB::extractDatabaseName(
    const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  if (params) {
    return params->db;
  }
  return "";
}

std::string LineageExtractorMariaDB::escapeSQL(MYSQL *conn,
                                               const std::string &str) {
  if (!conn || str.empty()) {
    return str;
  }
  char *escaped = new char[str.length() * 2 + 1];
  mysql_real_escape_string(conn, escaped, str.c_str(), str.length());
  std::string result(escaped);
  delete[] escaped;
  return result;
}

std::vector<std::vector<std::string>>
LineageExtractorMariaDB::executeQuery(MYSQL *conn, const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!conn) {
    return results;
  }

  if (mysql_query(conn, query.c_str())) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                  "Query failed: " + std::string(mysql_error(conn)));
    return results;
  }

  MYSQL_RES *result = mysql_store_result(conn);
  if (!result) {
    return results;
  }

  int numFields = mysql_num_fields(result);
  MYSQL_ROW row;

  while ((row = mysql_fetch_row(result))) {
    std::vector<std::string> rowData;
    unsigned long *lengths = mysql_fetch_lengths(result);
    for (int i = 0; i < numFields; i++) {
      if (row[i]) {
        rowData.push_back(std::string(row[i], lengths[i]));
      } else {
        rowData.push_back("");
      }
    }
    results.push_back(rowData);
  }

  mysql_free_result(result);
  return results;
}

std::string
LineageExtractorMariaDB::generateEdgeKey(const MariaDBLineageEdge &edge) {
  std::stringstream ss;
  ss << edge.server_name << "|" << edge.database_name << "|" << edge.schema_name
     << "|" << edge.object_name << "|" << edge.object_type << "|"
     << (edge.column_name.empty() ? "" : edge.column_name) << "|"
     << edge.target_object_name << "|" << edge.target_object_type << "|"
     << (edge.target_column_name.empty() ? "" : edge.target_column_name) << "|"
     << edge.relationship_type;
  return ss.str();
}

void LineageExtractorMariaDB::extractLineage() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
               "Starting lineage extraction for " + serverName_ + "/" +
                   databaseName_);

  lineageEdges_.clear();

  try {
    auto params = ConnectionStringParser::parse(connectionString_);
    if (!params) {
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                    "Invalid connection string");
      return;
    }

    MySQLConnection conn(*params);
    if (!conn.isValid()) {
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                    "Failed to connect to MariaDB");
      return;
    }

    MYSQL *mysqlConn = conn.get();

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Starting extraction methods for database: " + databaseName_);

    extractForeignKeyDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "After FK extraction: " + std::to_string(lineageEdges_.size()) + " edges");

    extractTableDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "After table extraction: " + std::to_string(lineageEdges_.size()) + " edges");

    extractViewDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "After view extraction: " + std::to_string(lineageEdges_.size()) + " edges");

    extractTriggerDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "After trigger extraction: " + std::to_string(lineageEdges_.size()) + " edges");

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Lineage extraction completed. Found " +
                     std::to_string(lineageEdges_.size()) + " total dependencies");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                  "Error extracting lineage: " + std::string(e.what()));
  }
}

void LineageExtractorMariaDB::extractForeignKeyDependencies() {
  try {
    auto params = ConnectionStringParser::parse(connectionString_);
    if (!params) {
      return;
    }

    MySQLConnection conn(*params);
    if (!conn.isValid()) {
      return;
    }

    MYSQL *mysqlConn = conn.get();
    std::string dbEscaped = escapeSQL(mysqlConn, databaseName_);

    std::string query = "SELECT "
                        "rc.CONSTRAINT_SCHEMA, "
                        "rc.TABLE_NAME, "
                        "kcu.COLUMN_NAME, "
                        "kcu.REFERENCED_TABLE_SCHEMA, "
                        "rc.REFERENCED_TABLE_NAME, "
                        "kcu.REFERENCED_COLUMN_NAME, "
                        "rc.CONSTRAINT_NAME "
                        "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc "
                        "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
                        "  ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA "
                        "  AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
                        "  AND rc.TABLE_NAME = kcu.TABLE_NAME "
                        "WHERE rc.CONSTRAINT_SCHEMA = '" +
                        dbEscaped +
                        "' "
                        "ORDER BY rc.TABLE_NAME, kcu.ORDINAL_POSITION";

    auto results = executeQuery(mysqlConn, query);

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Found " + std::to_string(results.size()) + " foreign key constraints");

    for (const auto &row : results) {
      if (row.size() >= 7) {
        MariaDBLineageEdge edge;
        edge.server_name = serverName_;
        edge.database_name = row[0];
        edge.schema_name = row[0];
        edge.object_name = row[1];
        edge.object_type = "TABLE";
        edge.column_name = row[2];
        std::string refSchema = row[3].empty() ? row[0] : row[3];
        edge.target_object_name = row[4];
        edge.target_object_type = "TABLE";
        edge.target_column_name = row[5];
        edge.relationship_type = "FOREIGN_KEY";
        edge.definition_text = "FK: " + row[6];
        edge.dependency_level = 1;
        edge.discovery_method = "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS";
        edge.confidence_score = 1.0;
        edge.edge_key = generateEdgeKey(edge);

        lineageEdges_.push_back(edge);
        Logger::debug(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                     "Added FK edge: " + edge.object_name + " -> " + edge.target_object_name);
      } else {
        Logger::warning(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                       "Row has insufficient columns: " + std::to_string(row.size()));
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Extracted " + std::to_string(lineageEdges_.size()) +
                     " foreign key dependencies from " + std::to_string(results.size()) + " constraints");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                  "Error extracting foreign key dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorMariaDB::extractTableDependencies() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
               "Table dependencies extraction not implemented for MariaDB");
}

void LineageExtractorMariaDB::extractViewDependencies() {
  try {
    auto params = ConnectionStringParser::parse(connectionString_);
    if (!params) {
      return;
    }

    MySQLConnection conn(*params);
    if (!conn.isValid()) {
      return;
    }

    MYSQL *mysqlConn = conn.get();
    std::string dbEscaped = escapeSQL(mysqlConn, databaseName_);

    std::string query = "SELECT "
                        "TABLE_SCHEMA, "
                        "TABLE_NAME, "
                        "VIEW_DEFINITION "
                        "FROM INFORMATION_SCHEMA.VIEWS "
                        "WHERE TABLE_SCHEMA = '" +
                        dbEscaped + "'";

    auto results = executeQuery(mysqlConn, query);

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Found " + std::to_string(results.size()) + " views to analyze");

    std::regex tableRegex(R"(\bFROM\s+[`"]?(\w+)[`"]?\.?[`"]?(\w+)[`"]?)",
                          std::regex_constants::icase);
    std::regex joinRegex(R"(\bJOIN\s+[`"]?(\w+)[`"]?\.?[`"]?(\w+)[`"]?)",
                         std::regex_constants::icase);

    int viewEdgesAdded = 0;
    for (const auto &row : results) {
      if (row.size() >= 3) {
        std::string viewSchema = row[0];
        std::string viewName = row[1];
        std::string viewDefinition = row[2];

        std::set<std::pair<std::string, std::string>> referencedTables;

        std::sregex_iterator iter(viewDefinition.begin(), viewDefinition.end(),
                                  tableRegex);
        std::sregex_iterator end;
        for (; iter != end; ++iter) {
          std::smatch match = *iter;
          std::string schema = match[1].str();
          std::string table = match[2].str();
          if (schema == databaseName_ || schema.empty()) {
            referencedTables.insert({databaseName_, table});
          } else {
            referencedTables.insert({schema, table});
          }
        }

        std::sregex_iterator joinIter(viewDefinition.begin(),
                                      viewDefinition.end(), joinRegex);
        for (; joinIter != end; ++joinIter) {
          std::smatch match = *joinIter;
          std::string schema = match[1].str();
          std::string table = match[2].str();
          if (schema == databaseName_ || schema.empty()) {
            referencedTables.insert({databaseName_, table});
          } else {
            referencedTables.insert({schema, table});
          }
        }

        for (const auto &refTable : referencedTables) {
          MariaDBLineageEdge edge;
          edge.server_name = serverName_;
          edge.database_name = viewSchema;
          edge.schema_name = viewSchema;
          edge.object_name = viewName;
          edge.object_type = "VIEW";
          edge.target_object_name = refTable.second;
          edge.target_object_type = "TABLE";
          edge.relationship_type = "VIEW_READS_TABLE";
          edge.definition_text = "View reads from table";
          edge.dependency_level = 1;
          edge.discovery_method = "INFORMATION_SCHEMA.VIEWS";
          edge.confidence_score = 0.9;
          edge.edge_key = generateEdgeKey(edge);

          lineageEdges_.push_back(edge);
          viewEdgesAdded++;
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Extracted " + std::to_string(viewEdgesAdded) +
                     " view dependencies from " + std::to_string(results.size()) + " views");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                  "Error extracting view dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorMariaDB::extractTriggerDependencies() {
  try {
    auto params = ConnectionStringParser::parse(connectionString_);
    if (!params) {
      return;
    }

    MySQLConnection conn(*params);
    if (!conn.isValid()) {
      return;
    }

    MYSQL *mysqlConn = conn.get();
    std::string dbEscaped = escapeSQL(mysqlConn, databaseName_);

    std::string query = "SELECT "
                        "TRIGGER_SCHEMA, "
                        "TRIGGER_NAME, "
                        "EVENT_MANIPULATION, "
                        "EVENT_OBJECT_TABLE, "
                        "ACTION_STATEMENT "
                        "FROM INFORMATION_SCHEMA.TRIGGERS "
                        "WHERE TRIGGER_SCHEMA = '" +
                        dbEscaped + "'";

    auto results = executeQuery(mysqlConn, query);

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Found " + std::to_string(results.size()) + " triggers to analyze");

    std::regex tableRegex(R"(\bFROM\s+[`"]?(\w+)[`"]?\.?[`"]?(\w+)[`"]?)",
                          std::regex_constants::icase);
    std::regex updateRegex(R"(\bUPDATE\s+[`"]?(\w+)[`"]?\.?[`"]?(\w+)[`"]?)",
                           std::regex_constants::icase);
    std::regex insertRegex(
        R"(\bINSERT\s+INTO\s+[`"]?(\w+)[`"]?\.?[`"]?(\w+)[`"]?)",
        std::regex_constants::icase);

    int triggerEdgesAdded = 0;
    for (const auto &row : results) {
      if (row.size() >= 5) {
        std::string triggerSchema = row[0];
        std::string triggerName = row[1];
        std::string eventTable = row[3];
        std::string actionStatement = row[4];

        std::set<std::pair<std::string, std::string>> referencedTables;

        std::sregex_iterator fromIter(actionStatement.begin(),
                                      actionStatement.end(), tableRegex);
        std::sregex_iterator end;
        for (; fromIter != end; ++fromIter) {
          std::smatch match = *fromIter;
          std::string schema = match[1].str();
          std::string table = match[2].str();
          if (schema == databaseName_ || schema.empty()) {
            referencedTables.insert({databaseName_, table});
          } else {
            referencedTables.insert({schema, table});
          }
        }

        std::sregex_iterator updateIter(actionStatement.begin(),
                                        actionStatement.end(), updateRegex);
        for (; updateIter != end; ++updateIter) {
          std::smatch match = *updateIter;
          std::string schema = match[1].str();
          std::string table = match[2].str();
          if (schema == databaseName_ || schema.empty()) {
            referencedTables.insert({databaseName_, table});
          } else {
            referencedTables.insert({schema, table});
          }
        }

        std::sregex_iterator insertIter(actionStatement.begin(),
                                        actionStatement.end(), insertRegex);
        for (; insertIter != end; ++insertIter) {
          std::smatch match = *insertIter;
          std::string schema = match[1].str();
          std::string table = match[2].str();
          if (schema == databaseName_ || schema.empty()) {
            referencedTables.insert({databaseName_, table});
          } else {
            referencedTables.insert({schema, table});
          }
        }

        for (const auto &refTable : referencedTables) {
          MariaDBLineageEdge edge;
          edge.server_name = serverName_;
          edge.database_name = triggerSchema;
          edge.schema_name = triggerSchema;
          edge.object_name = triggerName;
          edge.object_type = "TRIGGER";
          edge.target_object_name = refTable.second;
          edge.target_object_type = "TABLE";
          edge.relationship_type = "TRIGGER_READS_TABLE";
          edge.definition_text =
              "Trigger on " + eventTable + " references table";
          edge.dependency_level = 2;
          edge.discovery_method = "INFORMATION_SCHEMA.TRIGGERS";
          edge.confidence_score = 0.85;
          edge.edge_key = generateEdgeKey(edge);

          lineageEdges_.push_back(edge);
        }

        if (!eventTable.empty()) {
          MariaDBLineageEdge edge;
          edge.server_name = serverName_;
          edge.database_name = triggerSchema;
          edge.schema_name = triggerSchema;
          edge.object_name = triggerName;
          edge.object_type = "TRIGGER";
          edge.target_object_name = eventTable;
          edge.target_object_type = "TABLE";
          edge.relationship_type = "TRIGGER_ON_TABLE";
          edge.definition_text = "Trigger on table " + eventTable;
          edge.dependency_level = 1;
          edge.discovery_method = "INFORMATION_SCHEMA.TRIGGERS";
          edge.confidence_score = 1.0;
          edge.edge_key = generateEdgeKey(edge);

          lineageEdges_.push_back(edge);
          triggerEdgesAdded++;
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Extracted " + std::to_string(triggerEdgesAdded) +
                     " trigger dependencies from " + std::to_string(results.size()) + " triggers");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                  "Error extracting trigger dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorMariaDB::storeLineage() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
               "storeLineage called with " + std::to_string(lineageEdges_.size()) + " edges");

  if (lineageEdges_.empty()) {
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "No lineage data to store");
    return;
  }

  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    pqxx::connection conn(connStr);

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Connected to PostgreSQL, starting to store " + std::to_string(lineageEdges_.size()) + " edges");

    int stored = 0;
    int failed = 0;
    for (const auto &edge : lineageEdges_) {
      try {
        pqxx::work txn(conn);
        std::string query = R"(
          INSERT INTO metadata.mdb_lineage (
            edge_key, server_name, database_name, schema_name,
            object_name, object_type, column_name, target_object_name,
            target_object_type, target_column_name, relationship_type,
            definition_text, dependency_level, discovery_method, discovered_by, confidence_score
          ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
          )
          ON CONFLICT (edge_key) DO UPDATE SET
            last_seen_at = NOW(),
            updated_at = NOW()
        )";

        txn.exec_params(
            query, edge.edge_key, edge.server_name,
            edge.database_name.empty() ? nullptr : edge.database_name.c_str(),
            edge.schema_name.empty() ? nullptr : edge.schema_name.c_str(),
            edge.object_name, edge.object_type,
            edge.column_name.empty() ? nullptr : edge.column_name.c_str(),
            edge.target_object_name.empty() ? nullptr
                                            : edge.target_object_name.c_str(),
            edge.target_object_type.empty() ? nullptr
                                            : edge.target_object_type.c_str(),
            edge.target_column_name.empty() ? nullptr
                                            : edge.target_column_name.c_str(),
            edge.relationship_type,
            edge.definition_text.empty() ? nullptr
                                         : edge.definition_text.c_str(),
            edge.dependency_level, edge.discovery_method,
            "LineageExtractorMariaDB", edge.confidence_score);
        txn.commit();
        stored++;
      } catch (const std::exception &e) {
        failed++;
        Logger::warning(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                        "Error storing lineage edge " + edge.edge_key + ": " + std::string(e.what()));
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Stored " + std::to_string(stored) + " lineage edges, " + std::to_string(failed) + " failed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                  "Error storing lineage: " + std::string(e.what()));
  }
}
