#include "governance/LineageExtractorMariaDB.h"
#include "catalog/metadata_repository.h"
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
  std::vector<char> escaped(str.length() * 2 + 1);
  unsigned long len =
      mysql_real_escape_string(conn, escaped.data(), str.c_str(), str.length());
  if (len == (unsigned long)-1) {
    return str;
  }
  return std::string(escaped.data(), len);
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
  auto escapeKeyComponent = [](const std::string &str) -> std::string {
    // Handle empty strings
    if (str.empty()) {
      return "";
    }
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
  ss << escapeKeyComponent(edge.server_name.empty() ? "" : edge.server_name) << "|"
     << escapeKeyComponent(edge.database_name.empty() ? "" : edge.database_name) << "|"
     << escapeKeyComponent(edge.schema_name.empty() ? "" : edge.schema_name) << "|"
     << escapeKeyComponent(edge.object_name.empty() ? "" : edge.object_name) << "|"
     << escapeKeyComponent(edge.object_type.empty() ? "" : edge.object_type) << "|"
     << escapeKeyComponent(edge.column_name.empty() ? "" : edge.column_name)
     << "|" << escapeKeyComponent(edge.target_object_name.empty() ? "" : edge.target_object_name) << "|"
     << escapeKeyComponent(edge.target_object_type.empty() ? "" : edge.target_object_type) << "|"
     << escapeKeyComponent(
            edge.target_column_name.empty() ? "" : edge.target_column_name)
     << "|" << escapeKeyComponent(edge.relationship_type.empty() ? "" : edge.relationship_type);
  return ss.str();
}

void LineageExtractorMariaDB::extractLineage() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
               "Starting lineage extraction for " + serverName_ + "/" +
                   databaseName_);

  {
    std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
    lineageEdges_.clear();
  }

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
                 "After FK extraction: " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " edges");

    extractTableDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "After table extraction: " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " edges");

    extractViewDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "After view extraction: " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " edges");

    extractTriggerDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "After trigger extraction: " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " edges");

    extractDatalakeRelationships();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "After datalake extraction: " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " edges");

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Lineage extraction completed. Found " +
                     std::to_string(lineageEdges_.size()) +
                     " total dependencies");
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
                 "Found " + std::to_string(results.size()) +
                     " foreign key constraints");

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

        {
          std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
          lineageEdges_.push_back(edge);
        }
        Logger::debug(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                      "Added FK edge: " + edge.object_name + " -> " +
                          edge.target_object_name);
      } else {
        Logger::warning(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                        "Row has insufficient columns: " +
                            std::to_string(row.size()));
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Extracted " + std::to_string(lineageEdges_.size()) +
                     " foreign key dependencies from " +
                     std::to_string(results.size()) + " constraints");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                  "Error extracting foreign key dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorMariaDB::extractTableDependencies() {
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
                        "ROUTINE_SCHEMA, "
                        "ROUTINE_NAME, "
                        "ROUTINE_DEFINITION "
                        "FROM INFORMATION_SCHEMA.ROUTINES "
                        "WHERE ROUTINE_SCHEMA = '" +
                        dbEscaped +
                        "' "
                        "AND ROUTINE_TYPE IN ('PROCEDURE', 'FUNCTION')";

    auto results = executeQuery(mysqlConn, query);

    std::regex tableRegex(
        R"(\bFROM\s+[`"]?(\w+)[`"]?\.?[`"]?(\w+)[`"]?|\bJOIN\s+[`"]?(\w+)[`"]?\.?[`"]?(\w+)[`"]?|\bINTO\s+[`"]?(\w+)[`"]?\.?[`"]?(\w+)[`"]?|\bUPDATE\s+[`"]?(\w+)[`"]?\.?[`"]?(\w+)[`"]?)",
        std::regex_constants::icase);

    int dependenciesFound = 0;
    for (const auto &row : results) {
      if (row.size() >= 3) {
        std::string routineSchema = row[0];
        std::string routineName = row[1];
        std::string routineDefinition = row[2];

        std::set<std::pair<std::string, std::string>> referencedTables;

        std::sregex_iterator iter(routineDefinition.begin(),
                                  routineDefinition.end(), tableRegex);
        std::sregex_iterator end;
        for (; iter != end; ++iter) {
          std::smatch match = *iter;
          std::string schema;
          std::string table;

          for (size_t i = 1; i < match.size(); i += 2) {
            if (match[i].matched && !match[i].str().empty()) {
              schema = match[i].str();
              if (i + 1 < match.size() && match[i + 1].matched &&
                  !match[i + 1].str().empty()) {
                table = match[i + 1].str();
              }
            } else if (i + 1 < match.size() && match[i + 1].matched &&
                       !match[i + 1].str().empty()) {
              table = match[i + 1].str();
              schema = databaseName_;
            }

            if (!table.empty()) {
              if (schema.empty() || schema == databaseName_) {
                referencedTables.insert({databaseName_, table});
              } else {
                referencedTables.insert({schema, table});
              }
              break;
            }
          }
        }

        for (const auto &refTable : referencedTables) {
          MariaDBLineageEdge edge;
          edge.server_name = serverName_;
          edge.database_name = routineSchema;
          edge.schema_name = routineSchema;
          edge.object_name = routineName;
          edge.object_type = "ROUTINE";
          edge.target_object_name = refTable.second;
          edge.target_object_type = "TABLE";
          edge.relationship_type = "ROUTINE_REFERENCES_TABLE";
          edge.definition_text = "Routine references table";
          edge.dependency_level = 2;
          edge.discovery_method = "INFORMATION_SCHEMA.ROUTINES";
          edge.confidence_score = 0.8;
          edge.edge_key = generateEdgeKey(edge);

          {
            std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
            lineageEdges_.push_back(edge);
          }
          dependenciesFound++;
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Extracted " + std::to_string(dependenciesFound) +
                     " table dependencies from routines");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                  "Error extracting table dependencies: " +
                      std::string(e.what()));
  }
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
                 "Found " + std::to_string(results.size()) +
                     " views to analyze");

    std::regex tableRegex(
        R"(\bFROM\s+(?:`?(\w+)`?\.)?`?(\w+)`?|\bJOIN\s+(?:`?(\w+)`?\.)?`?(\w+)`?|\bINTO\s+(?:`?(\w+)`?\.)?`?(\w+)`?|\bUPDATE\s+(?:`?(\w+)`?\.)?`?(\w+)`?)",
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
          std::string schema;
          std::string table;

          for (size_t i = 1; i < match.size(); i += 2) {
            if (match[i].matched && !match[i].str().empty()) {
              schema = match[i].str();
              if (i + 1 < match.size() && match[i + 1].matched &&
                  !match[i + 1].str().empty()) {
                table = match[i + 1].str();
              }
            } else if (i + 1 < match.size() && match[i + 1].matched &&
                       !match[i + 1].str().empty()) {
              table = match[i + 1].str();
              schema = databaseName_;
            }

            if (!table.empty()) {
              if (schema.empty() || schema == databaseName_) {
                referencedTables.insert({databaseName_, table});
              } else {
                referencedTables.insert({schema, table});
              }
              break;
            }
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

          {
            std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
            lineageEdges_.push_back(edge);
          }
          viewEdgesAdded++;
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Extracted " + std::to_string(viewEdgesAdded) +
                     " view dependencies from " +
                     std::to_string(results.size()) + " views");
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
                 "Found " + std::to_string(results.size()) +
                     " triggers to analyze");

    int triggerEdgesAdded = 0;
    for (const auto &row : results) {
      if (row.size() >= 5) {
        std::string triggerSchema = row[0];
        std::string triggerName = row[1];
        std::string eventTable = row[3];
        std::string actionStatement = row[4];

        std::set<std::pair<std::string, std::string>> referencedTables =
            extractReferencedTablesFromStatement(actionStatement);
        addTriggerEdge(triggerSchema, triggerName, eventTable,
                       referencedTables);
        triggerEdgesAdded++;
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Extracted " + std::to_string(triggerEdgesAdded) +
                     " trigger dependencies from " +
                     std::to_string(results.size()) + " triggers");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                  "Error extracting trigger dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorMariaDB::storeLineage() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
               "storeLineage called with " +
                   std::to_string(lineageEdges_.size()) + " edges");

  std::vector<MariaDBLineageEdge> edgesCopy;
  {
    std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
    if (lineageEdges_.empty()) {
      Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                   "No lineage data to store");
      return;
    }
    edgesCopy = lineageEdges_;
  }

  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    pqxx::connection conn(connStr);

    pqxx::work createTxn(conn);
    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS metadata.mdb_lineage ("
        "id SERIAL PRIMARY KEY,"
        "edge_key VARCHAR(500) UNIQUE NOT NULL,"
        "server_name VARCHAR(200) NOT NULL,"
        "database_name VARCHAR(100) NOT NULL,"
        "schema_name VARCHAR(100) NOT NULL,"
        "object_name VARCHAR(100) NOT NULL,"
        "object_type VARCHAR(50) NOT NULL,"
        "column_name VARCHAR(100),"
        "target_object_name VARCHAR(100),"
        "target_object_type VARCHAR(50),"
        "target_column_name VARCHAR(100),"
        "relationship_type VARCHAR(50) NOT NULL,"
        "definition_text TEXT,"
        "dependency_level INTEGER DEFAULT 1,"
        "discovery_method VARCHAR(50),"
        "discovered_by VARCHAR(100),"
        "confidence_score DECIMAL(3,2) DEFAULT 1.0,"
        "first_seen_at TIMESTAMP DEFAULT NOW(),"
        "last_seen_at TIMESTAMP DEFAULT NOW(),"
        "created_at TIMESTAMP DEFAULT NOW(),"
        "updated_at TIMESTAMP DEFAULT NOW()"
        ");";
    createTxn.exec(createTableSQL);
    createTxn.exec("ALTER TABLE metadata.mdb_lineage ADD COLUMN IF NOT EXISTS "
                   "dependency_level INTEGER DEFAULT 1;");
    createTxn.commit();

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Connected to PostgreSQL, starting to store " +
                     std::to_string(edgesCopy.size()) + " edges");

    int stored = 0;
    int failed = 0;
    for (const auto &edge : edgesCopy) {
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

        pqxx::params params;
        params.append(edge.edge_key);
        params.append(edge.server_name);
        params.append(edge.database_name.empty() ? nullptr : edge.database_name);
        params.append(edge.schema_name.empty() ? nullptr : edge.schema_name);
        params.append(edge.object_name);
        params.append(edge.object_type);
        params.append(edge.column_name.empty() ? nullptr : edge.column_name);
        params.append(edge.target_object_name.empty() ? nullptr : edge.target_object_name);
        params.append(edge.target_object_type.empty() ? nullptr : edge.target_object_type);
        params.append(edge.target_column_name.empty() ? nullptr : edge.target_column_name);
        params.append(edge.relationship_type);
        params.append(edge.definition_text.empty() ? nullptr : edge.definition_text);
        params.append(edge.dependency_level);
        params.append(edge.discovery_method);
        params.append("LineageExtractorMariaDB");
        params.append(edge.confidence_score);
        
        txn.exec(pqxx::zview(query), params);
        txn.commit();
        stored++;
      } catch (const std::exception &e) {
        failed++;
        Logger::warning(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                        "Error storing lineage edge " + edge.edge_key + ": " +
                            std::string(e.what()));
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Stored " + std::to_string(stored) + " lineage edges, " +
                     std::to_string(failed) + " failed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                  "Error storing lineage: " + std::string(e.what()));
  }
}

std::set<std::pair<std::string, std::string>>
LineageExtractorMariaDB::extractReferencedTablesFromStatement(
    const std::string &actionStatement) {
  std::set<std::pair<std::string, std::string>> referencedTables;

  std::regex tableRegex(R"(\bFROM\s+[`"]?(\w+)[`"]?\.?[`"]?(\w+)[`"]?)",
                        std::regex_constants::icase);
  std::regex updateRegex(R"(\bUPDATE\s+[`"]?(\w+)[`"]?\.?[`"]?(\w+)[`"]?)",
                         std::regex_constants::icase);
  std::regex insertRegex(
      R"(\bINSERT\s+INTO\s+[`"]?(\w+)[`"]?\.?[`"]?(\w+)[`"]?)",
      std::regex_constants::icase);

  auto extractMatches = [&](const std::regex &regex) {
    std::sregex_iterator iter(actionStatement.begin(), actionStatement.end(),
                              regex);
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
  };

  extractMatches(tableRegex);
  extractMatches(updateRegex);
  extractMatches(insertRegex);

  return referencedTables;
}

void LineageExtractorMariaDB::addTriggerEdge(
    const std::string &triggerSchema, const std::string &triggerName,
    const std::string &eventTable,
    const std::set<std::pair<std::string, std::string>> &referencedTables) {
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
    edge.definition_text = "Trigger on " + eventTable + " references table";
    edge.dependency_level = 2;
    edge.discovery_method = "INFORMATION_SCHEMA.TRIGGERS";
    edge.confidence_score = 0.85;
    edge.edge_key = generateEdgeKey(edge);

    {
      std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
      lineageEdges_.push_back(edge);
    }
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

    {
      std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
      lineageEdges_.push_back(edge);
    }
  }
}

void LineageExtractorMariaDB::extractDatalakeRelationships() {
  try {
    // Connect to metadata database to query catalog
    std::string metadataConnStr = DatabaseConfig::getPostgresConnectionString();
    pqxx::connection metadataConn(metadataConnStr);
    if (!metadataConn.is_open()) {
      Logger::warning(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                      "Failed to connect to metadata database for datalake relationships");
      return;
    }

    // Parse connection string of this extractor to get host/port/database for comparison
    auto sourceParams = ConnectionStringParser::parse(connectionString_);
    if (!sourceParams) {
      Logger::warning(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                      "Failed to parse connection string for datalake relationships");
      return;
    }

    // Query catalog to find all active MariaDB tables (without filtering by connection_string)
    pqxx::work txn(metadataConn);
    std::string query = R"(
      SELECT DISTINCT
        schema_name,
        table_name,
        connection_string as source_connection_string
      FROM metadata.catalog
      WHERE db_engine = 'MariaDB'
        AND active = true
    )";

    auto catalogResults = txn.exec(pqxx::zview(query));
    txn.commit();

    int matchedCount = 0;
    // For each source table in catalog, check if it matches this source and create relationship
    for (const auto &catalogRow : catalogResults) {
      // Handle NULLs correctly
      if (catalogRow[0].is_null() || catalogRow[1].is_null()) {
        continue;
      }

      std::string sourceSchema = catalogRow[0].as<std::string>();
      std::string sourceTable = catalogRow[1].as<std::string>();
      
      // Parse connection string from catalog to compare by host/port/database
      std::string catalogConnStr;
      if (!catalogRow[2].is_null()) {
        catalogConnStr = catalogRow[2].as<std::string>();
      } else {
        continue;  // Skip if connection_string is NULL
      }

      auto catalogParams = ConnectionStringParser::parse(catalogConnStr);
      if (!catalogParams) {
        continue;  // Skip if we can't parse the connection string
      }

      // Compare by host, port, and database (more flexible than exact string match)
      bool matches = (sourceParams->host == catalogParams->host) &&
                     (sourceParams->port == catalogParams->port) &&
                     (sourceParams->db == catalogParams->db);

      if (!matches) {
        continue;  // Skip tables from different sources
      }
      
      // The target in datalake typically uses the same schema/table name
      // or could be in a specific datalake schema
      std::string targetSchema = sourceSchema;  // Could be 'datalake' or same as source
      std::string targetTable = sourceTable;

      MariaDBLineageEdge edge;
      edge.server_name = serverName_.empty() ? "UNKNOWN" : serverName_;
      edge.database_name = databaseName_.empty() ? "" : databaseName_;
      edge.schema_name = sourceSchema.empty() ? "" : sourceSchema;
      edge.object_name = sourceTable.empty() ? "" : sourceTable;
      edge.object_type = "TABLE";
      edge.target_object_name = targetTable.empty() ? "" : targetTable;
      edge.target_object_type = "TABLE";
      edge.relationship_type = "SYNCED_TO_DATALAKE";
      edge.definition_text = "Table synced from MariaDB source to DataLake PostgreSQL";
      edge.dependency_level = 0;  // Direct sync relationship
      edge.discovery_method = "METADATA_CATALOG";
      edge.confidence_score = 1.0;  // High confidence - comes from catalog
      edge.consumer_type = "DATALAKE";
      edge.consumer_name = "DataLake";
      edge.edge_key = generateEdgeKey(edge);

      {
        std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
        lineageEdges_.push_back(edge);
      }

      matchedCount++;
      Logger::debug(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                    "Added datalake edge: " + sourceSchema + "." + sourceTable +
                        " -> DataLake." + targetSchema + "." + targetTable);
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                 "Extracted " + std::to_string(matchedCount) +
                     " datalake relationships from catalog (matched from " +
                     std::to_string(catalogResults.size()) + " total MariaDB tables)");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
                  "Error extracting datalake relationships: " +
                      std::string(e.what()));
  }
}

void LineageExtractorMariaDB::addDatalakeEdge(
    const std::string &sourceSchema, const std::string &sourceTable,
    const std::string &/*targetSchema*/, const std::string &targetTable,
    const std::string &/*targetConnectionString*/) {
  MariaDBLineageEdge edge;
  edge.server_name = serverName_;
  edge.database_name = databaseName_;
  edge.schema_name = sourceSchema;
  edge.object_name = sourceTable;
  edge.object_type = "TABLE";
  edge.target_object_name = targetTable;
  edge.target_object_type = "TABLE";
  edge.relationship_type = "SYNCED_TO_DATALAKE";
  edge.definition_text = "Table synced from MariaDB to DataLake";
  edge.dependency_level = 0;
  edge.discovery_method = "METADATA_CATALOG";
  edge.confidence_score = 1.0;
  edge.consumer_type = "DATALAKE";
  edge.consumer_name = "DataLake";
  edge.edge_key = generateEdgeKey(edge);

  {
    std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
    lineageEdges_.push_back(edge);
  }
}
