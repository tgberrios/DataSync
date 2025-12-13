#include "governance/LineageExtractorOracle.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "engines/oracle_engine.h"
#include "utils/connection_utils.h"
#include <algorithm>
#include <iomanip>
#include <pqxx/pqxx>
#include <regex>
#include <set>
#include <sstream>

LineageExtractorOracle::LineageExtractorOracle(
    const std::string &connectionString)
    : connectionString_(connectionString) {
  serverName_ = extractServerName(connectionString);
  schemaName_ = extractSchemaName(connectionString);
}

LineageExtractorOracle::~LineageExtractorOracle() {}

std::string
LineageExtractorOracle::extractServerName(const std::string &connectionString) {
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);
    if (key == "host" || key == "HOST") {
      return value;
    }
  }
  return "UNKNOWN";
}

std::string
LineageExtractorOracle::extractSchemaName(const std::string &connectionString) {
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);
    if (key == "user" || key == "USER") {
      return value;
    }
  }
  return "";
}

std::string LineageExtractorOracle::escapeSQL(const std::string &str) {
  std::string escaped = str;
  size_t pos = 0;
  while ((pos = escaped.find('\'', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

std::vector<std::vector<std::string>>
LineageExtractorOracle::executeQuery(OCIConnection *conn,
                                     const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!conn || !conn->isValid()) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                  "Invalid Oracle connection");
    return results;
  }

  OCIStmt *stmt = nullptr;
  OCIError *err = conn->getErr();
  OCISvcCtx *svc = conn->getSvc();
  OCIEnv *env = conn->getEnv();
  sword status =
      OCIHandleAlloc((dvoid *)env, (dvoid **)&stmt, OCI_HTYPE_STMT, 0, nullptr);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                  "OCIHandleAlloc(STMT) failed");
    return results;
  }

  status = OCIStmtPrepare(stmt, err, (OraText *)query.c_str(), query.length(),
                          OCI_NTV_SYNTAX, OCI_DEFAULT);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                  "OCIStmtPrepare failed for query: " + query);
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    return results;
  }

  status = OCIStmtExecute(svc, stmt, err, 0, 0, nullptr, nullptr, OCI_DEFAULT);
  if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
    char errbuf[512];
    sb4 errcode = 0;
    OCIErrorGet(err, 1, nullptr, &errcode, (OraText *)errbuf, sizeof(errbuf),
                OCI_HTYPE_ERROR);
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                  "OCIStmtExecute failed for query: " + query +
                      " - Error: " + std::string(errbuf) +
                      " (code: " + std::to_string(errcode) + ")");
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    return results;
  }

  ub4 numCols = 0;
  status = OCIAttrGet(stmt, OCI_HTYPE_STMT, &numCols, nullptr,
                      OCI_ATTR_PARAM_COUNT, err);
  if (status != OCI_SUCCESS || numCols == 0) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                  "OCIAttrGet(PARAM_COUNT) failed or no columns");
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    return results;
  }

  constexpr ub4 MAX_COLUMN_SIZE = 32768;
  std::vector<OCIDefine *> defines(numCols);
  std::vector<std::vector<char>> buffers(numCols);
  std::vector<ub2> lengths(numCols);
  std::vector<sb2> inds(numCols);

  for (ub4 i = 0; i < numCols; ++i) {
    buffers[i].resize(MAX_COLUMN_SIZE);
    status = OCIDefineByPos(stmt, &defines[i], err, i + 1, buffers[i].data(),
                            MAX_COLUMN_SIZE, SQLT_STR, &inds[i], &lengths[i],
                            nullptr, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                    "OCIDefineByPos failed for column " +
                        std::to_string(i + 1));
      OCIHandleFree(stmt, OCI_HTYPE_STMT);
      return results;
    }
  }

  sword fetchStatus;
  while ((fetchStatus = OCIStmtFetch(stmt, err, 1, OCI_FETCH_NEXT,
                                     OCI_DEFAULT)) == OCI_SUCCESS ||
         fetchStatus == OCI_SUCCESS_WITH_INFO) {
    std::vector<std::string> row;
    for (ub4 i = 0; i < numCols; ++i) {
      if (inds[i] == -1 || inds[i] == OCI_IND_NULL) {
        row.push_back("");
      } else if (lengths[i] > 0) {
        ub4 copyLen =
            (lengths[i] < MAX_COLUMN_SIZE) ? lengths[i] : MAX_COLUMN_SIZE;
        std::string cellValue(buffers[i].data(), copyLen);
        if (lengths[i] >= MAX_COLUMN_SIZE) {
          cellValue += "...(truncated)";
        }
        row.push_back(cellValue);
      } else {
        row.push_back("");
      }
    }
    results.push_back(row);
  }

  if (fetchStatus != OCI_NO_DATA && fetchStatus != OCI_SUCCESS) {
    char errbuf[512];
    sb4 errcode = 0;
    OCIErrorGet(err, 1, nullptr, &errcode, (OraText *)errbuf, sizeof(errbuf),
                OCI_HTYPE_ERROR);
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                  "OCIStmtFetch failed: " + std::string(errbuf) +
                      " (code: " + std::to_string(errcode) + ")");
  }

  OCIHandleFree(stmt, OCI_HTYPE_STMT);
  return results;
}

std::string
LineageExtractorOracle::generateEdgeKey(const OracleLineageEdge &edge) {
  std::stringstream ss;
  ss << edge.server_name << "|" << edge.schema_name << "|" << edge.object_name
     << "|" << edge.object_type << "|"
     << (edge.column_name.empty() ? "" : edge.column_name) << "|"
     << edge.target_object_name << "|" << edge.target_object_type << "|"
     << (edge.target_column_name.empty() ? "" : edge.target_column_name) << "|"
     << edge.relationship_type;
  return ss.str();
}

void LineageExtractorOracle::extractLineage() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
               "Starting lineage extraction for " + serverName_ + "/" +
                   schemaName_);

  {
    std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
    lineageEdges_.clear();
  }

  try {
    auto conn = std::make_unique<OCIConnection>(connectionString_);
    if (!conn || !conn->isValid()) {
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                    "Failed to connect to Oracle");
      return;
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "Starting extraction methods for schema: " + schemaName_);

    extractForeignKeyDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "After FK extraction: " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " edges");

    extractViewDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "After view extraction: " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " edges");

    extractTriggerDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "After trigger extraction: " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " edges");

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "Lineage extraction completed. Found " +
                     std::to_string([this]() {
                       std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                       return lineageEdges_.size();
                     }()) +
                     " total dependencies");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                  "Error extracting lineage: " + std::string(e.what()));
  }
}

void LineageExtractorOracle::extractForeignKeyDependencies() {
  try {
    auto conn = std::make_unique<OCIConnection>(connectionString_);
    if (!conn || !conn->isValid()) {
      return;
    }

    std::string upperSchema = schemaName_;
    std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                   ::toupper);
    std::string escapedSchema = escapeSQL(upperSchema);

    std::string query =
        "SELECT "
        "uc.owner, "
        "uc.table_name, "
        "ucc.column_name, "
        "uc.r_owner, "
        "rc.table_name AS r_table_name, "
        "ucc_r.column_name AS r_column_name, "
        "uc.constraint_name "
        "FROM all_constraints uc "
        "INNER JOIN all_cons_columns ucc ON uc.owner = ucc.owner AND "
        "uc.constraint_name = ucc.constraint_name AND uc.table_name = "
        "ucc.table_name "
        "LEFT JOIN all_constraints rc ON uc.r_owner = rc.owner AND "
        "uc.r_constraint_name = rc.constraint_name "
        "LEFT JOIN all_cons_columns ucc_r ON rc.owner = ucc_r.owner AND "
        "rc.constraint_name = ucc_r.constraint_name AND rc.table_name = "
        "ucc_r.table_name "
        "WHERE uc.constraint_type = 'R' "
        "AND uc.owner = '" +
        escapedSchema +
        "' "
        "ORDER BY uc.table_name, ucc.position";

    auto results = executeQuery(conn.get(), query);

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "Found " + std::to_string(results.size()) +
                     " foreign key constraints");

    for (const auto &row : results) {
      if (row.size() >= 7) {
        OracleLineageEdge edge;
        edge.server_name = serverName_;
        edge.schema_name = row[0];
        edge.database_name = row[0];
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
        edge.discovery_method = "ALL_CONSTRAINTS";
        edge.confidence_score = 1.0;
        edge.edge_key = generateEdgeKey(edge);

        {
          std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
          lineageEdges_.push_back(edge);
        }
        Logger::debug(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                      "Added FK edge: " + edge.object_name + " -> " +
                          edge.target_object_name);
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "Extracted " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " foreign key dependencies from " +
                     std::to_string(results.size()) + " constraints");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                  "Error extracting foreign key dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorOracle::extractTableDependencies() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
               "Table dependencies extraction not implemented for Oracle");
}

void LineageExtractorOracle::extractViewDependencies() {
  try {
    auto conn = std::make_unique<OCIConnection>(connectionString_);
    if (!conn || !conn->isValid()) {
      return;
    }

    std::string upperSchema = schemaName_;
    std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                   ::toupper);
    std::string escapedSchema = escapeSQL(upperSchema);

    std::string query = "SELECT "
                        "owner, "
                        "view_name, "
                        "text "
                        "FROM all_views "
                        "WHERE owner = '" +
                        escapedSchema + "'";

    auto results = executeQuery(conn.get(), query);

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "Found " + std::to_string(results.size()) +
                     " views to analyze");

    std::regex tableRegex(R"(\bFROM\s+["]?(\w+)["]?\.?["]?(\w+)["]?)",
                          std::regex_constants::icase);
    std::regex joinRegex(R"(\bJOIN\s+["]?(\w+)["]?\.?["]?(\w+)["]?)",
                         std::regex_constants::icase);

    int viewEdgesAdded = 0;
    for (const auto &row : results) {
      if (row.size() >= 3) {
        std::string viewSchema = row[0];
        std::string viewName = row[1];
        std::string viewText = row[2];

        std::set<std::pair<std::string, std::string>> referencedTables;

        std::sregex_iterator iter(viewText.begin(), viewText.end(), tableRegex);
        std::sregex_iterator end;
        for (; iter != end; ++iter) {
          std::smatch match = *iter;
          std::string schema = match[1].str();
          std::string table = match[2].str();
          std::transform(schema.begin(), schema.end(), schema.begin(),
                         ::toupper);
          std::transform(table.begin(), table.end(), table.begin(), ::toupper);
          if (schema == upperSchema || schema.empty()) {
            referencedTables.insert({upperSchema, table});
          } else {
            referencedTables.insert({schema, table});
          }
        }

        std::sregex_iterator joinIter(viewText.begin(), viewText.end(),
                                      joinRegex);
        for (; joinIter != end; ++joinIter) {
          std::smatch match = *joinIter;
          std::string schema = match[1].str();
          std::string table = match[2].str();
          std::transform(schema.begin(), schema.end(), schema.begin(),
                         ::toupper);
          std::transform(table.begin(), table.end(), table.begin(), ::toupper);
          if (schema == upperSchema || schema.empty()) {
            referencedTables.insert({upperSchema, table});
          } else {
            referencedTables.insert({schema, table});
          }
        }

        for (const auto &refTable : referencedTables) {
          OracleLineageEdge edge;
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
          edge.discovery_method = "ALL_VIEWS";
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

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "Extracted " + std::to_string(viewEdgesAdded) +
                     " view dependencies from " +
                     std::to_string(results.size()) + " views");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                  "Error extracting view dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorOracle::extractTriggerDependencies() {
  try {
    auto conn = std::make_unique<OCIConnection>(connectionString_);
    if (!conn || !conn->isValid()) {
      return;
    }

    std::string upperSchema = schemaName_;
    std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                   ::toupper);
    std::string escapedSchema = escapeSQL(upperSchema);

    std::string query = "SELECT "
                        "owner, "
                        "trigger_name, "
                        "table_owner, "
                        "table_name, "
                        "triggering_event, "
                        "trigger_body "
                        "FROM all_triggers "
                        "WHERE owner = '" +
                        escapedSchema + "'";

    auto results = executeQuery(conn.get(), query);

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "Found " + std::to_string(results.size()) +
                     " triggers to analyze");

    int triggerEdgesAdded = 0;
    for (const auto &row : results) {
      if (row.size() >= 6) {
        std::string triggerSchema = row[0];
        std::string triggerName = row[1];
        std::string eventTableOwner = row[2];
        std::string eventTable = row[3];
        std::string triggerBody = row[5];

        std::set<std::pair<std::string, std::string>> referencedTables =
            extractReferencedTablesFromStatement(triggerBody);
        addTriggerEdge(triggerSchema, triggerName, eventTable,
                       referencedTables);
        triggerEdgesAdded++;
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "Extracted " + std::to_string(triggerEdgesAdded) +
                     " trigger dependencies from " +
                     std::to_string(results.size()) + " triggers");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                  "Error extracting trigger dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorOracle::storeLineage() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
               "storeLineage called with " + std::to_string([this]() {
                 std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                 return lineageEdges_.size();
               }()) +
                   " edges");

  std::vector<OracleLineageEdge> edgesCopy;
  {
    std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
    if (lineageEdges_.empty()) {
      Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                   "No lineage data to store");
      return;
    }
    edgesCopy = lineageEdges_;
  }

  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    pqxx::connection conn(connStr);

    pqxx::work txn(conn);
    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS metadata.oracle_lineage ("
        "id SERIAL PRIMARY KEY,"
        "edge_key VARCHAR(500) UNIQUE NOT NULL,"
        "server_name VARCHAR(200) NOT NULL,"
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

    txn.exec(createTableSQL);

    std::string createIndexSQL =
        "CREATE INDEX IF NOT EXISTS idx_oracle_lineage_server_schema "
        "ON metadata.oracle_lineage (server_name, schema_name);"
        "CREATE INDEX IF NOT EXISTS idx_oracle_lineage_object "
        "ON metadata.oracle_lineage (object_name);"
        "CREATE INDEX IF NOT EXISTS idx_oracle_lineage_target "
        "ON metadata.oracle_lineage (target_object_name);";

    txn.exec(createIndexSQL);
    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "Connected to PostgreSQL, starting to store " +
                     std::to_string(edgesCopy.size()) + " edges");

    int stored = 0;
    int failed = 0;
    for (const auto &edge : edgesCopy) {
      try {
        pqxx::work insertTxn(conn);
        std::string query = R"(
          INSERT INTO metadata.oracle_lineage (
            edge_key, server_name, schema_name,
            object_name, object_type, column_name, target_object_name,
            target_object_type, target_column_name, relationship_type,
            definition_text, dependency_level, discovery_method, discovered_by, confidence_score
          ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
          )
          ON CONFLICT (edge_key) DO UPDATE SET
            last_seen_at = NOW(),
            updated_at = NOW()
        )";

        insertTxn.exec_params(
            query, edge.edge_key, edge.server_name,
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
            "LineageExtractorOracle", edge.confidence_score);
        insertTxn.commit();
        stored++;
      } catch (const std::exception &e) {
        failed++;
        Logger::warning(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                        "Error storing lineage edge " + edge.edge_key + ": " +
                            std::string(e.what()));
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                 "Stored " + std::to_string(stored) + " lineage edges, " +
                     std::to_string(failed) + " failed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorOracle",
                  "Error storing lineage: " + std::string(e.what()));
  }
}

std::set<std::pair<std::string, std::string>>
LineageExtractorOracle::extractReferencedTablesFromStatement(
    const std::string &actionStatement) {
  std::set<std::pair<std::string, std::string>> referencedTables;

  std::regex tableRegex(R"(\bFROM\s+["]?(\w+)["]?\.?["]?(\w+)["]?)",
                        std::regex_constants::icase);
  std::regex updateRegex(R"(\bUPDATE\s+["]?(\w+)["]?\.?["]?(\w+)["]?)",
                         std::regex_constants::icase);
  std::regex insertRegex(R"(\bINSERT\s+INTO\s+["]?(\w+)["]?\.?["]?(\w+)["]?)",
                         std::regex_constants::icase);

  std::string upperSchema = schemaName_;
  std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                 ::toupper);

  auto extractMatches = [&](const std::regex &regex) {
    std::sregex_iterator iter(actionStatement.begin(), actionStatement.end(),
                              regex);
    std::sregex_iterator end;
    for (; iter != end; ++iter) {
      std::smatch match = *iter;
      std::string schema = match[1].str();
      std::string table = match[2].str();
      std::transform(schema.begin(), schema.end(), schema.begin(), ::toupper);
      std::transform(table.begin(), table.end(), table.begin(), ::toupper);
      if (schema == upperSchema || schema.empty()) {
        referencedTables.insert({upperSchema, table});
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

void LineageExtractorOracle::addTriggerEdge(
    const std::string &triggerSchema, const std::string &triggerName,
    const std::string &eventTable,
    const std::set<std::pair<std::string, std::string>> &referencedTables) {
  for (const auto &refTable : referencedTables) {
    OracleLineageEdge edge;
    edge.server_name = serverName_;
    edge.schema_name = triggerSchema;
    edge.database_name = triggerSchema;
    edge.object_name = triggerName;
    edge.object_type = "TRIGGER";
    edge.target_object_name = refTable.second;
    edge.target_object_type = "TABLE";
    edge.relationship_type = "TRIGGER_READS_TABLE";
    edge.definition_text = "Trigger on " + eventTable + " references table";
    edge.dependency_level = 2;
    edge.discovery_method = "ALL_TRIGGERS";
    edge.confidence_score = 0.85;
    edge.edge_key = generateEdgeKey(edge);

    {
      std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
      lineageEdges_.push_back(edge);
    }
  }

  if (!eventTable.empty()) {
    OracleLineageEdge edge;
    edge.server_name = serverName_;
    edge.schema_name = triggerSchema;
    edge.database_name = triggerSchema;
    edge.object_name = triggerName;
    edge.object_type = "TRIGGER";
    edge.target_object_name = eventTable;
    edge.target_object_type = "TABLE";
    edge.relationship_type = "TRIGGER_ON_TABLE";
    edge.definition_text = "Trigger on table " + eventTable;
    edge.dependency_level = 1;
    edge.discovery_method = "ALL_TRIGGERS";
    edge.confidence_score = 1.0;
    edge.edge_key = generateEdgeKey(edge);

    {
      std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
      lineageEdges_.push_back(edge);
    }
  }
}
