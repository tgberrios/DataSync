#include "governance/AccessControlManager.h"
#include "core/logger.h"
#include <algorithm>
#include <ctime>
#include <pqxx/pqxx>

AccessControlManager::AccessControlManager(const std::string &connectionString)
    : connectionString_(connectionString) {}

bool AccessControlManager::isSensitiveData(const std::string &schemaName,
                                           const std::string &tableName,
                                           const std::string &columnName) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    if (!columnName.empty()) {
      std::string query = R"(
        SELECT contains_pii, contains_phi
        FROM metadata.column_catalog
        WHERE schema_name = $1 AND table_name = $2 AND column_name = $3
      )";

      auto result = txn.exec_params(query, schemaName, tableName, columnName);
      if (!result.empty()) {
        bool pii = result[0][0].is_null() ? false : result[0][0].as<bool>();
        bool phi = result[0][1].is_null() ? false : result[0][1].as<bool>();
        return pii || phi;
      }
    } else {
      std::string query = R"(
        SELECT COUNT(*) > 0
        FROM metadata.column_catalog
        WHERE schema_name = $1 AND table_name = $2
          AND (contains_pii = true OR contains_phi = true)
      )";

      auto result = txn.exec_params(query, schemaName, tableName);
      if (!result.empty()) {
        return result[0][0].as<bool>();
      }
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AccessControlManager",
                  "Error checking sensitive data: " + std::string(e.what()));
  }

  return false;
}

bool AccessControlManager::shouldMask(const std::string &schemaName,
                                      const std::string &tableName,
                                      const std::string &columnName,
                                      const std::string &username) {
  if (!isSensitiveData(schemaName, tableName, columnName)) {
    return false;
  }

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT masking_policy_applied, access_control_policy
      FROM metadata.data_governance_catalog
      WHERE schema_name = $1 AND table_name = $2
    )";

    auto result = txn.exec_params(query, schemaName, tableName);
    if (!result.empty()) {
      bool maskingEnabled =
          result[0][0].is_null() ? false : result[0][0].as<bool>();
      return maskingEnabled;
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AccessControlManager",
                  "Error checking masking policy: " + std::string(e.what()));
  }

  return false;
}

void AccessControlManager::logAccess(const AccessLogEntry &entry) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      INSERT INTO metadata.data_access_log (
        schema_name, table_name, column_name, access_type, username,
        application_name, client_addr, query_text, rows_accessed,
        is_sensitive_data, masking_applied, compliance_requirement
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    )";

    txn.exec_params(query, entry.schema_name, entry.table_name,
                    entry.column_name, entry.access_type, entry.username,
                    entry.application_name, entry.client_addr, entry.query_text,
                    entry.rows_accessed, entry.is_sensitive_data,
                    entry.masking_applied, entry.compliance_requirement);

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AccessControlManager",
                  "Error logging access: " + std::string(e.what()));
  }
}

void AccessControlManager::logQueryAccess(const std::string &schemaName,
                                          const std::string &tableName,
                                          const std::string &username,
                                          const std::string &queryText,
                                          long long rowsAccessed,
                                          const std::string &applicationName,
                                          const std::string &clientAddr) {
  AccessLogEntry entry;
  entry.schema_name = schemaName;
  entry.table_name = tableName;
  entry.username = username;
  entry.query_text = queryText;
  entry.rows_accessed = rowsAccessed;
  entry.application_name = applicationName;
  entry.client_addr = clientAddr;
  entry.access_type = "SELECT";
  entry.is_sensitive_data = isSensitiveData(schemaName, tableName);
  entry.masking_applied = shouldMask(schemaName, tableName, "", username);

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT compliance_requirements
      FROM metadata.data_governance_catalog
      WHERE schema_name = $1 AND table_name = $2
    )";

    auto result = txn.exec_params(query, schemaName, tableName);
    if (!result.empty() && !result[0][0].is_null()) {
      entry.compliance_requirement = result[0][0].as<std::string>();
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::GOVERNANCE, "AccessControlManager",
                    "Error getting compliance requirements: " +
                        std::string(e.what()));
  }

  logAccess(entry);
}

std::vector<AccessLogEntry>
AccessControlManager::getAccessHistory(const std::string &schemaName,
                                       const std::string &tableName,
                                       const std::string &username, int limit) {
  std::vector<AccessLogEntry> entries;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT schema_name, table_name, column_name, access_type, username,
             application_name, client_addr, query_text, rows_accessed,
             is_sensitive_data, masking_applied, compliance_requirement, access_timestamp
      FROM metadata.data_access_log
      WHERE schema_name = $1 AND table_name = $2
    )";

    std::vector<std::string> params = {schemaName, tableName};

    if (!username.empty()) {
      query += " AND username = $3";
      params.push_back(username);
    }

    query += " ORDER BY access_timestamp DESC LIMIT $" +
             std::to_string(params.size() + 1);
    params.push_back(std::to_string(limit));

    pqxx::params pqParams;
    for (const auto &p : params) {
      pqParams.append(p);
    }

    auto result = txn.exec_params(query, pqParams);

    for (const auto &row : result) {
      AccessLogEntry entry;
      entry.schema_name = row[0].as<std::string>();
      entry.table_name = row[1].as<std::string>();
      entry.column_name = row[2].is_null() ? "" : row[2].as<std::string>();
      entry.access_type = row[3].as<std::string>();
      entry.username = row[4].as<std::string>();
      entry.application_name = row[5].is_null() ? "" : row[5].as<std::string>();
      entry.client_addr = row[6].is_null() ? "" : row[6].as<std::string>();
      entry.query_text = row[7].is_null() ? "" : row[7].as<std::string>();
      entry.rows_accessed = row[8].is_null() ? 0 : row[8].as<long long>();
      entry.is_sensitive_data = row[9].is_null() ? false : row[9].as<bool>();
      entry.masking_applied = row[10].is_null() ? false : row[10].as<bool>();
      entry.compliance_requirement =
          row[11].is_null() ? "" : row[11].as<std::string>();

      entries.push_back(entry);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AccessControlManager",
                  "Error getting access history: " + std::string(e.what()));
  }

  return entries;
}

std::vector<AccessLogEntry>
AccessControlManager::getSensitiveDataAccess(const std::string &username,
                                             int days) {
  std::vector<AccessLogEntry> entries;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT schema_name, table_name, column_name, access_type, username,
             application_name, client_addr, query_text, rows_accessed,
             is_sensitive_data, masking_applied, compliance_requirement, access_timestamp
      FROM metadata.data_access_log
      WHERE is_sensitive_data = true
        AND access_timestamp >= NOW() - INTERVAL '1 day' * $1
    )";

    std::vector<std::string> params = {std::to_string(days)};

    if (!username.empty()) {
      query += " AND username = $2";
      params.push_back(username);
    }

    query += " ORDER BY access_timestamp DESC";

    pqxx::params pqParams;
    for (const auto &p : params) {
      pqParams.append(p);
    }

    auto result = txn.exec_params(query, pqParams);

    for (const auto &row : result) {
      AccessLogEntry entry;
      entry.schema_name = row[0].as<std::string>();
      entry.table_name = row[1].as<std::string>();
      entry.column_name = row[2].is_null() ? "" : row[2].as<std::string>();
      entry.access_type = row[3].as<std::string>();
      entry.username = row[4].as<std::string>();
      entry.application_name = row[5].is_null() ? "" : row[5].as<std::string>();
      entry.client_addr = row[6].is_null() ? "" : row[6].as<std::string>();
      entry.query_text = row[7].is_null() ? "" : row[7].as<std::string>();
      entry.rows_accessed = row[8].is_null() ? 0 : row[8].as<long long>();
      entry.is_sensitive_data = row[9].is_null() ? false : row[9].as<bool>();
      entry.masking_applied = row[10].is_null() ? false : row[10].as<bool>();
      entry.compliance_requirement =
          row[11].is_null() ? "" : row[11].as<std::string>();

      entries.push_back(entry);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AccessControlManager",
                  "Error getting sensitive data access: " +
                      std::string(e.what()));
  }

  return entries;
}

bool AccessControlManager::checkAccessPermission(
    const std::string &schemaName, const std::string &tableName,
    const std::string &username, const std::string &accessType) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT access_control_policy, sensitivity_level
      FROM metadata.data_governance_catalog
      WHERE schema_name = $1 AND table_name = $2
    )";

    auto result = txn.exec_params(query, schemaName, tableName);

    if (!result.empty()) {
      std::string policy =
          result[0][0].is_null() ? "" : result[0][0].as<std::string>();
      std::string sensitivity =
          result[0][1].is_null() ? "" : result[0][1].as<std::string>();

      if (sensitivity == "CRITICAL" || sensitivity == "HIGH") {
        if (policy.empty()) {
          return false;
        }
      }
    }

    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AccessControlManager",
                  "Error checking access permission: " + std::string(e.what()));
    return false;
  }
}

void AccessControlManager::detectAccessAnomalies(int days) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT username, COUNT(*) as access_count, COUNT(DISTINCT table_name) as table_count
      FROM metadata.data_access_log
      WHERE access_timestamp >= NOW() - INTERVAL '1 day' * $1
        AND is_sensitive_data = true
      GROUP BY username
      HAVING COUNT(*) > 1000 OR COUNT(DISTINCT table_name) > 50
      ORDER BY access_count DESC
    )";

    auto result = txn.exec_params(query, std::to_string(days));

    for (const auto &row : result) {
      std::string username = row[0].as<std::string>();
      long long accessCount = row[1].as<long long>();
      long long tableCount = row[2].as<long long>();

      Logger::warning(LogCategory::GOVERNANCE, "AccessControlManager",
                      "Anomaly detected: User " + username + " accessed " +
                          std::to_string(accessCount) + " times across " +
                          std::to_string(tableCount) +
                          " sensitive tables in last " + std::to_string(days) +
                          " days");
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AccessControlManager",
                  "Error detecting access anomalies: " + std::string(e.what()));
  }
}
