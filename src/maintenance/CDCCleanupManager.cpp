#include "maintenance/CDCCleanupManager.h"
#include "core/logger.h"
#include <pqxx/pqxx>
#include <sstream>
#include <iomanip>
#include <ctime>

CDCCleanupManager::CDCCleanupManager(const std::string& connectionString)
    : connectionString_(connectionString) {
  // Crear tablas si no existen
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.cdc_cleanup_policies ("
        "policy_id SERIAL PRIMARY KEY,"
        "connection_string VARCHAR(500) NOT NULL,"
        "db_engine VARCHAR(50) NOT NULL,"
        "retention_days INTEGER NOT NULL DEFAULT 30,"
        "batch_size INTEGER DEFAULT 10000,"
        "enabled BOOLEAN DEFAULT true,"
        "last_cleanup_at TIMESTAMP,"
        "created_at TIMESTAMP DEFAULT NOW(),"
        "updated_at TIMESTAMP DEFAULT NOW(),"
        "UNIQUE(connection_string, db_engine)"
        ")");

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.cdc_cleanup_history ("
        "cleanup_id SERIAL PRIMARY KEY,"
        "policy_id INTEGER REFERENCES metadata.cdc_cleanup_policies(policy_id) ON DELETE SET NULL,"
        "connection_string VARCHAR(500) NOT NULL,"
        "db_engine VARCHAR(50) NOT NULL,"
        "rows_deleted BIGINT DEFAULT 0,"
        "tables_cleaned INTEGER DEFAULT 0,"
        "space_freed_mb DECIMAL(10,2),"
        "started_at TIMESTAMP DEFAULT NOW(),"
        "completed_at TIMESTAMP,"
        "status VARCHAR(20) DEFAULT 'running',"
        "error_message TEXT"
        ")");

    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_cdc_cleanup_policies_db_engine "
        "ON metadata.cdc_cleanup_policies(db_engine)");

    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_cdc_cleanup_policies_enabled "
        "ON metadata.cdc_cleanup_policies(enabled)");

    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_cdc_cleanup_history_policy_id "
        "ON metadata.cdc_cleanup_history(policy_id)");

    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_cdc_cleanup_history_started_at "
        "ON metadata.cdc_cleanup_history(started_at)");

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MAINTENANCE, "CDCCleanupManager",
                  "Error creating tables: " + std::string(e.what()));
  }
}

int CDCCleanupManager::createOrUpdatePolicy(const CleanupPolicy& policy) {
  if (savePolicyToDatabase(policy)) {
    return policy.policyId;
  }
  return 0;
}

bool CDCCleanupManager::savePolicyToDatabase(const CleanupPolicy& policy) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string lastCleanupStr;
    if (policy.lastCleanupAt.time_since_epoch().count() > 0) {
      auto timeT = std::chrono::system_clock::to_time_t(policy.lastCleanupAt);
      std::tm tm = *std::localtime(&timeT);
      std::ostringstream oss;
      oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
      lastCleanupStr = oss.str();
    }

    if (policy.policyId > 0) {
      // Update
      txn.exec_params(
          "UPDATE metadata.cdc_cleanup_policies SET "
          "retention_days = $1, batch_size = $2, enabled = $3, "
          "last_cleanup_at = $4::timestamp, updated_at = NOW() "
          "WHERE policy_id = $5",
          policy.retentionDays, policy.batchSize, policy.enabled,
          lastCleanupStr.empty() ? nullptr : lastCleanupStr,
          policy.policyId);
    } else {
      // Insert
      auto result = txn.exec_params(
          "INSERT INTO metadata.cdc_cleanup_policies "
          "(connection_string, db_engine, retention_days, batch_size, enabled, last_cleanup_at) "
          "VALUES ($1, $2, $3, $4, $5, $6::timestamp) "
          "ON CONFLICT (connection_string, db_engine) DO UPDATE SET "
          "retention_days = EXCLUDED.retention_days, "
          "batch_size = EXCLUDED.batch_size, "
          "enabled = EXCLUDED.enabled, "
          "last_cleanup_at = EXCLUDED.last_cleanup_at, "
          "updated_at = NOW() "
          "RETURNING policy_id",
          policy.connectionString, policy.dbEngine, policy.retentionDays,
          policy.batchSize, policy.enabled,
          lastCleanupStr.empty() ? nullptr : lastCleanupStr);

      if (!result.empty()) {
        // policyId will be set by caller if needed
      }
    }

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MAINTENANCE, "CDCCleanupManager",
                  "Error saving policy: " + std::string(e.what()));
    return false;
  }
}

std::unique_ptr<CDCCleanupManager::CleanupPolicy> CDCCleanupManager::getPolicy(
    const std::string& connectionString, const std::string& dbEngine) {
  return loadPolicyFromDatabase(connectionString, dbEngine);
}

std::unique_ptr<CDCCleanupManager::CleanupPolicy> CDCCleanupManager::loadPolicyFromDatabase(
    const std::string& connectionString, const std::string& dbEngine) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params(
        "SELECT * FROM metadata.cdc_cleanup_policies "
        "WHERE connection_string = $1 AND db_engine = $2",
        connectionString, dbEngine);

    if (result.empty()) {
      return nullptr;
    }

    auto row = result[0];
    auto policy = std::make_unique<CleanupPolicy>();
    policy->policyId = row["policy_id"].as<int>();
    policy->connectionString = row["connection_string"].as<std::string>();
    policy->dbEngine = row["db_engine"].as<std::string>();
    policy->retentionDays = row["retention_days"].as<int>();
    policy->batchSize = row["batch_size"].as<int>();
    policy->enabled = row["enabled"].as<bool>();
    if (!row["last_cleanup_at"].is_null()) {
      auto lastCleanupStr = row["last_cleanup_at"].as<std::string>();
      std::tm tm = {};
      std::istringstream ss(lastCleanupStr);
      ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
      policy->lastCleanupAt = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    }

    return policy;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MAINTENANCE, "CDCCleanupManager",
                  "Error loading policy: " + std::string(e.what()));
    return nullptr;
  }
}

std::vector<CDCCleanupManager::CleanupPolicy> CDCCleanupManager::listPolicies(bool enabledOnly) {
  std::vector<CleanupPolicy> policies;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = "SELECT * FROM metadata.cdc_cleanup_policies";
    if (enabledOnly) {
      query += " WHERE enabled = true";
    }
    query += " ORDER BY connection_string, db_engine";

    auto result = txn.exec(query);

    for (const auto& row : result) {
      std::string connStr = row["connection_string"].as<std::string>();
      std::string engine = row["db_engine"].as<std::string>();
      auto policy = loadPolicyFromDatabase(connStr, engine);
      if (policy) {
        policies.push_back(*policy);
      }
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MAINTENANCE, "CDCCleanupManager",
                  "Error listing policies: " + std::string(e.what()));
  }

  return policies;
}

int64_t CDCCleanupManager::getLastProcessedChangeId(const std::string& connectionString,
                                                    const std::string& dbEngine,
                                                    const std::string& schemaName,
                                                    const std::string& tableName) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Obtener último change_id procesado desde metadata.catalog
    auto result = txn.exec_params(
        "SELECT sync_metadata->>'last_change_id' as last_change_id "
        "FROM metadata.catalog "
        "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
        schemaName, tableName, dbEngine);

    if (!result.empty() && !result[0]["last_change_id"].is_null()) {
      std::string value = result[0]["last_change_id"].as<std::string>();
      if (!value.empty()) {
        try {
          return std::stoll(value);
        } catch (...) {
          return 0;
        }
      }
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MAINTENANCE, "CDCCleanupManager",
                  "Error getting last processed change_id: " + std::string(e.what()));
  }

  return 0;
}

std::vector<std::pair<std::string, std::string>> CDCCleanupManager::getChangeLogTables(
    const std::string& connectionString, const std::string& dbEngine) {
  std::vector<std::pair<std::string, std::string>> tables;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Obtener tablas que usan CDC changelog desde metadata.catalog
    auto result = txn.exec_params(
        "SELECT DISTINCT schema_name, table_name "
        "FROM metadata.catalog "
        "WHERE db_engine = $1 AND connection_string = $2 "
        "AND sync_metadata->>'last_change_id' IS NOT NULL",
        dbEngine, connectionString);

    for (const auto& row : result) {
      tables.push_back({row["schema_name"].as<std::string>(),
                       row["table_name"].as<std::string>()});
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MAINTENANCE, "CDCCleanupManager",
                  "Error getting changelog tables: " + std::string(e.what()));
  }

  return tables;
}

int64_t CDCCleanupManager::cleanupChangeLogTable(const std::string& connectionString,
                                                 const std::string& dbEngine,
                                                 const std::string& schemaName,
                                                 const std::string& tableName,
                                                 int64_t beforeChangeId,
                                                 int batchSize) {
  int64_t totalDeleted = 0;

  try {
    // La limpieza de ds_change_log requiere acceso directo a la base de datos fuente
    // Por ahora, retornamos 0 y logueamos que necesita implementación específica por engine
    Logger::info(LogCategory::MAINTENANCE, "CDCCleanupManager",
                 "Cleanup for " + schemaName + "." + tableName +
                     " requires engine-specific implementation");

    // La implementación real dependería del engine:
    // - MariaDB/MSSQL/Oracle/PostgreSQL: DELETE FROM datasync_metadata.ds_change_log WHERE change_id < beforeChangeId
    // - MongoDB: No tiene ds_change_log, usa Change Streams
    // - Necesitaríamos acceso a la conexión nativa de cada engine
    // Por ahora, esto es un stub que necesita implementación completa

  } catch (const std::exception& e) {
    Logger::error(LogCategory::MAINTENANCE, "CDCCleanupManager",
                  "Error cleaning changelog table: " + std::string(e.what()));
  }

  return totalDeleted;
}

CDCCleanupManager::CleanupResult CDCCleanupManager::executeCleanup(int policyId) {
  CleanupResult result = {};
  result.status = "running";
  result.startedAt = std::chrono::system_clock::now();

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto policyResult = txn.exec_params(
        "SELECT * FROM metadata.cdc_cleanup_policies WHERE policy_id = $1", policyId);

    if (policyResult.empty()) {
      result.status = "failed";
      result.errorMessage = "Policy not found";
      result.completedAt = std::chrono::system_clock::now();
      saveCleanupResultToDatabase(result);
      return result;
    }

    auto row = policyResult[0];
    std::string connectionString = row["connection_string"].as<std::string>();
    std::string dbEngine = row["db_engine"].as<std::string>();
    int retentionDays = row["retention_days"].as<int>();
    int batchSize = row["batch_size"].as<int>();

    result.policyId = policyId;
    result.connectionString = connectionString;
    result.dbEngine = dbEngine;

    // Calcular change_id threshold basado en retention_days
    auto cutoffTime = std::chrono::system_clock::now() - std::chrono::hours(24 * retentionDays);
    // En producción, necesitaríamos convertir esto a change_id basado en change_time

    auto tables = getChangeLogTables(connectionString, dbEngine);
    result.tablesCleaned = tables.size();

    for (const auto& [schemaName, tableName] : tables) {
      int64_t lastProcessedId = getLastProcessedChangeId(connectionString, dbEngine,
                                                         schemaName, tableName);
      // Solo limpiar cambios anteriores al último procesado
      if (lastProcessedId > 0) {
        // Calcular change_id threshold (simplificado - en producción usar change_time)
        int64_t thresholdId = lastProcessedId - (retentionDays * 1000); // Simplificado
        int64_t deleted = cleanupChangeLogTable(connectionString, dbEngine, schemaName,
                                                tableName, thresholdId, batchSize);
        result.rowsDeleted += deleted;
      }
    }

    result.status = "completed";
    result.completedAt = std::chrono::system_clock::now();

    // Actualizar last_cleanup_at en policy
    txn.exec_params(
        "UPDATE metadata.cdc_cleanup_policies SET last_cleanup_at = NOW() WHERE policy_id = $1",
        policyId);
    txn.commit();

    saveCleanupResultToDatabase(result);
  } catch (const std::exception& e) {
    result.status = "failed";
    result.errorMessage = e.what();
    result.completedAt = std::chrono::system_clock::now();
    saveCleanupResultToDatabase(result);
  }

  return result;
}

CDCCleanupManager::CleanupResult CDCCleanupManager::executeCleanup(
    const std::string& connectionString, const std::string& dbEngine) {
  auto policy = getPolicy(connectionString, dbEngine);
  if (!policy) {
    CleanupResult result = {};
    result.status = "failed";
    result.errorMessage = "Policy not found for connection";
    result.completedAt = std::chrono::system_clock::now();
    return result;
  }

  return executeCleanup(policy->policyId);
}

std::vector<CDCCleanupManager::CleanupResult> CDCCleanupManager::executeCleanupAll() {
  std::vector<CleanupResult> results;
  auto policies = listPolicies(true); // Solo habilitadas

  for (const auto& policy : policies) {
    if (policy.enabled) {
      results.push_back(executeCleanup(policy.policyId));
    }
  }

  return results;
}

std::vector<CDCCleanupManager::CleanupResult> CDCCleanupManager::getCleanupHistory(
    const std::string& connectionString, int limit) {
  std::vector<CleanupResult> results;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = "SELECT * FROM metadata.cdc_cleanup_history";
    if (!connectionString.empty()) {
      query += " WHERE connection_string = $1";
      query += " ORDER BY started_at DESC LIMIT $" + std::to_string(connectionString.empty() ? 1 : 2);
    } else {
      query += " ORDER BY started_at DESC LIMIT $1";
    }

    pqxx::result result;
    if (!connectionString.empty()) {
      result = txn.exec_params(query, connectionString, limit);
    } else {
      result = txn.exec_params(query, limit);
    }

    for (const auto& row : result) {
      CleanupResult cleanup;
      cleanup.cleanupId = row["cleanup_id"].as<int>();
      if (!row["policy_id"].is_null()) {
        cleanup.policyId = row["policy_id"].as<int>();
      }
      cleanup.connectionString = row["connection_string"].as<std::string>();
      cleanup.dbEngine = row["db_engine"].as<std::string>();
      cleanup.rowsDeleted = row["rows_deleted"].as<int64_t>();
      cleanup.tablesCleaned = row["tables_cleaned"].as<int>();
      if (!row["space_freed_mb"].is_null()) {
        cleanup.spaceFreedMB = row["space_freed_mb"].as<double>();
      }
      if (!row["started_at"].is_null()) {
        auto startedStr = row["started_at"].as<std::string>();
        std::tm tm = {};
        std::istringstream ss(startedStr);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        cleanup.startedAt = std::chrono::system_clock::from_time_t(std::mktime(&tm));
      }
      if (!row["completed_at"].is_null()) {
        auto completedStr = row["completed_at"].as<std::string>();
        std::tm tm = {};
        std::istringstream ss(completedStr);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        cleanup.completedAt = std::chrono::system_clock::from_time_t(std::mktime(&tm));
      }
      cleanup.status = row["status"].as<std::string>();
      if (!row["error_message"].is_null()) {
        cleanup.errorMessage = row["error_message"].as<std::string>();
      }
      results.push_back(cleanup);
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MAINTENANCE, "CDCCleanupManager",
                  "Error getting cleanup history: " + std::string(e.what()));
  }

  return results;
}

bool CDCCleanupManager::saveCleanupResultToDatabase(const CleanupResult& result) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto startedTimeT = std::chrono::system_clock::to_time_t(result.startedAt);
    std::tm startedTm = *std::localtime(&startedTimeT);
    std::ostringstream startedOss;
    startedOss << std::put_time(&startedTm, "%Y-%m-%d %H:%M:%S");
    std::string startedStr = startedOss.str();

    std::string completedStr;
    if (result.completedAt.time_since_epoch().count() > 0) {
      auto completedTimeT = std::chrono::system_clock::to_time_t(result.completedAt);
      std::tm completedTm = *std::localtime(&completedTimeT);
      std::ostringstream completedOss;
      completedOss << std::put_time(&completedTm, "%Y-%m-%d %H:%M:%S");
      completedStr = completedOss.str();
    }

    if (result.cleanupId > 0) {
      // Update
      txn.exec_params(
          "UPDATE metadata.cdc_cleanup_history SET "
          "rows_deleted = $1, tables_cleaned = $2, space_freed_mb = $3, "
          "completed_at = $4::timestamp, status = $5, error_message = $6 "
          "WHERE cleanup_id = $7",
          result.rowsDeleted, result.tablesCleaned, result.spaceFreedMB,
          completedStr.empty() ? nullptr : completedStr,
          result.status,
          result.errorMessage.empty() ? nullptr : result.errorMessage,
          result.cleanupId);
    } else {
      // Insert
      auto insertResult = txn.exec_params(
          "INSERT INTO metadata.cdc_cleanup_history "
          "(policy_id, connection_string, db_engine, rows_deleted, tables_cleaned, "
          "space_freed_mb, started_at, completed_at, status, error_message) "
          "VALUES ($1, $2, $3, $4, $5, $6, $7::timestamp, $8::timestamp, $9, $10) "
          "RETURNING cleanup_id",
          result.policyId > 0 ? result.policyId : 0,
          result.connectionString, result.dbEngine,
          result.rowsDeleted, result.tablesCleaned, result.spaceFreedMB,
          startedStr,
          completedStr.empty() ? nullptr : completedStr,
          result.status,
          result.errorMessage.empty() ? nullptr : result.errorMessage);

      if (!insertResult.empty()) {
        // cleanupId will be set by caller if needed
      }
    }

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MAINTENANCE, "CDCCleanupManager",
                  "Error saving cleanup result: " + std::string(e.what()));
    return false;
  }
}

json CDCCleanupManager::getCleanupStats() {
  json stats = json::object();

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto policiesResult = txn.exec(
        "SELECT COUNT(*) as total, COUNT(CASE WHEN enabled THEN 1 END) as enabled "
        "FROM metadata.cdc_cleanup_policies");

    auto historyResult = txn.exec(
        "SELECT "
        "SUM(rows_deleted) as total_rows_deleted, "
        "SUM(tables_cleaned) as total_tables_cleaned, "
        "SUM(space_freed_mb) as total_space_freed_mb, "
        "COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_cleanups, "
        "COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_cleanups "
        "FROM metadata.cdc_cleanup_history");

    auto result = policiesResult;

    if (!policiesResult.empty()) {
      auto policyRow = policiesResult[0];
      stats["total_policies"] = policyRow["total"].as<int>();
      stats["enabled_policies"] = policyRow["enabled"].as<int>();
    }

    if (!historyResult.empty()) {
      auto historyRow = historyResult[0];
      if (!historyRow["total_rows_deleted"].is_null()) {
        stats["total_rows_deleted"] = historyRow["total_rows_deleted"].as<int64_t>();
      }
      if (!historyRow["total_tables_cleaned"].is_null()) {
        stats["total_tables_cleaned"] = historyRow["total_tables_cleaned"].as<int>();
      }
      if (!historyRow["total_space_freed_mb"].is_null()) {
        stats["total_space_freed_mb"] = historyRow["total_space_freed_mb"].as<double>();
      }
      stats["successful_cleanups"] = historyRow["successful_cleanups"].as<int>();
      stats["failed_cleanups"] = historyRow["failed_cleanups"].as<int>();
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MAINTENANCE, "CDCCleanupManager",
                  "Error getting stats: " + std::string(e.what()));
  }

  return stats;
}
