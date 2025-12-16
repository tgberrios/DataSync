#include "governance/DataRetentionManager.h"
#include "core/logger.h"
#include <ctime>
#include <iomanip>
#include <pqxx/pqxx>
#include <sstream>

DataRetentionManager::DataRetentionManager(const std::string &connectionString)
    : connectionString_(connectionString) {}

bool DataRetentionManager::isLegalHold(const std::string &schemaName,
                                       const std::string &tableName) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT legal_hold, legal_hold_until
      FROM metadata.data_governance_catalog
      WHERE schema_name = $1 AND table_name = $2
    )";

    auto result = txn.exec_params(query, schemaName, tableName);
    if (!result.empty()) {
      bool hold = result[0][0].is_null() ? false : result[0][0].as<bool>();
      if (hold) {
        if (!result[0][1].is_null()) {
          std::string holdUntil = result[0][1].as<std::string>();
          std::tm tm = {};
          std::istringstream ss(holdUntil);
          ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
          auto holdTime = std::mktime(&tm);
          auto now = std::time(nullptr);
          if (holdTime > now) {
            return true;
          }
        } else {
          return true;
        }
      }
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataRetentionManager",
                  "Error checking legal hold: " + std::string(e.what()));
  }

  return false;
}

std::string DataRetentionManager::calculateExpirationDate(
    const std::string &retentionPolicy) {
  auto now = std::time(nullptr);
  std::tm *tm = std::localtime(&now);

  int years = 0;
  if (retentionPolicy.find("7_YEARS") != std::string::npos) {
    years = 7;
  } else if (retentionPolicy.find("5_YEARS") != std::string::npos) {
    years = 5;
  } else if (retentionPolicy.find("3_YEARS") != std::string::npos) {
    years = 3;
  } else if (retentionPolicy.find("2_YEARS") != std::string::npos) {
    years = 2;
  } else if (retentionPolicy.find("1_YEAR") != std::string::npos) {
    years = 1;
  }

  tm->tm_year += years;

  std::stringstream ss;
  ss << std::put_time(tm, "%Y-%m-%d %H:%M:%S");
  return ss.str();
}

bool DataRetentionManager::archiveData(const std::string &schemaName,
                                       const std::string &tableName,
                                       const std::string &archivalLocation) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string archiveQuery =
        "COPY " + txn.quote_name(schemaName) + "." + txn.quote_name(tableName) +
        " TO " +
        txn.quote(archivalLocation + "/" + schemaName + "_" + tableName + "_" +
                  std::to_string(std::time(nullptr)) + ".csv") +
        " WITH CSV HEADER";

    txn.exec(archiveQuery);

    std::string updateQuery = R"(
      UPDATE metadata.data_governance_catalog
      SET archival_location = $1,
          last_archived_at = NOW()
      WHERE schema_name = $2 AND table_name = $3
    )";

    txn.exec_params(updateQuery, archivalLocation, schemaName, tableName);
    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "DataRetentionManager",
                 "Archived table " + schemaName + "." + tableName + " to " +
                     archivalLocation);

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataRetentionManager",
                  "Error archiving data: " + std::string(e.what()));
    return false;
  }
}

bool DataRetentionManager::deleteExpiredData(
    const std::string &schemaName, const std::string &tableName,
    const std::string &expirationDate) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string deleteQuery = "DELETE FROM " + txn.quote_name(schemaName) +
                              "." + txn.quote_name(tableName) +
                              " WHERE created_at < " +
                              txn.quote(expirationDate);

    auto result = txn.exec(deleteQuery);
    long long deletedRows = result.affected_rows();

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "DataRetentionManager",
                 "Deleted " + std::to_string(deletedRows) +
                     " expired rows from " + schemaName + "." + tableName);

    return deletedRows > 0;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataRetentionManager",
                  "Error deleting expired data: " + std::string(e.what()));
    return false;
  }
}

void DataRetentionManager::scheduleRetentionJobs() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT schema_name, table_name, retention_policy, data_expiration_date,
             retention_enforced, legal_hold
      FROM metadata.data_governance_catalog
      WHERE retention_policy IS NOT NULL
        AND retention_enforced = true
        AND legal_hold = false
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      std::string schemaName = row[0].as<std::string>();
      std::string tableName = row[1].as<std::string>();
      std::string retentionPolicy = row[2].as<std::string>();
      std::string expirationDate =
          row[3].is_null() ? "" : row[3].as<std::string>();

      if (expirationDate.empty()) {
        expirationDate = calculateExpirationDate(retentionPolicy);

        std::string updateQuery = R"(
          UPDATE metadata.data_governance_catalog
          SET data_expiration_date = $1
          WHERE schema_name = $2 AND table_name = $3
        )";

        txn.exec_params(updateQuery, expirationDate, schemaName, tableName);
      }

      std::string insertQuery = R"(
        INSERT INTO metadata.data_retention_jobs (
          schema_name, table_name, job_type, retention_policy, scheduled_date, status
        ) VALUES ($1, $2, $3, $4, $5, 'PENDING')
        ON CONFLICT DO NOTHING
      )";

      std::string jobType = "DELETE";
      if (retentionPolicy.find("ARCHIVE") != std::string::npos) {
        jobType = "ARCHIVE";
      }

      txn.exec_params(insertQuery, schemaName, tableName, jobType,
                      retentionPolicy, expirationDate);
    }

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "DataRetentionManager",
                 "Scheduled " + std::to_string(result.size()) +
                     " retention jobs");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataRetentionManager",
                  "Error scheduling retention jobs: " + std::string(e.what()));
  }
}

bool DataRetentionManager::executeRetentionJob(int jobId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string selectQuery = R"(
      SELECT schema_name, table_name, job_type, retention_policy, scheduled_date
      FROM metadata.data_retention_jobs
      WHERE id = $1 AND status = 'PENDING'
    )";

    auto result = txn.exec_params(selectQuery, std::to_string(jobId));
    if (result.empty()) {
      return false;
    }

    std::string schemaName = result[0][0].as<std::string>();
    std::string tableName = result[0][1].as<std::string>();
    std::string jobType = result[0][2].as<std::string>();
    std::string scheduledDate = result[0][4].as<std::string>();

    auto now = std::time(nullptr);
    std::tm tm = {};
    std::istringstream ss(scheduledDate);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    auto scheduledTime = std::mktime(&tm);

    if (scheduledTime > now) {
      std::string updateQuery = R"(
        UPDATE metadata.data_retention_jobs
        SET status = 'SCHEDULED'
        WHERE id = $1
      )";
      txn.exec_params(updateQuery, std::to_string(jobId));
      txn.commit();
      return true;
    }

    bool success = false;
    long long rowsAffected = 0;
    std::string errorMessage = "";

    if (jobType == "ARCHIVE") {
      std::string archivalLocation = "/archive/" + schemaName + "/" + tableName;
      success = archiveData(schemaName, tableName, archivalLocation);
      if (success) {
        rowsAffected = 1;
      }
    } else if (jobType == "DELETE") {
      success = deleteExpiredData(schemaName, tableName, scheduledDate);
      if (success) {
        rowsAffected = 1;
      }
    }

    std::string updateQuery = R"(
      UPDATE metadata.data_retention_jobs
      SET status = $1,
          executed_at = NOW(),
          rows_affected = $2,
          error_message = $3
      WHERE id = $4
    )";

    std::string status = success ? "COMPLETED" : "FAILED";
    txn.exec_params(updateQuery, status, std::to_string(rowsAffected),
                    errorMessage, std::to_string(jobId));

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "DataRetentionManager",
                 "Executed retention job " + std::to_string(jobId) +
                     " with status " + status);

    return success;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataRetentionManager",
                  "Error executing retention job: " + std::string(e.what()));
    return false;
  }
}

std::vector<RetentionJob> DataRetentionManager::getPendingJobs() {
  std::vector<RetentionJob> jobs;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, schema_name, table_name, job_type, retention_policy,
             scheduled_date, status, rows_affected, error_message
      FROM metadata.data_retention_jobs
      WHERE status IN ('PENDING', 'SCHEDULED')
      ORDER BY scheduled_date ASC
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      RetentionJob job;
      job.id = row[0].as<int>();
      job.schema_name = row[1].as<std::string>();
      job.table_name = row[2].as<std::string>();
      job.job_type = row[3].as<std::string>();
      job.retention_policy = row[4].as<std::string>();
      job.scheduled_date = row[5].as<std::string>();
      job.status = row[6].as<std::string>();
      job.rows_affected = row[7].is_null() ? 0 : row[7].as<long long>();
      job.error_message = row[8].is_null() ? "" : row[8].as<std::string>();

      jobs.push_back(job);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataRetentionManager",
                  "Error getting pending jobs: " + std::string(e.what()));
  }

  return jobs;
}

std::vector<RetentionJob>
DataRetentionManager::getJobsByStatus(const std::string &status) {
  std::vector<RetentionJob> jobs;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, schema_name, table_name, job_type, retention_policy,
             scheduled_date, status, rows_affected, error_message
      FROM metadata.data_retention_jobs
      WHERE status = $1
      ORDER BY executed_at DESC
    )";

    auto result = txn.exec_params(query, status);

    for (const auto &row : result) {
      RetentionJob job;
      job.id = row[0].as<int>();
      job.schema_name = row[1].as<std::string>();
      job.table_name = row[2].as<std::string>();
      job.job_type = row[3].as<std::string>();
      job.retention_policy = row[4].as<std::string>();
      job.scheduled_date = row[5].as<std::string>();
      job.status = row[6].as<std::string>();
      job.rows_affected = row[7].is_null() ? 0 : row[7].as<long long>();
      job.error_message = row[8].is_null() ? "" : row[8].as<std::string>();

      jobs.push_back(job);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataRetentionManager",
                  "Error getting jobs by status: " + std::string(e.what()));
  }

  return jobs;
}

bool DataRetentionManager::enforceRetentionPolicy(
    const std::string &schemaName, const std::string &tableName) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.data_governance_catalog
      SET retention_enforced = true
      WHERE schema_name = $1 AND table_name = $2
    )";

    txn.exec_params(query, schemaName, tableName);
    txn.commit();

    scheduleRetentionJobs();

    Logger::info(LogCategory::GOVERNANCE, "DataRetentionManager",
                 "Enabled retention enforcement for " + schemaName + "." +
                     tableName);

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataRetentionManager",
                  "Error enforcing retention policy: " + std::string(e.what()));
    return false;
  }
}

bool DataRetentionManager::setLegalHold(const std::string &schemaName,
                                        const std::string &tableName,
                                        const std::string &reason,
                                        const std::string &holdUntil) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.data_governance_catalog
      SET legal_hold = true,
          legal_hold_reason = $1,
          legal_hold_until = $2,
          retention_enforced = false
      WHERE schema_name = $3 AND table_name = $4
    )";

    txn.exec_params(query, reason, holdUntil, schemaName, tableName);
    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "DataRetentionManager",
                 "Set legal hold for " + schemaName + "." + tableName);

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataRetentionManager",
                  "Error setting legal hold: " + std::string(e.what()));
    return false;
  }
}

bool DataRetentionManager::releaseLegalHold(const std::string &schemaName,
                                            const std::string &tableName) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.data_governance_catalog
      SET legal_hold = false,
          legal_hold_reason = NULL,
          legal_hold_until = NULL,
          retention_enforced = true
      WHERE schema_name = $1 AND table_name = $2
    )";

    txn.exec_params(query, schemaName, tableName);
    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "DataRetentionManager",
                 "Released legal hold for " + schemaName + "." + tableName);

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataRetentionManager",
                  "Error releasing legal hold: " + std::string(e.what()));
    return false;
  }
}

bool DataRetentionManager::archiveTable(const std::string &schemaName,
                                        const std::string &tableName,
                                        const std::string &archivalLocation) {
  return archiveData(schemaName, tableName, archivalLocation);
}

void DataRetentionManager::processExpiredData() {
  auto pendingJobs = getPendingJobs();

  for (const auto &job : pendingJobs) {
    executeRetentionJob(job.id);
  }

  Logger::info(LogCategory::GOVERNANCE, "DataRetentionManager",
               "Processed " + std::to_string(pendingJobs.size()) +
                   " expired data jobs");
}
