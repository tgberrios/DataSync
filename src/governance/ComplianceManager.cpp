#include "governance/ComplianceManager.h"
#include "core/database_config.h"
#include "core/logger.h"
#include <algorithm>
#include <ctime>
#include <iomanip>
#include <pqxx/pqxx>
#include <random>
#include <sstream>

ComplianceManager::ComplianceManager(const std::string &connectionString)
    : connectionString_(connectionString) {}

std::string ComplianceManager::generateRequestId() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 15);

  std::stringstream ss;
  ss << "DSAR-";
  auto now = std::time(nullptr);
  ss << std::put_time(std::localtime(&now), "%Y%m%d");
  ss << "-";

  for (int i = 0; i < 8; ++i) {
    ss << std::hex << dis(gen);
  }

  return ss.str();
}

bool ComplianceManager::validateRequest(const DataSubjectRequest &request) {
  if (request.request_type.empty()) {
    return false;
  }

  if (request.request_type != "RIGHT_TO_BE_FORGOTTEN" &&
      request.request_type != "DATA_PORTABILITY" &&
      request.request_type != "ACCESS_REQUEST") {
    return false;
  }

  if (request.data_subject_email.empty() && request.data_subject_name.empty()) {
    return false;
  }

  return true;
}

std::vector<std::string>
ComplianceManager::findDataLocations(const std::string &email,
                                     const std::string &name) {
  std::vector<std::string> locations;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT DISTINCT schema_name || '.' || table_name as location
      FROM metadata.column_catalog
      WHERE (contains_pii = true OR contains_phi = true)
        AND (
          column_name ILIKE '%email%' OR
          column_name ILIKE '%name%' OR
          column_name ILIKE '%user%' OR
          column_name ILIKE '%person%'
        )
    )";

    auto result = txn.exec(query);
    for (const auto &row : result) {
      if (!row[0].is_null()) {
        locations.push_back(row[0].as<std::string>());
      }
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Error finding data locations: " + std::string(e.what()));
  }

  return locations;
}

std::string
ComplianceManager::createDataSubjectRequest(const DataSubjectRequest &request) {
  if (!validateRequest(request)) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Invalid data subject request");
    return "";
  }

  std::string requestId =
      request.request_id.empty() ? generateRequestId() : request.request_id;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string insertQuery = R"(
      INSERT INTO metadata.data_subject_requests (
        request_id, request_type, data_subject_email, data_subject_name,
        request_status, requested_data, compliance_requirement, requested_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, NOW()
      )
    )";

    txn.exec_params(insertQuery, requestId, request.request_type,
                    request.data_subject_email, request.data_subject_name,
                    "PENDING", request.requested_data,
                    request.compliance_requirement);

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "ComplianceManager",
                 "Created data subject request: " + requestId);

    return requestId;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Error creating data subject request: " +
                      std::string(e.what()));
    return "";
  }
}

bool ComplianceManager::processRightToBeForgotten(
    const std::string &requestId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string selectQuery = R"(
      SELECT data_subject_email, data_subject_name, compliance_requirement
      FROM metadata.data_subject_requests
      WHERE request_id = $1 AND request_type = 'RIGHT_TO_BE_FORGOTTEN'
    )";

    auto result = txn.exec_params(selectQuery, requestId);
    if (result.empty()) {
      Logger::warning(LogCategory::GOVERNANCE, "ComplianceManager",
                      "Request not found: " + requestId);
      return false;
    }

    std::string email =
        result[0][0].is_null() ? "" : result[0][0].as<std::string>();
    std::string name =
        result[0][1].is_null() ? "" : result[0][1].as<std::string>();

    bool deleted = deleteDataForSubject(email, name);

    if (deleted) {
      std::string updateQuery = R"(
        UPDATE metadata.data_subject_requests
        SET request_status = 'COMPLETED',
            completed_at = NOW(),
            processed_by = 'SYSTEM'
        WHERE request_id = $1
      )";

      txn.exec_params(updateQuery, requestId);
      txn.commit();

      Logger::info(LogCategory::GOVERNANCE, "ComplianceManager",
                   "Right to be forgotten processed: " + requestId);
      return true;
    }

    txn.commit();
    return false;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Error processing right to be forgotten: " +
                      std::string(e.what()));
    return false;
  }
}

bool ComplianceManager::deleteDataForSubject(const std::string &email,
                                             const std::string &name) {
  try {
    auto locations = findDataLocations(email, name);

    if (locations.empty()) {
      Logger::info(LogCategory::GOVERNANCE, "ComplianceManager",
                   "No data locations found for subject");
      return true;
    }

    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    int deletedCount = 0;

    for (const auto &location : locations) {
      size_t dotPos = location.find('.');
      if (dotPos == std::string::npos)
        continue;

      std::string schemaName = location.substr(0, dotPos);
      std::string tableName = location.substr(dotPos + 1);

      std::string query = R"(
        SELECT column_name
        FROM metadata.column_catalog
        WHERE schema_name = $1 AND table_name = $2
          AND (column_name ILIKE '%email%' OR column_name ILIKE '%name%')
        LIMIT 1
      )";

      auto colResult = txn.exec_params(query, schemaName, tableName);
      if (!colResult.empty()) {
        std::string columnName = colResult[0][0].as<std::string>();

        std::string deleteQuery = "DELETE FROM " + txn.quote_name(schemaName) +
                                  "." + txn.quote_name(tableName) + " WHERE " +
                                  txn.quote_name(columnName);

        if (!email.empty()) {
          deleteQuery += " = " + txn.quote(email);
        } else if (!name.empty()) {
          deleteQuery += " ILIKE " + txn.quote("%" + name + "%");
        }

        try {
          auto deleteResult = txn.exec(deleteQuery);
          deletedCount += deleteResult.affected_rows();
        } catch (const std::exception &e) {
          Logger::warning(LogCategory::GOVERNANCE, "ComplianceManager",
                          "Error deleting from " + location + ": " +
                              std::string(e.what()));
        }
      }
    }

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "ComplianceManager",
                 "Deleted " + std::to_string(deletedCount) +
                     " rows for data subject");

    return deletedCount > 0;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Error deleting data for subject: " + std::string(e.what()));
    return false;
  }
}

bool ComplianceManager::processDataPortability(const std::string &requestId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string selectQuery = R"(
      SELECT data_subject_email, data_subject_name
      FROM metadata.data_subject_requests
      WHERE request_id = $1 AND request_type = 'DATA_PORTABILITY'
    )";

    auto result = txn.exec_params(selectQuery, requestId);
    if (result.empty()) {
      return false;
    }

    std::string email =
        result[0][0].is_null() ? "" : result[0][0].as<std::string>();
    std::string name =
        result[0][1].is_null() ? "" : result[0][1].as<std::string>();

    std::string exportedData = exportDataForSubject(email, name);

    std::string updateQuery = R"(
      UPDATE metadata.data_subject_requests
      SET request_status = 'COMPLETED',
          response_data = $1,
          completed_at = NOW(),
          processed_by = 'SYSTEM'
      WHERE request_id = $2
    )";

    txn.exec_params(updateQuery, exportedData, requestId);
    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "ComplianceManager",
                 "Data portability processed: " + requestId);

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Error processing data portability: " +
                      std::string(e.what()));
    return false;
  }
}

std::string ComplianceManager::exportDataForSubject(const std::string &email,
                                                    const std::string &name) {
  std::stringstream exportData;
  exportData << "{\"data_subject\": {";

  if (!email.empty()) {
    exportData << "\"email\": \"" << email << "\",";
  }
  if (!name.empty()) {
    exportData << "\"name\": \"" << name << "\",";
  }

  exportData << "\"data_locations\": [";

  auto locations = findDataLocations(email, name);
  for (size_t i = 0; i < locations.size(); ++i) {
    if (i > 0)
      exportData << ",";
    exportData << "\"" << locations[i] << "\"";
  }

  exportData << "], \"export_timestamp\": \"" << std::time(nullptr) << "\"";
  exportData << "}}";

  return exportData.str();
}

bool ComplianceManager::processAccessRequest(const std::string &requestId) {
  return processDataPortability(requestId);
}

std::vector<DataSubjectRequest> ComplianceManager::getPendingRequests() {
  std::vector<DataSubjectRequest> requests;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT request_id, request_type, data_subject_email, data_subject_name,
             request_status, requested_data, compliance_requirement
      FROM metadata.data_subject_requests
      WHERE request_status = 'PENDING'
      ORDER BY requested_at ASC
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      DataSubjectRequest req;
      req.request_id = row[0].as<std::string>();
      req.request_type = row[1].as<std::string>();
      req.data_subject_email = row[2].is_null() ? "" : row[2].as<std::string>();
      req.data_subject_name = row[3].is_null() ? "" : row[3].as<std::string>();
      req.request_status = row[4].as<std::string>();
      req.requested_data = row[5].is_null() ? "" : row[5].as<std::string>();
      req.compliance_requirement =
          row[6].is_null() ? "" : row[6].as<std::string>();

      requests.push_back(req);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Error getting pending requests: " + std::string(e.what()));
  }

  return requests;
}

bool ComplianceManager::updateRequestStatus(const std::string &requestId,
                                            const std::string &status,
                                            const std::string &processedBy) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.data_subject_requests
      SET request_status = $1,
          processed_by = $2,
          completed_at = CASE WHEN $1 = 'COMPLETED' THEN NOW() ELSE completed_at END
      WHERE request_id = $3
    )";

    txn.exec_params(query, status, processedBy, requestId);
    txn.commit();

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Error updating request status: " + std::string(e.what()));
    return false;
  }
}

bool ComplianceManager::recordConsent(const ConsentRecord &consent) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      INSERT INTO metadata.consent_management (
        schema_name, table_name, data_subject_id, consent_type,
        consent_status, consent_given_at, legal_basis, purpose, retention_period
      ) VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7, $8)
      ON CONFLICT (schema_name, table_name, data_subject_id, consent_type)
      DO UPDATE SET
        consent_status = EXCLUDED.consent_status,
        consent_given_at = CASE WHEN EXCLUDED.consent_status = 'GIVEN' THEN NOW() ELSE consent_given_at END,
        consent_withdrawn_at = CASE WHEN EXCLUDED.consent_status = 'WITHDRAWN' THEN NOW() ELSE consent_withdrawn_at END,
        updated_at = NOW()
    )";

    txn.exec_params(query, consent.schema_name, consent.table_name,
                    consent.data_subject_id, consent.consent_type,
                    consent.consent_status, consent.legal_basis,
                    consent.purpose, consent.retention_period);

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "ComplianceManager",
                 "Consent recorded for subject: " + consent.data_subject_id);

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Error recording consent: " + std::string(e.what()));
    return false;
  }
}

bool ComplianceManager::withdrawConsent(const std::string &dataSubjectId,
                                        const std::string &tableName) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.consent_management
      SET consent_status = 'WITHDRAWN',
          consent_withdrawn_at = NOW(),
          updated_at = NOW()
      WHERE data_subject_id = $1 AND table_name = $2
    )";

    txn.exec_params(query, dataSubjectId, tableName);
    txn.commit();

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Error withdrawing consent: " + std::string(e.what()));
    return false;
  }
}

std::vector<ConsentRecord>
ComplianceManager::getConsentsForSubject(const std::string &dataSubjectId) {
  std::vector<ConsentRecord> consents;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT schema_name, table_name, data_subject_id, consent_type,
             consent_status, legal_basis, purpose, retention_period
      FROM metadata.consent_management
      WHERE data_subject_id = $1
    )";

    auto result = txn.exec_params(query, dataSubjectId);

    for (const auto &row : result) {
      ConsentRecord consent;
      consent.schema_name = row[0].as<std::string>();
      consent.table_name = row[1].as<std::string>();
      consent.data_subject_id = row[2].as<std::string>();
      consent.consent_type = row[3].as<std::string>();
      consent.consent_status = row[4].as<std::string>();
      consent.legal_basis = row[5].is_null() ? "" : row[5].as<std::string>();
      consent.purpose = row[6].is_null() ? "" : row[6].as<std::string>();
      consent.retention_period =
          row[7].is_null() ? "" : row[7].as<std::string>();

      consents.push_back(consent);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Error getting consents: " + std::string(e.what()));
  }

  return consents;
}

bool ComplianceManager::checkBreachNotification(const std::string &schemaName,
                                                const std::string &tableName) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT breach_notification_required, last_breach_check
      FROM metadata.data_governance_catalog
      WHERE schema_name = $1 AND table_name = $2
    )";

    auto result = txn.exec_params(query, schemaName, tableName);

    if (!result.empty()) {
      bool required = result[0][0].is_null() ? false : result[0][0].as<bool>();
      return required;
    }

    return false;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Error checking breach notification: " +
                      std::string(e.what()));
    return false;
  }
}

void ComplianceManager::logBreachCheck(const std::string &schemaName,
                                       const std::string &tableName) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.data_governance_catalog
      SET last_breach_check = NOW()
      WHERE schema_name = $1 AND table_name = $2
    )";

    txn.exec_params(query, schemaName, tableName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ComplianceManager",
                  "Error logging breach check: " + std::string(e.what()));
  }
}
