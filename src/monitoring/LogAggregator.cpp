#include "monitoring/LogAggregator.h"
#include "core/logger.h"
#include <curl/curl.h>
#include <pqxx/pqxx>
#include <sstream>
#include <iomanip>
#include <ctime>

LogAggregator::LogAggregator(const std::string& connectionString)
    : connectionString_(connectionString) {
  // Tables are created in migration
}

bool LogAggregator::configure(const AggregationConfig& config) {
  return saveConfigToDatabase(config);
}

bool LogAggregator::saveConfigToDatabase(const AggregationConfig& config) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    json severityMappingJson = json::object();
    // Config doesn't have severity mapping, but we keep the structure

    txn.exec_params(
        "INSERT INTO metadata.log_aggregation_config "
        "(config_id, type, endpoint, index_name, token, username, password, enabled, batch_size, "
        "batch_interval_seconds) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) "
        "ON CONFLICT (config_id) DO UPDATE SET "
        "endpoint = EXCLUDED.endpoint, index_name = EXCLUDED.index_name, "
        "token = EXCLUDED.token, username = EXCLUDED.username, password = EXCLUDED.password, "
        "enabled = EXCLUDED.enabled, batch_size = EXCLUDED.batch_size, "
        "batch_interval_seconds = EXCLUDED.batch_interval_seconds",
        config.id, config.type, config.endpoint.empty() ? nullptr : config.endpoint,
        config.index.empty() ? nullptr : config.index, config.token.empty() ? nullptr : config.token,
        config.username.empty() ? nullptr : config.username,
        config.password.empty() ? nullptr : config.password, config.enabled, config.batchSize,
        config.batchIntervalSeconds);

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "LogAggregator",
                  "Error saving config: " + std::string(e.what()));
    return false;
  }
}

std::unique_ptr<LogAggregator::AggregationConfig> LogAggregator::getConfig(
    const std::string& configId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params("SELECT * FROM metadata.log_aggregation_config WHERE config_id = $1",
                                   configId);

    if (result.empty()) {
      return nullptr;
    }

    auto row = result[0];
    auto config = std::make_unique<AggregationConfig>();
    config->id = row["config_id"].as<std::string>();
    config->type = row["type"].as<std::string>();
    if (!row["endpoint"].is_null()) {
      config->endpoint = row["endpoint"].as<std::string>();
    }
    if (!row["index_name"].is_null()) {
      config->index = row["index_name"].as<std::string>();
    }
    if (!row["token"].is_null()) {
      config->token = row["token"].as<std::string>();
    }
    if (!row["username"].is_null()) {
      config->username = row["username"].as<std::string>();
    }
    if (!row["password"].is_null()) {
      config->password = row["password"].as<std::string>();
    }
    config->enabled = row["enabled"].as<bool>();
    config->batchSize = row["batch_size"].as<int>();
    config->batchIntervalSeconds = row["batch_interval_seconds"].as<int>();

    return config;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "LogAggregator",
                  "Error loading config: " + std::string(e.what()));
    return nullptr;
  }
}

std::vector<LogAggregator::AggregationConfig> LogAggregator::listConfigs() {
  std::vector<AggregationConfig> configs;
  // TODO: Implement
  return configs;
}

int LogAggregator::exportLogs(const std::string& configId, int limit) {
  auto config = getConfig(configId);
  if (!config || !config->enabled) {
    return 0;
  }

  auto logs = fetchLogsFromDatabase(limit);
  if (logs.empty()) {
    return 0;
  }

  bool success = false;
  if (config->type == "elasticsearch") {
    success = exportToElasticsearch(*config, logs);
  } else if (config->type == "splunk") {
    success = exportToSplunk(*config, logs);
  }

  if (success) {
    AggregationStatus status;
    status.configId = configId;
    status.lastExport = std::chrono::system_clock::now();
    status.logsExported = logs.size();
    status.logsFailed = 0;
    status.isActive = true;
    saveStatusToDatabase(status);
    return logs.size();
  }

  return 0;
}

std::vector<json> LogAggregator::fetchLogsFromDatabase(int limit) {
  std::vector<json> logs;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params(
        "SELECT * FROM metadata.system_logs ORDER BY timestamp DESC LIMIT $1", limit);

    for (const auto& row : result) {
      json log;
      log["timestamp"] = row["timestamp"].as<std::string>();
      log["level"] = row["level"].as<std::string>();
      log["category"] = row["category"].as<std::string>();
      log["message"] = row["message"].as<std::string>();
      if (!row["trace_id"].is_null()) {
        log["trace_id"] = row["trace_id"].as<std::string>();
      }
      if (!row["user_id"].is_null()) {
        log["user_id"] = row["user_id"].as<std::string>();
      }

      logs.push_back(formatLogForExport(log));
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "LogAggregator",
                  "Error fetching logs: " + std::string(e.what()));
  }

  return logs;
}

json LogAggregator::formatLogForExport(const json& log) {
  return log; // Already formatted
}

bool LogAggregator::exportToElasticsearch(const AggregationConfig& config,
                                          const std::vector<json>& logs) {
  if (config.endpoint.empty() || config.index.empty()) {
    return false;
  }

  json bulkPayload;
  for (const auto& log : logs) {
    json action;
    action["index"]["_index"] = config.index;
    bulkPayload.push_back(action.dump() + "\n" + log.dump() + "\n");
  }

  std::string url = config.endpoint + "/_bulk";
  std::string payload = "";
  for (const auto& line : bulkPayload) {
    payload += line.get<std::string>();
  }

  CURL* curl = curl_easy_init();
  if (!curl) {
    return false;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());

  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  if (!config.username.empty() && !config.password.empty()) {
    std::string auth = config.username + ":" + config.password;
    curl_easy_setopt(curl, CURLOPT_USERPWD, auth.c_str());
  }
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode res = curl_easy_perform(curl);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  return res == CURLE_OK;
}

bool LogAggregator::exportToSplunk(const AggregationConfig& config,
                                   const std::vector<json>& logs) {
  if (config.endpoint.empty() || config.token.empty()) {
    return false;
  }

  json events = json::array();
  for (const auto& log : logs) {
    events.push_back(log);
  }

  std::string url = config.endpoint + "/services/collector/event";
  std::string payload = events.dump();

  CURL* curl = curl_easy_init();
  if (!curl) {
    return false;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());

  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  std::string authHeader = "Authorization: Splunk " + config.token;
  headers = curl_slist_append(headers, authHeader.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode res = curl_easy_perform(curl);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  return res == CURLE_OK;
}

std::unique_ptr<LogAggregator::AggregationStatus> LogAggregator::getStatus(
    const std::string& configId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params("SELECT * FROM metadata.log_aggregation_status WHERE config_id = $1",
                                   configId);

    if (result.empty()) {
      return nullptr;
    }

    auto row = result[0];
    auto status = std::make_unique<AggregationStatus>();
    status->configId = row["config_id"].as<std::string>();
    if (!row["last_export"].is_null()) {
      status->lastExport = std::chrono::system_clock::from_time_t(
          std::chrono::seconds(row["last_export"].as<int64_t>()).count());
    }
    status->logsExported = row["logs_exported"].as<int64_t>();
    status->logsFailed = row["logs_failed"].as<int64_t>();
    if (!row["last_error"].is_null()) {
      status->lastError = row["last_error"].as<std::string>();
    }
    status->isActive = row["is_active"].as<bool>();

    return status;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "LogAggregator",
                  "Error loading status: " + std::string(e.what()));
    return nullptr;
  }
}

bool LogAggregator::saveStatusToDatabase(const AggregationStatus& status) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto lastExportT = std::chrono::system_clock::to_time_t(status.lastExport);
    std::tm lastExportTm = *std::localtime(&lastExportT);
    std::ostringstream lastExportStr;
    lastExportStr << std::put_time(&lastExportTm, "%Y-%m-%d %H:%M:%S");

    txn.exec_params(
        "INSERT INTO metadata.log_aggregation_status "
        "(config_id, last_export, logs_exported, logs_failed, last_error, is_active) "
        "VALUES ($1, $2::timestamp, $3, $4, $5, $6) "
        "ON CONFLICT (config_id) DO UPDATE SET "
        "last_export = EXCLUDED.last_export, logs_exported = EXCLUDED.logs_exported, "
        "logs_failed = EXCLUDED.logs_failed, last_error = EXCLUDED.last_error, "
        "is_active = EXCLUDED.is_active",
        status.configId, lastExportStr.str(), status.logsExported, status.logsFailed,
        status.lastError.empty() ? nullptr : status.lastError, status.isActive);

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "LogAggregator",
                  "Error saving status: " + std::string(e.what()));
    return false;
  }
}
