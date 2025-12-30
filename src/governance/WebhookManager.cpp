#include "governance/WebhookManager.h"
#include "core/logger.h"
#include "third_party/json.hpp"
#include <algorithm>
#include <ctime>
#include <curl/curl.h>
#include <sstream>
#include <thread>

WebhookManager::WebhookManager(const std::string &connectionString)
    : connectionString_(connectionString) {}

std::string WebhookManager::webhookTypeToString(WebhookType type) {
  switch (type) {
  case WebhookType::HTTP:
    return "HTTP";
  case WebhookType::SLACK:
    return "SLACK";
  case WebhookType::TEAMS:
    return "TEAMS";
  case WebhookType::TELEGRAM:
    return "TELEGRAM";
  case WebhookType::EMAIL:
    return "EMAIL";
  default:
    return "HTTP";
  }
}

WebhookType WebhookManager::stringToWebhookType(const std::string &str) {
  if (str == "HTTP")
    return WebhookType::HTTP;
  if (str == "SLACK")
    return WebhookType::SLACK;
  if (str == "TEAMS")
    return WebhookType::TEAMS;
  if (str == "TELEGRAM")
    return WebhookType::TELEGRAM;
  if (str == "EMAIL")
    return WebhookType::EMAIL;
  return WebhookType::HTTP;
}

std::string WebhookManager::eventTypeToString(EventType type) {
  switch (type) {
  case EventType::SYNC_COMPLETED:
    return "SYNC_COMPLETED";
  case EventType::SYNC_ERROR:
    return "SYNC_ERROR";
  case EventType::SYNC_STARTED:
    return "SYNC_STARTED";
  case EventType::ALERT_CREATED:
    return "ALERT_CREATED";
  case EventType::ALERT_RESOLVED:
    return "ALERT_RESOLVED";
  case EventType::DATA_QUALITY_ISSUE:
    return "DATA_QUALITY_ISSUE";
  case EventType::SCHEMA_CHANGE:
    return "SCHEMA_CHANGE";
  case EventType::PERFORMANCE_DEGRADED:
    return "PERFORMANCE_DEGRADED";
  case EventType::CUSTOM:
    return "CUSTOM";
  default:
    return "CUSTOM";
  }
}

EventType WebhookManager::stringToEventType(const std::string &str) {
  if (str == "SYNC_COMPLETED")
    return EventType::SYNC_COMPLETED;
  if (str == "SYNC_ERROR")
    return EventType::SYNC_ERROR;
  if (str == "SYNC_STARTED")
    return EventType::SYNC_STARTED;
  if (str == "ALERT_CREATED")
    return EventType::ALERT_CREATED;
  if (str == "ALERT_RESOLVED")
    return EventType::ALERT_RESOLVED;
  if (str == "DATA_QUALITY_ISSUE")
    return EventType::DATA_QUALITY_ISSUE;
  if (str == "SCHEMA_CHANGE")
    return EventType::SCHEMA_CHANGE;
  if (str == "PERFORMANCE_DEGRADED")
    return EventType::PERFORMANCE_DEGRADED;
  return EventType::CUSTOM;
}

std::string WebhookManager::severityToString(AlertSeverity severity) {
  switch (severity) {
  case AlertSeverity::INFO:
    return "INFO";
  case AlertSeverity::WARNING:
    return "WARNING";
  case AlertSeverity::CRITICAL:
    return "CRITICAL";
  case AlertSeverity::ERROR:
    return "ERROR";
  default:
    return "INFO";
  }
}

static size_t WriteCallback(void *contents, size_t size, size_t nmemb,
                            void *userp) {
  ((std::string *)userp)->append((char *)contents, size * nmemb);
  return size * nmemb;
}

bool WebhookManager::sendHTTPWebhook(const WebhookConfig &config,
                                     const json &payload) {
  CURL *curl = curl_easy_init();
  if (!curl) {
    Logger::error(LogCategory::GOVERNANCE, "WebhookManager",
                  "Failed to initialize CURL for HTTP webhook");
    return false;
  }

  std::string responseBody;
  struct curl_slist *headerList = nullptr;
  headerList = curl_slist_append(headerList, "Content-Type: application/json");

  if (!config.api_key.empty()) {
    std::string authHeader = "Authorization: Bearer " + config.api_key;
    headerList = curl_slist_append(headerList, authHeader.c_str());
  }

  std::string payloadStr = payload.dump();

  curl_easy_setopt(curl, CURLOPT_URL, config.url.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payloadStr.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, payloadStr.length());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerList);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

  CURLcode res = curl_easy_perform(curl);
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);

  curl_slist_free_all(headerList);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    Logger::error(LogCategory::GOVERNANCE, "WebhookManager",
                  "HTTP webhook failed: " +
                      std::string(curl_easy_strerror(res)));
    return false;
  }

  if (httpCode >= 200 && httpCode < 300) {
    Logger::info(LogCategory::GOVERNANCE, "WebhookManager",
                 "HTTP webhook sent successfully to " + config.url);
    return true;
  } else {
    Logger::warning(LogCategory::GOVERNANCE, "WebhookManager",
                    "HTTP webhook returned status " + std::to_string(httpCode) +
                        " for " + config.url);
    return false;
  }
}

bool WebhookManager::sendSlackWebhook(const WebhookConfig &config,
                                      const json &payload) {
  json slackPayload;
  slackPayload["text"] = payload.value("title", "DataSync Log");
  slackPayload["attachments"] = json::array();

  json attachment;
  std::string level = payload.value("level", payload.value("severity", "INFO"));
  attachment["color"] = level == "ERROR" || level == "CRITICAL" ? "danger"
                        : level == "WARNING"                    ? "warning"
                                                                : "good";
  attachment["fields"] = json::array();

  if (payload.contains("message")) {
    json field;
    field["title"] = "Message";
    field["value"] = payload["message"];
    field["short"] = false;
    attachment["fields"].push_back(field);
  }

  if (payload.contains("category")) {
    json field;
    field["title"] = "Category";
    field["value"] = payload["category"];
    field["short"] = true;
    attachment["fields"].push_back(field);
  }

  if (payload.contains("function")) {
    json field;
    field["title"] = "Function";
    field["value"] = payload["function"];
    field["short"] = true;
    attachment["fields"].push_back(field);
  }

  if (payload.contains("level")) {
    json field;
    field["title"] = "Level";
    field["value"] = payload["level"];
    field["short"] = true;
    attachment["fields"].push_back(field);
  }

  attachment["ts"] = std::time(nullptr);
  slackPayload["attachments"].push_back(attachment);

  return sendHTTPWebhook(config, slackPayload);
}

bool WebhookManager::sendTeamsWebhook(const WebhookConfig &config,
                                      const json &payload) {
  json teamsPayload;
  teamsPayload["@type"] = "MessageCard";
  teamsPayload["@context"] = "https://schema.org/extensions";
  teamsPayload["summary"] = payload.value("title", "DataSync Log");
  std::string level = payload.value("level", payload.value("severity", "INFO"));
  teamsPayload["themeColor"] = level == "ERROR" || level == "CRITICAL"
                                   ? "FF0000"
                               : level == "WARNING" ? "FFA500"
                                                    : "00FF00";

  teamsPayload["sections"] = json::array();
  json section;
  section["activityTitle"] = payload.value("title", "DataSync Log");
  section["text"] = payload.value("message", "");

  section["facts"] = json::array();
  if (payload.contains("category")) {
    json fact;
    fact["name"] = "Category";
    fact["value"] = payload["category"];
    section["facts"].push_back(fact);
  }
  if (payload.contains("level")) {
    json fact;
    fact["name"] = "Level";
    fact["value"] = payload["level"];
    section["facts"].push_back(fact);
  }
  if (payload.contains("function")) {
    json fact;
    fact["name"] = "Function";
    fact["value"] = payload["function"];
    section["facts"].push_back(fact);
  }

  teamsPayload["sections"].push_back(section);

  return sendHTTPWebhook(config, teamsPayload);
}

bool WebhookManager::sendTelegramWebhook(const WebhookConfig &config,
                                         const json &payload) {
  if (config.bot_token.empty() || config.chat_id.empty()) {
    Logger::error(LogCategory::GOVERNANCE, "WebhookManager",
                  "Telegram webhook missing bot_token or chat_id");
    return false;
  }

  std::string telegramUrl =
      "https://api.telegram.org/bot" + config.bot_token + "/sendMessage";

  std::string emoji = "ℹ️";
  std::string level = payload.value("level", payload.value("severity", "INFO"));
  if (level == "ERROR" || level == "CRITICAL") {
    emoji = "❌";
  } else if (level == "WARNING") {
    emoji = "⚠️";
  } else if (level == "DEBUG") {
    emoji = "🔍";
  }

  std::stringstream message;
  message << emoji << " *" << payload.value("title", "DataSync Log") << "*\n\n";
  message << payload.value("message", "");

  if (payload.contains("category")) {
    message << "\n\n📁 *Category:* " << payload.value("category", "");
  }

  if (payload.contains("function")) {
    message << "\n⚙️ *Function:* " << payload.value("function", "");
  }

  if (payload.contains("timestamp")) {
    message << "\n🕐 *Time:* " << payload.value("timestamp", "");
  }

  if (payload.contains("raw")) {
    message << "\n\n```\n" << payload.value("raw", "") << "\n```";
  }

  json telegramPayload;
  telegramPayload["chat_id"] = config.chat_id;
  telegramPayload["text"] = message.str();
  telegramPayload["parse_mode"] = "Markdown";

  WebhookConfig tempConfig = config;
  tempConfig.url = telegramUrl;
  return sendHTTPWebhook(tempConfig, telegramPayload);
}

bool WebhookManager::sendEmailWebhook(const WebhookConfig &config,
                                      const json &payload) {
  Logger::info(LogCategory::GOVERNANCE, "WebhookManager",
               "Email webhook would be sent to " + config.email_address + ": " +
                   payload.value("title", "Alert"));
  return true;
}

json WebhookManager::buildPayload(EventType eventType, const std::string &title,
                                  const std::string &message,
                                  AlertSeverity severity,
                                  const json &metadata) {
  json payload;
  payload["event_type"] = eventTypeToString(eventType);
  payload["title"] = title;
  payload["message"] = message;
  payload["severity"] = severityToString(severity);
  payload["timestamp"] = std::time(nullptr);

  if (metadata.contains("schema_name"))
    payload["schema_name"] = metadata["schema_name"];
  if (metadata.contains("table_name"))
    payload["table_name"] = metadata["table_name"];
  if (metadata.contains("db_engine"))
    payload["db_engine"] = metadata["db_engine"];
  if (metadata.contains("status"))
    payload["status"] = metadata["status"];
  if (metadata.contains("error_message"))
    payload["error_message"] = metadata["error_message"];

  return payload;
}

int WebhookManager::createWebhook(const WebhookConfig &config) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string logLevelsStr = "[";
    for (size_t i = 0; i < config.log_levels.size(); ++i) {
      if (i > 0)
        logLevelsStr += ",";
      logLevelsStr += "\"" + config.log_levels[i] + "\"";
    }
    logLevelsStr += "]";

    std::string logCategoriesStr = "[";
    for (size_t i = 0; i < config.log_categories.size(); ++i) {
      if (i > 0)
        logCategoriesStr += ",";
      logCategoriesStr += "\"" + config.log_categories[i] + "\"";
    }
    logCategoriesStr += "]";

    std::string query = R"(
      INSERT INTO metadata.webhooks (
        name, webhook_type, url, api_key, bot_token, chat_id, email_address,
        log_levels, log_categories, enabled
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10)
      RETURNING id
    )";

    auto result = txn.exec_params(
        query, config.name, webhookTypeToString(config.webhook_type),
        config.url, config.api_key, config.bot_token, config.chat_id,
        config.email_address, logLevelsStr, logCategoriesStr, config.enabled);

    int webhookId = -1;
    if (!result.empty()) {
      webhookId = result[0][0].as<int>();
    }

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "WebhookManager",
                 "Created webhook ID: " + std::to_string(webhookId) + " - " +
                     config.name);

    return webhookId;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "WebhookManager",
                  "Error creating webhook: " + std::string(e.what()));
    return -1;
  }
}

bool WebhookManager::updateWebhook(int webhookId, const WebhookConfig &config) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string logLevelsStr = "[";
    for (size_t i = 0; i < config.log_levels.size(); ++i) {
      if (i > 0)
        logLevelsStr += ",";
      logLevelsStr += "\"" + config.log_levels[i] + "\"";
    }
    logLevelsStr += "]";

    std::string logCategoriesStr = "[";
    for (size_t i = 0; i < config.log_categories.size(); ++i) {
      if (i > 0)
        logCategoriesStr += ",";
      logCategoriesStr += "\"" + config.log_categories[i] + "\"";
    }
    logCategoriesStr += "]";

    std::string query = R"(
      UPDATE metadata.webhooks
      SET name = $1, webhook_type = $2, url = $3, api_key = $4,
          bot_token = $5, chat_id = $6, email_address = $7,
          log_levels = $8::jsonb, log_categories = $9::jsonb, enabled = $10,
          updated_at = NOW()
      WHERE id = $11
    )";

    txn.exec_params(query, config.name,
                    webhookTypeToString(config.webhook_type), config.url,
                    config.api_key, config.bot_token, config.chat_id,
                    config.email_address, logLevelsStr, logCategoriesStr,
                    config.enabled, std::to_string(webhookId));

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "WebhookManager",
                 "Updated webhook ID: " + std::to_string(webhookId));

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "WebhookManager",
                  "Error updating webhook: " + std::string(e.what()));
    return false;
  }
}

bool WebhookManager::deleteWebhook(int webhookId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec_params("DELETE FROM metadata.webhooks WHERE id = $1",
                    std::to_string(webhookId));

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "WebhookManager",
                 "Deleted webhook ID: " + std::to_string(webhookId));

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "WebhookManager",
                  "Error deleting webhook: " + std::string(e.what()));
    return false;
  }
}

bool WebhookManager::enableWebhook(int webhookId, bool enabled) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec_params("UPDATE metadata.webhooks SET enabled = $1, updated_at = "
                    "NOW() WHERE id = $2",
                    enabled, std::to_string(webhookId));

    txn.commit();

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "WebhookManager",
                  "Error enabling/disabling webhook: " + std::string(e.what()));
    return false;
  }
}

std::vector<WebhookConfig> WebhookManager::getAllWebhooks() {
  std::vector<WebhookConfig> webhooks;
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    bool hasNewColumns = false;
    try {
      auto testResult =
          txn.exec("SELECT log_levels FROM metadata.webhooks LIMIT 1");
      hasNewColumns = true;
    } catch (...) {
      hasNewColumns = false;
    }

    pqxx::result result;
    if (hasNewColumns) {
      result = txn.exec(
          "SELECT id, name, webhook_type, url, api_key, bot_token, chat_id, "
          "email_address, log_levels, log_categories, enabled, created_at, "
          "updated_at "
          "FROM metadata.webhooks ORDER BY created_at DESC");
    } else {
      result = txn.exec(
          "SELECT id, name, webhook_type, url, api_key, bot_token, chat_id, "
          "email_address, event_types, severities, enabled, created_at, "
          "updated_at "
          "FROM metadata.webhooks ORDER BY created_at DESC");
    }

    for (const auto &row : result) {
      WebhookConfig config;
      config.id = row[0].as<int>();
      config.name = row[1].as<std::string>();
      config.webhook_type = stringToWebhookType(row[2].as<std::string>());
      config.url = row[3].is_null() ? "" : row[3].as<std::string>();
      config.api_key = row[4].is_null() ? "" : row[4].as<std::string>();
      config.bot_token = row[5].is_null() ? "" : row[5].as<std::string>();
      config.chat_id = row[6].is_null() ? "" : row[6].as<std::string>();
      config.email_address = row[7].is_null() ? "" : row[7].as<std::string>();

      if (hasNewColumns) {
        if (!row[8].is_null()) {
          json logLevelsJson = json::parse(row[8].as<std::string>());
          for (const auto &level : logLevelsJson) {
            config.log_levels.push_back(level.get<std::string>());
          }
        }

        if (!row[9].is_null()) {
          json logCategoriesJson = json::parse(row[9].as<std::string>());
          for (const auto &cat : logCategoriesJson) {
            config.log_categories.push_back(cat.get<std::string>());
          }
        }
        config.enabled = row[10].as<bool>();
        config.created_at = row[11].is_null() ? "" : row[11].as<std::string>();
        config.updated_at = row[12].is_null() ? "" : row[12].as<std::string>();
      } else {
        config.log_levels.clear();
        config.log_categories.clear();
        config.enabled = row[10].as<bool>();
        config.created_at = row[11].is_null() ? "" : row[11].as<std::string>();
        config.updated_at = row[12].is_null() ? "" : row[12].as<std::string>();
      }

      webhooks.push_back(config);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "WebhookManager",
                  "Error getting webhooks: " + std::string(e.what()));
  }

  return webhooks;
}

std::vector<WebhookConfig> WebhookManager::getActiveWebhooks() {
  auto allWebhooks = getAllWebhooks();
  std::vector<WebhookConfig> active;
  for (const auto &webhook : allWebhooks) {
    if (webhook.enabled) {
      active.push_back(webhook);
    }
  }
  return active;
}

void WebhookManager::triggerWebhook(EventType eventType,
                                    const std::string &title,
                                    const std::string &message,
                                    AlertSeverity severity,
                                    const json &metadata) {}

void WebhookManager::triggerSyncEvent(EventType eventType,
                                      const std::string &schemaName,
                                      const std::string &tableName,
                                      const std::string &dbEngine,
                                      const std::string &status,
                                      const std::string &errorMessage) {}

void WebhookManager::triggerAlertEvent(const Alert &alert) {}

void WebhookManager::createWebhookTable() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec("CREATE SCHEMA IF NOT EXISTS metadata");

    std::string createTableSQL = R"(
      CREATE TABLE IF NOT EXISTS metadata.webhooks (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        webhook_type VARCHAR(20) NOT NULL CHECK (webhook_type IN ('HTTP', 'SLACK', 'TEAMS', 'TELEGRAM', 'EMAIL')),
        url TEXT,
        api_key TEXT,
        bot_token TEXT,
        chat_id VARCHAR(255),
        email_address VARCHAR(255),
        log_levels JSONB NOT NULL DEFAULT '[]'::jsonb,
        log_categories JSONB NOT NULL DEFAULT '[]'::jsonb,
        enabled BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    )";

    txn.exec(createTableSQL);

    try {
      bool hasLogLevels = false;
      try {
        auto testResult =
            txn.exec("SELECT log_levels FROM metadata.webhooks LIMIT 1");
        hasLogLevels = true;
      } catch (...) {
        hasLogLevels = false;
      }

      if (!hasLogLevels) {
        txn.exec("ALTER TABLE metadata.webhooks ADD COLUMN IF NOT EXISTS "
                 "log_levels JSONB DEFAULT '[]'::jsonb");
        txn.exec("ALTER TABLE metadata.webhooks ADD COLUMN IF NOT EXISTS "
                 "log_categories JSONB DEFAULT '[]'::jsonb");
        Logger::info(
            LogCategory::GOVERNANCE, "WebhookManager",
            "Added log_levels and log_categories columns to webhooks table");
      }
    } catch (const std::exception &e) {
      Logger::warning(LogCategory::GOVERNANCE, "WebhookManager",
                      "Could not migrate webhooks table: " +
                          std::string(e.what()));
    }

    txn.exec("CREATE INDEX IF NOT EXISTS idx_webhooks_enabled ON "
             "metadata.webhooks(enabled)");
    txn.exec("CREATE INDEX IF NOT EXISTS idx_webhooks_type ON "
             "metadata.webhooks(webhook_type)");

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "WebhookManager",
                 "Webhook table created successfully");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "WebhookManager",
                  "Error creating webhook table: " + std::string(e.what()));
  }
}

json WebhookManager::buildLogPayload(const std::string &timestamp,
                                     const std::string &level,
                                     const std::string &category,
                                     const std::string &function,
                                     const std::string &message) {
  json payload;
  payload["timestamp"] = timestamp;
  payload["level"] = level;
  payload["category"] = category;
  payload["function"] = function;
  payload["message"] = message;
  payload["title"] = "[" + level + "] " + category;
  payload["raw"] = "[" + timestamp + "] [" + level + "] [" + category + "] [" +
                   function + "] " + message;
  return payload;
}

void WebhookManager::processLogEntry(const std::string &timestamp,
                                     const std::string &level,
                                     const std::string &category,
                                     const std::string &function,
                                     const std::string &message) {
  auto webhooks = getActiveWebhooks();
  if (webhooks.empty())
    return;

  json payload = buildLogPayload(timestamp, level, category, function, message);

  for (const auto &webhook : webhooks) {
    bool levelMatch = false;
    if (webhook.log_levels.empty()) {
      levelMatch = true;
    } else {
      for (const auto &wl : webhook.log_levels) {
        if (wl == level) {
          levelMatch = true;
          break;
        }
      }
    }

    if (!levelMatch)
      continue;

    bool categoryMatch = false;
    if (webhook.log_categories.empty()) {
      categoryMatch = true;
    } else {
      for (const auto &wc : webhook.log_categories) {
        if (wc == category) {
          categoryMatch = true;
          break;
        }
      }
    }

    if (!categoryMatch)
      continue;

    bool success = false;
    switch (webhook.webhook_type) {
    case WebhookType::HTTP:
      success = sendHTTPWebhook(webhook, payload);
      break;
    case WebhookType::SLACK:
      success = sendSlackWebhook(webhook, payload);
      break;
    case WebhookType::TEAMS:
      success = sendTeamsWebhook(webhook, payload);
      break;
    case WebhookType::TELEGRAM:
      success = sendTelegramWebhook(webhook, payload);
      break;
    case WebhookType::EMAIL:
      success = sendEmailWebhook(webhook, payload);
      break;
    }

    if (!success) {
      Logger::warning(LogCategory::GOVERNANCE, "WebhookManager",
                      "Failed to send webhook for log: " + webhook.name);
    }
  }
}

void WebhookManager::monitorLogsAndTriggerWebhooks() {
  static std::string lastTimestamp = "";
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query;
    if (lastTimestamp.empty()) {
      query = "SELECT ts, level, category, function, message "
              "FROM metadata.logs "
              "WHERE ts > NOW() - INTERVAL '10 seconds' "
              "ORDER BY ts DESC";
    } else {
      query = "SELECT ts, level, category, function, message "
              "FROM metadata.logs "
              "WHERE ts > $1::timestamp "
              "ORDER BY ts DESC";
    }

    pqxx::result result;
    if (lastTimestamp.empty()) {
      result = txn.exec(query);
    } else {
      result = txn.exec_params(query, lastTimestamp);
    }

    std::string maxTimestamp = lastTimestamp;

    for (const auto &row : result) {
      std::string timestamp = row[0].is_null() ? "" : row[0].as<std::string>();
      std::string level = row[1].is_null() ? "" : row[1].as<std::string>();
      std::string category = row[2].is_null() ? "" : row[2].as<std::string>();
      std::string function = row[3].is_null() ? "" : row[3].as<std::string>();
      std::string message = row[4].is_null() ? "" : row[4].as<std::string>();

      if (!timestamp.empty() && !level.empty() && !category.empty()) {
        if (timestamp > maxTimestamp || maxTimestamp.empty()) {
          maxTimestamp = timestamp;
        }
        processLogEntry(timestamp, level, category, function, message);
      }
    }

    if (!maxTimestamp.empty()) {
      lastTimestamp = maxTimestamp;
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "WebhookManager",
                  "Error monitoring logs: " + std::string(e.what()));
  }
}
