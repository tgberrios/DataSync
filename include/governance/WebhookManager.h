#ifndef WEBHOOK_MANAGER_H
#define WEBHOOK_MANAGER_H

#include "governance/AlertingManager.h"
#include "third_party/json.hpp"
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;

enum class WebhookType { HTTP, SLACK, TEAMS, TELEGRAM, EMAIL };

enum class EventType {
  SYNC_COMPLETED,
  SYNC_ERROR,
  SYNC_STARTED,
  ALERT_CREATED,
  ALERT_RESOLVED,
  DATA_QUALITY_ISSUE,
  SCHEMA_CHANGE,
  PERFORMANCE_DEGRADED,
  CUSTOM
};

struct WebhookConfig {
  int id;
  std::string name;
  WebhookType webhook_type;
  std::string url;
  std::string api_key;
  std::string bot_token;
  std::string chat_id;
  std::string email_address;
  std::vector<std::string> log_levels;
  std::vector<std::string> log_categories;
  bool enabled;
  std::string created_at;
  std::string updated_at;
};

class WebhookManager {
private:
  std::string connectionString_;

  std::string webhookTypeToString(WebhookType type);
  WebhookType stringToWebhookType(const std::string &str);
  std::string eventTypeToString(EventType type);
  EventType stringToEventType(const std::string &str);
  std::string severityToString(AlertSeverity severity);

  bool sendHTTPWebhook(const WebhookConfig &config, const json &payload);
  bool sendSlackWebhook(const WebhookConfig &config, const json &payload);
  bool sendTeamsWebhook(const WebhookConfig &config, const json &payload);
  bool sendTelegramWebhook(const WebhookConfig &config, const json &payload);
  bool sendEmailWebhook(const WebhookConfig &config, const json &payload);

  json buildPayload(EventType eventType, const std::string &title,
                    const std::string &message,
                    AlertSeverity severity = AlertSeverity::INFO,
                    const json &metadata = json::object());
  json buildLogPayload(const std::string &timestamp, const std::string &level,
                       const std::string &category, const std::string &function,
                       const std::string &message);

public:
  explicit WebhookManager(const std::string &connectionString);
  ~WebhookManager() = default;

  int createWebhook(const WebhookConfig &config);
  bool updateWebhook(int webhookId, const WebhookConfig &config);
  bool deleteWebhook(int webhookId);
  bool enableWebhook(int webhookId, bool enabled);
  std::vector<WebhookConfig> getAllWebhooks();
  std::vector<WebhookConfig> getActiveWebhooks();

  void triggerWebhook(EventType eventType, const std::string &title,
                      const std::string &message,
                      AlertSeverity severity = AlertSeverity::INFO,
                      const json &metadata = json::object());

  void triggerSyncEvent(EventType eventType, const std::string &schemaName,
                        const std::string &tableName,
                        const std::string &dbEngine, const std::string &status,
                        const std::string &errorMessage = "");

  void triggerAlertEvent(const Alert &alert);

  void monitorLogsAndTriggerWebhooks();
  void processLogEntry(const std::string &timestamp, const std::string &level,
                       const std::string &category, const std::string &function,
                       const std::string &message);

  void createWebhookTable();
};

#endif
