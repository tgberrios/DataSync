#include "governance/AlertingManager.h"
#include "core/logger.h"
#include "governance/WebhookManager.h"
#include "third_party/json.hpp"
#include <algorithm>
#include <ctime>
#include <pqxx/pqxx>
#include <sstream>

using json = nlohmann::json;

AlertingManager::AlertingManager(const std::string &connectionString)
    : connectionString_(connectionString) {}

std::string AlertingManager::alertTypeToString(AlertType type) {
  switch (type) {
  case AlertType::DATA_QUALITY_DEGRADED:
    return "DATA_QUALITY_DEGRADED";
  case AlertType::PII_DETECTED:
    return "PII_DETECTED";
  case AlertType::ACCESS_ANOMALY:
    return "ACCESS_ANOMALY";
  case AlertType::RETENTION_EXPIRED:
    return "RETENTION_EXPIRED";
  case AlertType::SCHEMA_CHANGE:
    return "SCHEMA_CHANGE";
  case AlertType::DATA_FRESHNESS:
    return "DATA_FRESHNESS";
  case AlertType::PERFORMANCE_DEGRADED:
    return "PERFORMANCE_DEGRADED";
  case AlertType::COMPLIANCE_VIOLATION:
    return "COMPLIANCE_VIOLATION";
  case AlertType::SECURITY_BREACH:
    return "SECURITY_BREACH";
  case AlertType::CUSTOM:
    return "CUSTOM";
  default:
    return "UNKNOWN";
  }
}

std::string AlertingManager::severityToString(AlertSeverity severity) {
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

AlertType AlertingManager::stringToAlertType(const std::string &str) {
  if (str == "DATA_QUALITY_DEGRADED")
    return AlertType::DATA_QUALITY_DEGRADED;
  if (str == "PII_DETECTED")
    return AlertType::PII_DETECTED;
  if (str == "ACCESS_ANOMALY")
    return AlertType::ACCESS_ANOMALY;
  if (str == "RETENTION_EXPIRED")
    return AlertType::RETENTION_EXPIRED;
  if (str == "SCHEMA_CHANGE")
    return AlertType::SCHEMA_CHANGE;
  if (str == "DATA_FRESHNESS")
    return AlertType::DATA_FRESHNESS;
  if (str == "PERFORMANCE_DEGRADED")
    return AlertType::PERFORMANCE_DEGRADED;
  if (str == "COMPLIANCE_VIOLATION")
    return AlertType::COMPLIANCE_VIOLATION;
  if (str == "SECURITY_BREACH")
    return AlertType::SECURITY_BREACH;
  return AlertType::CUSTOM;
}

AlertSeverity AlertingManager::stringToSeverity(const std::string &str) {
  if (str == "INFO")
    return AlertSeverity::INFO;
  if (str == "WARNING")
    return AlertSeverity::WARNING;
  if (str == "CRITICAL")
    return AlertSeverity::CRITICAL;
  if (str == "ERROR")
    return AlertSeverity::ERROR;
  return AlertSeverity::INFO;
}

int AlertingManager::createAlert(const Alert &alert) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      INSERT INTO metadata.alerts (
        alert_type, severity, title, message, schema_name, table_name,
        column_name, source, status, metadata_json
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
      RETURNING id
    )";

    std::string metadataStr =
        alert.metadata_json.empty() ? "{}" : alert.metadata_json;

    auto result = txn.exec_params(query, alertTypeToString(alert.alert_type),
                                  severityToString(alert.severity), alert.title,
                                  alert.message, alert.schema_name,
                                  alert.table_name, alert.column_name,
                                  alert.source, alert.status, metadataStr);

    int alertId = -1;
    if (!result.empty()) {
      alertId = result[0][0].as<int>();
    }

    txn.commit();

    if (alertId > 0) {
      sendNotification(alert, "");
      Logger::info(LogCategory::GOVERNANCE, "AlertingManager",
                   "Created alert ID: " + std::to_string(alertId) + " - " +
                       alert.title);
    }

    return alertId;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error creating alert: " + std::string(e.what()));
    return -1;
  }
}

bool AlertingManager::updateAlertStatus(int alertId, const std::string &status,
                                        const std::string &assignedTo) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.alerts
      SET status = $1,
          assigned_to = CASE WHEN $2 != '' THEN $2 ELSE assigned_to END,
          updated_at = NOW()
      WHERE id = $3
    )";

    txn.exec_params(query, status, assignedTo, std::to_string(alertId));
    txn.commit();

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error updating alert status: " + std::string(e.what()));
    return false;
  }
}

bool AlertingManager::resolveAlert(int alertId, const std::string &resolvedBy) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.alerts
      SET status = 'RESOLVED',
          resolved_at = NOW(),
          assigned_to = CASE WHEN $1 != '' THEN $1 ELSE assigned_to END,
          updated_at = NOW()
      WHERE id = $2
    )";

    txn.exec_params(query, resolvedBy, std::to_string(alertId));
    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "AlertingManager",
                 "Resolved alert ID: " + std::to_string(alertId));

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error resolving alert: " + std::string(e.what()));
    return false;
  }
}

std::vector<Alert> AlertingManager::getActiveAlerts(const std::string &severity,
                                                    int limit) {
  std::vector<Alert> alerts;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, alert_type, severity, title, message, schema_name, table_name,
             column_name, source, status, assigned_to, resolved_at,
             metadata_json::text, created_at, updated_at
      FROM metadata.alerts
      WHERE status = 'OPEN'
    )";

    std::vector<std::string> params;
    if (!severity.empty()) {
      query += " AND severity = $1";
      params.push_back(severity);
    }

    query +=
        " ORDER BY created_at DESC LIMIT $" + std::to_string(params.size() + 1);
    params.push_back(std::to_string(limit));

    pqxx::params pqParams;
    for (const auto &p : params) {
      pqParams.append(p);
    }

    auto result = txn.exec_params(query, pqParams);

    for (const auto &row : result) {
      Alert alert;
      alert.id = row[0].as<int>();
      alert.alert_type = stringToAlertType(row[1].as<std::string>());
      alert.severity = stringToSeverity(row[2].as<std::string>());
      alert.title = row[3].as<std::string>();
      alert.message = row[4].as<std::string>();
      alert.schema_name = row[5].is_null() ? "" : row[5].as<std::string>();
      alert.table_name = row[6].is_null() ? "" : row[6].as<std::string>();
      alert.column_name = row[7].is_null() ? "" : row[7].as<std::string>();
      alert.source = row[8].is_null() ? "" : row[8].as<std::string>();
      alert.status = row[9].as<std::string>();
      alert.assigned_to = row[10].is_null() ? "" : row[10].as<std::string>();
      alert.resolved_at = row[11].is_null() ? "" : row[11].as<std::string>();
      alert.metadata_json =
          row[12].is_null() ? "{}" : row[12].as<std::string>();
      alert.created_at = row[13].is_null() ? "" : row[13].as<std::string>();
      alert.updated_at = row[14].is_null() ? "" : row[14].as<std::string>();

      alerts.push_back(alert);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error getting active alerts: " + std::string(e.what()));
  }

  return alerts;
}

std::vector<Alert> AlertingManager::getAlertsByType(AlertType type, int days) {
  std::vector<Alert> alerts;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, alert_type, severity, title, message, schema_name, table_name,
             column_name, source, status, assigned_to, resolved_at,
             metadata_json::text, created_at, updated_at
      FROM metadata.alerts
      WHERE alert_type = $1
        AND created_at >= NOW() - INTERVAL '1 day' * $2
      ORDER BY created_at DESC
    )";

    auto result =
        txn.exec_params(query, alertTypeToString(type), std::to_string(days));

    for (const auto &row : result) {
      Alert alert;
      alert.id = row[0].as<int>();
      alert.alert_type = stringToAlertType(row[1].as<std::string>());
      alert.severity = stringToSeverity(row[2].as<std::string>());
      alert.title = row[3].as<std::string>();
      alert.message = row[4].as<std::string>();
      alert.schema_name = row[5].is_null() ? "" : row[5].as<std::string>();
      alert.table_name = row[6].is_null() ? "" : row[6].as<std::string>();
      alert.column_name = row[7].is_null() ? "" : row[7].as<std::string>();
      alert.source = row[8].is_null() ? "" : row[8].as<std::string>();
      alert.status = row[9].as<std::string>();
      alert.assigned_to = row[10].is_null() ? "" : row[10].as<std::string>();
      alert.resolved_at = row[11].is_null() ? "" : row[11].as<std::string>();
      alert.metadata_json =
          row[12].is_null() ? "{}" : row[12].as<std::string>();
      alert.created_at = row[13].is_null() ? "" : row[13].as<std::string>();
      alert.updated_at = row[14].is_null() ? "" : row[14].as<std::string>();

      alerts.push_back(alert);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error getting alerts by type: " + std::string(e.what()));
  }

  return alerts;
}

std::vector<Alert>
AlertingManager::getAlertsForTable(const std::string &schemaName,
                                   const std::string &tableName) {
  std::vector<Alert> alerts;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, alert_type, severity, title, message, schema_name, table_name,
             column_name, source, status, assigned_to, resolved_at,
             metadata_json::text, created_at, updated_at
      FROM metadata.alerts
      WHERE schema_name = $1 AND table_name = $2
      ORDER BY created_at DESC
    )";

    auto result = txn.exec_params(query, schemaName, tableName);

    for (const auto &row : result) {
      Alert alert;
      alert.id = row[0].as<int>();
      alert.alert_type = stringToAlertType(row[1].as<std::string>());
      alert.severity = stringToSeverity(row[2].as<std::string>());
      alert.title = row[3].as<std::string>();
      alert.message = row[4].as<std::string>();
      alert.schema_name = row[5].is_null() ? "" : row[5].as<std::string>();
      alert.table_name = row[6].is_null() ? "" : row[6].as<std::string>();
      alert.column_name = row[7].is_null() ? "" : row[7].as<std::string>();
      alert.source = row[8].is_null() ? "" : row[8].as<std::string>();
      alert.status = row[9].as<std::string>();
      alert.assigned_to = row[10].is_null() ? "" : row[10].as<std::string>();
      alert.resolved_at = row[11].is_null() ? "" : row[11].as<std::string>();
      alert.metadata_json =
          row[12].is_null() ? "{}" : row[12].as<std::string>();
      alert.created_at = row[13].is_null() ? "" : row[13].as<std::string>();
      alert.updated_at = row[14].is_null() ? "" : row[14].as<std::string>();

      alerts.push_back(alert);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error getting alerts for table: " + std::string(e.what()));
  }

  return alerts;
}

bool AlertingManager::addAlertRule(const AlertRule &rule) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      INSERT INTO metadata.alert_rules (
        rule_name, alert_type, severity, condition_expression,
        threshold_value, enabled, notification_channels
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (rule_name) DO UPDATE SET
        alert_type = EXCLUDED.alert_type,
        severity = EXCLUDED.severity,
        condition_expression = EXCLUDED.condition_expression,
        threshold_value = EXCLUDED.threshold_value,
        enabled = EXCLUDED.enabled,
        notification_channels = EXCLUDED.notification_channels,
        updated_at = NOW()
    )";

    txn.exec_params(query, rule.rule_name, alertTypeToString(rule.alert_type),
                    severityToString(rule.severity), rule.condition,
                    rule.threshold_value, rule.enabled,
                    rule.notification_channels);

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "AlertingManager",
                 "Added/updated alert rule: " + rule.rule_name);

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error adding alert rule: " + std::string(e.what()));
    return false;
  }
}

bool AlertingManager::updateAlertRule(int ruleId, const AlertRule &rule) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.alert_rules
      SET rule_name = $1,
          alert_type = $2,
          severity = $3,
          condition_expression = $4,
          threshold_value = $5,
          enabled = $6,
          notification_channels = $7,
          updated_at = NOW()
      WHERE id = $8
    )";

    txn.exec_params(query, rule.rule_name, alertTypeToString(rule.alert_type),
                    severityToString(rule.severity), rule.condition,
                    rule.threshold_value, rule.enabled,
                    rule.notification_channels, std::to_string(ruleId));

    txn.commit();

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error updating alert rule: " + std::string(e.what()));
    return false;
  }
}

bool AlertingManager::deleteAlertRule(int ruleId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = "DELETE FROM metadata.alert_rules WHERE id = $1";
    txn.exec_params(query, std::to_string(ruleId));

    txn.commit();

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error deleting alert rule: " + std::string(e.what()));
    return false;
  }
}

bool AlertingManager::enableAlertRule(int ruleId, bool enabled) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.alert_rules
      SET enabled = $1, updated_at = NOW()
      WHERE id = $2
    )";

    txn.exec_params(query, enabled, std::to_string(ruleId));

    txn.commit();

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error enabling/disabling alert rule: " +
                      std::string(e.what()));
    return false;
  }
}

std::vector<AlertRule> AlertingManager::getAllRules() {
  std::vector<AlertRule> rules;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, rule_name, alert_type, severity, condition_expression,
             threshold_value, enabled, notification_channels, created_at, updated_at
      FROM metadata.alert_rules
      ORDER BY rule_name ASC
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      AlertRule rule;
      rule.id = row[0].as<int>();
      rule.rule_name = row[1].as<std::string>();
      rule.alert_type = stringToAlertType(row[2].as<std::string>());
      rule.severity = stringToSeverity(row[3].as<std::string>());
      rule.condition = row[4].as<std::string>();
      rule.threshold_value = row[5].is_null() ? "" : row[5].as<std::string>();
      rule.enabled = row[6].as<bool>();
      rule.notification_channels =
          row[7].is_null() ? "" : row[7].as<std::string>();
      rule.created_at = row[8].is_null() ? "" : row[8].as<std::string>();
      rule.updated_at = row[9].is_null() ? "" : row[9].as<std::string>();

      rules.push_back(rule);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error getting all rules: " + std::string(e.what()));
  }

  return rules;
}

std::vector<AlertRule> AlertingManager::getActiveRules() {
  std::vector<AlertRule> rules;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, rule_name, alert_type, severity, condition_expression,
             threshold_value, enabled, notification_channels, created_at, updated_at
      FROM metadata.alert_rules
      WHERE enabled = true
      ORDER BY rule_name ASC
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      AlertRule rule;
      rule.id = row[0].as<int>();
      rule.rule_name = row[1].as<std::string>();
      rule.alert_type = stringToAlertType(row[2].as<std::string>());
      rule.severity = stringToSeverity(row[3].as<std::string>());
      rule.condition = row[4].as<std::string>();
      rule.threshold_value = row[5].is_null() ? "" : row[5].as<std::string>();
      rule.enabled = row[6].as<bool>();
      rule.notification_channels =
          row[7].is_null() ? "" : row[7].as<std::string>();
      rule.created_at = row[8].is_null() ? "" : row[8].as<std::string>();
      rule.updated_at = row[9].is_null() ? "" : row[9].as<std::string>();

      rules.push_back(rule);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error getting active rules: " + std::string(e.what()));
  }

  return rules;
}

bool AlertingManager::evaluateRule(const AlertRule &rule) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = rule.condition;
    auto result = txn.exec(query);

    return !result.empty() && result[0][0].as<bool>();
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::GOVERNANCE, "AlertingManager",
                    "Error evaluating rule " + rule.rule_name + ": " +
                        std::string(e.what()));
    return false;
  }
}

void AlertingManager::sendNotification(const Alert &alert,
                                       const std::string &channels) {
  std::string message = buildAlertMessage(alert);

  Logger::warning(LogCategory::GOVERNANCE, "AlertingManager",
                  "ALERT [" + severityToString(alert.severity) +
                      "]: " + message);

  triggerWebhooks(alert);

  if (channels.find("email") != std::string::npos) {
    Logger::info(LogCategory::GOVERNANCE, "AlertingManager",
                 "Email notification would be sent: " + message);
  }

  if (channels.find("slack") != std::string::npos) {
    Logger::info(LogCategory::GOVERNANCE, "AlertingManager",
                 "Slack notification would be sent: " + message);
  }
}

void AlertingManager::triggerWebhooks(const Alert &alert) {
  try {
    WebhookManager webhookManager(connectionString_);
    webhookManager.triggerAlertEvent(alert);
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::GOVERNANCE, "AlertingManager",
                    "Failed to trigger webhooks: " + std::string(e.what()));
  }
}

std::string AlertingManager::buildAlertMessage(const Alert &alert) {
  std::stringstream ss;
  ss << alert.title;
  if (!alert.schema_name.empty() && !alert.table_name.empty()) {
    ss << " - " << alert.schema_name << "." << alert.table_name;
  }
  if (!alert.column_name.empty()) {
    ss << "." << alert.column_name;
  }
  ss << " - " << alert.message;
  return ss.str();
}

void AlertingManager::checkDataQualityAlerts() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT schema_name, table_name, data_quality_score
      FROM metadata.data_governance_catalog
      WHERE data_quality_score < 70
        AND last_analyzed >= NOW() - INTERVAL '1 day'
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      std::string schemaName = row[0].as<std::string>();
      std::string tableName = row[1].as<std::string>();
      double qualityScore = row[2].is_null() ? 0.0 : row[2].as<double>();

      Alert alert;
      alert.alert_type = AlertType::DATA_QUALITY_DEGRADED;
      alert.severity =
          qualityScore < 50 ? AlertSeverity::CRITICAL : AlertSeverity::WARNING;
      alert.title = "Data Quality Degraded";
      alert.message = "Data quality score is " + std::to_string(qualityScore) +
                      " (below threshold of 70)";
      alert.schema_name = schemaName;
      alert.table_name = tableName;
      alert.source = "DataQualityMonitor";
      alert.status = "OPEN";

      json metadata;
      metadata["quality_score"] = qualityScore;
      metadata["threshold"] = 70.0;
      alert.metadata_json = metadata.dump();

      createAlert(alert);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error checking data quality alerts: " +
                      std::string(e.what()));
  }
}

void AlertingManager::checkPIIAlerts() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT schema_name, table_name, sensitive_data_count, pii_confidence_score
      FROM metadata.data_governance_catalog
      WHERE sensitive_data_count > 0
        AND (encryption_at_rest = false OR masking_policy_applied = false)
        AND last_analyzed >= NOW() - INTERVAL '1 day'
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      std::string schemaName = row[0].as<std::string>();
      std::string tableName = row[1].as<std::string>();
      int sensitiveCount = row[2].is_null() ? 0 : row[2].as<int>();
      double confidence = row[3].is_null() ? 0.0 : row[3].as<double>();

      Alert alert;
      alert.alert_type = AlertType::PII_DETECTED;
      alert.severity = AlertSeverity::CRITICAL;
      alert.title = "PII Detected Without Protection";
      alert.message = "Table contains " + std::to_string(sensitiveCount) +
                      " sensitive columns but encryption/masking not applied";
      alert.schema_name = schemaName;
      alert.table_name = tableName;
      alert.source = "PIIDetector";
      alert.status = "OPEN";

      json metadata;
      metadata["sensitive_data_count"] = sensitiveCount;
      metadata["pii_confidence_score"] = confidence;
      alert.metadata_json = metadata.dump();

      createAlert(alert);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error checking PII alerts: " + std::string(e.what()));
  }
}

void AlertingManager::checkAccessAnomalies() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT username, COUNT(*) as access_count, COUNT(DISTINCT table_name) as table_count
      FROM metadata.data_access_log
      WHERE access_timestamp >= NOW() - INTERVAL '1 day'
        AND is_sensitive_data = true
      GROUP BY username
      HAVING COUNT(*) > 1000 OR COUNT(DISTINCT table_name) > 50
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      std::string username = row[0].as<std::string>();
      long long accessCount = row[1].as<long long>();
      long long tableCount = row[2].as<long long>();

      Alert alert;
      alert.alert_type = AlertType::ACCESS_ANOMALY;
      alert.severity = AlertSeverity::WARNING;
      alert.title = "Access Anomaly Detected";
      alert.message = "User " + username + " accessed " +
                      std::to_string(accessCount) + " times across " +
                      std::to_string(tableCount) + " sensitive tables";
      alert.source = "AccessMonitor";
      alert.status = "OPEN";

      json metadata;
      metadata["username"] = username;
      metadata["access_count"] = accessCount;
      metadata["table_count"] = tableCount;
      alert.metadata_json = metadata.dump();

      createAlert(alert);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error checking access anomalies: " + std::string(e.what()));
  }
}

void AlertingManager::checkRetentionAlerts() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT schema_name, table_name, data_expiration_date, retention_enforced
      FROM metadata.data_governance_catalog
      WHERE retention_enforced = true
        AND data_expiration_date IS NOT NULL
        AND data_expiration_date <= NOW()
        AND legal_hold = false
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      std::string schemaName = row[0].as<std::string>();
      std::string tableName = row[1].as<std::string>();
      std::string expirationDate = row[2].as<std::string>();

      Alert alert;
      alert.alert_type = AlertType::RETENTION_EXPIRED;
      alert.severity = AlertSeverity::WARNING;
      alert.title = "Data Retention Expired";
      alert.message = "Data expiration date (" + expirationDate +
                      ") has passed. Data should be archived or deleted.";
      alert.schema_name = schemaName;
      alert.table_name = tableName;
      alert.source = "RetentionManager";
      alert.status = "OPEN";

      json metadata;
      metadata["expiration_date"] = expirationDate;
      alert.metadata_json = metadata.dump();

      createAlert(alert);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error checking retention alerts: " + std::string(e.what()));
  }
}

void AlertingManager::checkSchemaChanges() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT schema_name, table_name, last_schema_change
      FROM metadata.data_governance_catalog
      WHERE schema_evolution_tracking = true
        AND last_schema_change >= NOW() - INTERVAL '1 day'
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      std::string schemaName = row[0].as<std::string>();
      std::string tableName = row[1].as<std::string>();
      std::string lastChange = row[2].as<std::string>();

      Alert alert;
      alert.alert_type = AlertType::SCHEMA_CHANGE;
      alert.severity = AlertSeverity::INFO;
      alert.title = "Schema Change Detected";
      alert.message = "Schema changed at " + lastChange;
      alert.schema_name = schemaName;
      alert.table_name = tableName;
      alert.source = "SchemaMonitor";
      alert.status = "OPEN";

      json metadata;
      metadata["last_schema_change"] = lastChange;
      alert.metadata_json = metadata.dump();

      createAlert(alert);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error checking schema changes: " + std::string(e.what()));
  }
}

void AlertingManager::checkDataFreshness() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT schema_name, table_name, last_analyzed, data_freshness_threshold_hours
      FROM metadata.data_governance_catalog
      WHERE data_freshness_threshold_hours IS NOT NULL
        AND last_analyzed < NOW() - INTERVAL '1 hour' * data_freshness_threshold_hours
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      std::string schemaName = row[0].as<std::string>();
      std::string tableName = row[1].as<std::string>();
      std::string lastAnalyzed =
          row[2].is_null() ? "" : row[2].as<std::string>();
      int thresholdHours = row[3].is_null() ? 24 : row[3].as<int>();

      Alert alert;
      alert.alert_type = AlertType::DATA_FRESHNESS;
      alert.severity = AlertSeverity::WARNING;
      alert.title = "Data Freshness Threshold Exceeded";
      alert.message = "Data last analyzed " + lastAnalyzed +
                      " (threshold: " + std::to_string(thresholdHours) +
                      " hours)";
      alert.schema_name = schemaName;
      alert.table_name = tableName;
      alert.source = "FreshnessMonitor";
      alert.status = "OPEN";

      json metadata;
      metadata["last_analyzed"] = lastAnalyzed;
      metadata["threshold_hours"] = thresholdHours;
      alert.metadata_json = metadata.dump();

      createAlert(alert);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error checking data freshness: " + std::string(e.what()));
  }
}

void AlertingManager::checkPerformanceAlerts() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT schema_name, table_name, fragmentation_percentage
      FROM metadata.data_governance_catalog
      WHERE fragmentation_percentage > 30
        AND last_analyzed >= NOW() - INTERVAL '1 day'
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      std::string schemaName = row[0].as<std::string>();
      std::string tableName = row[1].as<std::string>();
      double fragmentation = row[2].is_null() ? 0.0 : row[2].as<double>();

      Alert alert;
      alert.alert_type = AlertType::PERFORMANCE_DEGRADED;
      alert.severity =
          fragmentation > 50 ? AlertSeverity::CRITICAL : AlertSeverity::WARNING;
      alert.title = "Performance Degradation";
      alert.message =
          "Table fragmentation is " + std::to_string(fragmentation) + "%";
      alert.schema_name = schemaName;
      alert.table_name = tableName;
      alert.source = "PerformanceMonitor";
      alert.status = "OPEN";

      json metadata;
      metadata["fragmentation_percentage"] = fragmentation;
      alert.metadata_json = metadata.dump();

      createAlert(alert);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error checking performance alerts: " +
                      std::string(e.what()));
  }
}

void AlertingManager::checkComplianceAlerts() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT schema_name, table_name, compliance_requirements, encryption_at_rest
      FROM metadata.data_governance_catalog
      WHERE compliance_requirements IS NOT NULL
        AND compliance_requirements != ''
        AND encryption_at_rest = false
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      std::string schemaName = row[0].as<std::string>();
      std::string tableName = row[1].as<std::string>();
      std::string compliance = row[2].as<std::string>();

      Alert alert;
      alert.alert_type = AlertType::COMPLIANCE_VIOLATION;
      alert.severity = AlertSeverity::CRITICAL;
      alert.title = "Compliance Violation";
      alert.message = "Table requires " + compliance +
                      " compliance but encryption not enabled";
      alert.schema_name = schemaName;
      alert.table_name = tableName;
      alert.source = "ComplianceMonitor";
      alert.status = "OPEN";

      json metadata;
      metadata["compliance_requirements"] = compliance;
      alert.metadata_json = metadata.dump();

      createAlert(alert);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error checking compliance alerts: " + std::string(e.what()));
  }
}

void AlertingManager::runAllChecks() {
  Logger::info(LogCategory::GOVERNANCE, "AlertingManager",
               "Running all alert checks");

  checkDataQualityAlerts();
  checkPIIAlerts();
  checkAccessAnomalies();
  checkRetentionAlerts();
  checkSchemaChanges();
  checkDataFreshness();
  checkPerformanceAlerts();
  checkComplianceAlerts();

  Logger::info(LogCategory::GOVERNANCE, "AlertingManager",
               "All alert checks completed");
}

void AlertingManager::monitorDataFreshness(int thresholdHours) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.data_governance_catalog
      SET data_freshness_threshold_hours = $1,
          last_freshness_check = NOW()
      WHERE data_freshness_threshold_hours IS NULL
    )";

    txn.exec_params(query, std::to_string(thresholdHours));
    txn.commit();

    checkDataFreshness();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error monitoring data freshness: " + std::string(e.what()));
  }
}

void AlertingManager::monitorSchemaEvolution() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.data_governance_catalog
      SET schema_evolution_tracking = true,
          last_schema_change = NOW()
      WHERE schema_evolution_tracking = false
    )";

    txn.exec(query);
    txn.commit();

    checkSchemaChanges();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "AlertingManager",
                  "Error monitoring schema evolution: " +
                      std::string(e.what()));
  }
}

void AlertingManager::detectAnomalies(int days) {
  checkAccessAnomalies();
  checkDataQualityAlerts();
  checkPerformanceAlerts();
}
