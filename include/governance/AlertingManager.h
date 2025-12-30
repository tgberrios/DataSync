#ifndef ALERTING_MANAGER_H
#define ALERTING_MANAGER_H

#include <pqxx/pqxx>
#include <string>
#include <vector>

enum class AlertSeverity { INFO, WARNING, CRITICAL, ERROR };

enum class AlertType {
  DATA_QUALITY_DEGRADED,
  PII_DETECTED,
  ACCESS_ANOMALY,
  RETENTION_EXPIRED,
  SCHEMA_CHANGE,
  DATA_FRESHNESS,
  PERFORMANCE_DEGRADED,
  COMPLIANCE_VIOLATION,
  SECURITY_BREACH,
  CUSTOM
};

struct Alert {
  int id;
  AlertType alert_type;
  AlertSeverity severity;
  std::string title;
  std::string message;
  std::string schema_name;
  std::string table_name;
  std::string column_name;
  std::string source;
  std::string status;
  std::string assigned_to;
  std::string resolved_at;
  std::string metadata_json;
  std::string created_at;
  std::string updated_at;
};

struct AlertRule {
  int id;
  std::string rule_name;
  AlertType alert_type;
  AlertSeverity severity;
  std::string condition;
  std::string threshold_value;
  bool enabled;
  std::string notification_channels;
  std::string created_at;
  std::string updated_at;
};

class AlertingManager {
private:
  std::string connectionString_;

  std::string alertTypeToString(AlertType type);
  std::string severityToString(AlertSeverity severity);
  AlertType stringToAlertType(const std::string &str);
  AlertSeverity stringToSeverity(const std::string &str);
  bool evaluateRule(const AlertRule &rule);
  void sendNotification(const Alert &alert, const std::string &channels);
  std::string buildAlertMessage(const Alert &alert);
  void triggerWebhooks(const Alert &alert);

public:
  explicit AlertingManager(const std::string &connectionString);
  ~AlertingManager() = default;

  int createAlert(const Alert &alert);
  bool updateAlertStatus(int alertId, const std::string &status,
                         const std::string &assignedTo = "");
  bool resolveAlert(int alertId, const std::string &resolvedBy);
  std::vector<Alert> getActiveAlerts(const std::string &severity = "",
                                     int limit = 100);
  std::vector<Alert> getAlertsByType(AlertType type, int days = 7);
  std::vector<Alert> getAlertsForTable(const std::string &schemaName,
                                       const std::string &tableName);

  bool addAlertRule(const AlertRule &rule);
  bool updateAlertRule(int ruleId, const AlertRule &rule);
  bool deleteAlertRule(int ruleId);
  bool enableAlertRule(int ruleId, bool enabled);
  std::vector<AlertRule> getAllRules();
  std::vector<AlertRule> getActiveRules();

  void checkDataQualityAlerts();
  void checkPIIAlerts();
  void checkAccessAnomalies();
  void checkRetentionAlerts();
  void checkSchemaChanges();
  void checkDataFreshness();
  void checkPerformanceAlerts();
  void checkComplianceAlerts();
  void runAllChecks();

  void monitorDataFreshness(int thresholdHours = 24);
  void monitorSchemaEvolution();
  void detectAnomalies(int days = 7);
};

#endif
