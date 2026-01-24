#ifndef ADVANCED_ALERTING_MANAGER_H
#define ADVANCED_ALERTING_MANAGER_H

#include "governance/AlertingManager.h"
#include "core/logger.h"
#include "third_party/json.hpp"
#include <string>
#include <vector>

using json = nlohmann::json;

// AdvancedAlertingManager: Integración con PagerDuty y Opsgenie
class AdvancedAlertingManager {
public:
  struct Integration {
    std::string id;
    std::string type; // "pagerduty", "opsgenie"
    std::string name;
    std::string integrationKey; // Para PagerDuty
    std::string apiKey;         // Para Opsgenie
    std::string serviceId;      // Para PagerDuty
    std::string teamId;         // Para Opsgenie
    bool enabled;
    std::map<std::string, std::string> severityMapping; // "CRITICAL" -> "critical"
  };

  struct Incident {
    std::string id;
    std::string integrationId;
    std::string externalId; // ID en PagerDuty/Opsgenie
    std::string alertId;
    std::string status; // "triggered", "acknowledged", "resolved"
    std::chrono::system_clock::time_point createdAt;
    std::chrono::system_clock::time_point resolvedAt;
    json metadata;
  };

  explicit AdvancedAlertingManager(const std::string& connectionString);
  ~AdvancedAlertingManager() = default;

  // Crear integración
  bool createIntegration(const Integration& integration);

  // Obtener integración
  std::unique_ptr<Integration> getIntegration(const std::string& integrationId);

  // Listar integraciones
  std::vector<Integration> listIntegrations();

  // Trigger alerta en integración externa
  std::string triggerAlert(const std::string& integrationId, const Alert& alert);

  // Obtener incidentes
  std::vector<Incident> getIncidents(const std::string& integrationId = "");

  // Acknowledge incident
  bool acknowledgeIncident(const std::string& incidentId);

  // Resolve incident
  bool resolveIncident(const std::string& incidentId);

private:
  std::string connectionString_;

  std::string severityToString(AlertSeverity severity);
  bool triggerPagerDutyAlert(const Integration& integration, const Alert& alert);
  bool triggerOpsgenieAlert(const Integration& integration, const Alert& alert);
  std::string mapSeverityToUrgency(const std::string& severity,
                                    const std::map<std::string, std::string>& mapping);
  bool saveIntegrationToDatabase(const Integration& integration);
  bool saveIncidentToDatabase(const Incident& incident);
};

#endif // ADVANCED_ALERTING_MANAGER_H
