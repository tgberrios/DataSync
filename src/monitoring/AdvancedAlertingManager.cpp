#include "monitoring/AdvancedAlertingManager.h"
#include "governance/AlertingManager.h"
#include "core/logger.h"
#include <curl/curl.h>
#include <pqxx/pqxx>
#include <sstream>
#include <ctime>
#include <iomanip>

AdvancedAlertingManager::AdvancedAlertingManager(const std::string& connectionString)
    : connectionString_(connectionString) {
  // Tables are created in migration
}

std::string AdvancedAlertingManager::severityToString(AlertSeverity severity) {
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
      return "UNKNOWN";
  }
}

bool AdvancedAlertingManager::createIntegration(const Integration& integration) {
  return saveIntegrationToDatabase(integration);
}

bool AdvancedAlertingManager::saveIntegrationToDatabase(const Integration& integration) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    json severityMappingJson = json::object();
    for (const auto& [key, value] : integration.severityMapping) {
      severityMappingJson[key] = value;
    }

    txn.exec_params(
        "INSERT INTO metadata.alerting_integrations "
        "(integration_id, type, name, integration_key, api_key, service_id, team_id, enabled, "
        "severity_mapping) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) "
        "ON CONFLICT (integration_id) DO UPDATE SET "
        "name = EXCLUDED.name, integration_key = EXCLUDED.integration_key, "
        "api_key = EXCLUDED.api_key, service_id = EXCLUDED.service_id, "
        "team_id = EXCLUDED.team_id, enabled = EXCLUDED.enabled, "
        "severity_mapping = EXCLUDED.severity_mapping",
        integration.id, integration.type, integration.name,
        integration.integrationKey.empty() ? nullptr : integration.integrationKey,
        integration.apiKey.empty() ? nullptr : integration.apiKey,
        integration.serviceId.empty() ? nullptr : integration.serviceId,
        integration.teamId.empty() ? nullptr : integration.teamId, integration.enabled,
        severityMappingJson.dump());

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "AdvancedAlertingManager",
                  "Error saving integration: " + std::string(e.what()));
    return false;
  }
}

std::unique_ptr<AdvancedAlertingManager::Integration> AdvancedAlertingManager::getIntegration(
    const std::string& integrationId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params(
        "SELECT * FROM metadata.alerting_integrations WHERE integration_id = $1", integrationId);

    if (result.empty()) {
      return nullptr;
    }

    auto row = result[0];
    auto integration = std::make_unique<Integration>();
    integration->id = row["integration_id"].as<std::string>();
    integration->type = row["type"].as<std::string>();
    integration->name = row["name"].as<std::string>();
    if (!row["integration_key"].is_null()) {
      integration->integrationKey = row["integration_key"].as<std::string>();
    }
    if (!row["api_key"].is_null()) {
      integration->apiKey = row["api_key"].as<std::string>();
    }
    if (!row["service_id"].is_null()) {
      integration->serviceId = row["service_id"].as<std::string>();
    }
    if (!row["team_id"].is_null()) {
      integration->teamId = row["team_id"].as<std::string>();
    }
    integration->enabled = row["enabled"].as<bool>();

    if (!row["severity_mapping"].is_null()) {
      json mappingJson = json::parse(row["severity_mapping"].as<std::string>());
      for (auto it = mappingJson.begin(); it != mappingJson.end(); ++it) {
        integration->severityMapping[it.key()] = it.value().get<std::string>();
      }
    }

    return integration;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "AdvancedAlertingManager",
                  "Error loading integration: " + std::string(e.what()));
    return nullptr;
  }
}

std::vector<AdvancedAlertingManager::Integration> AdvancedAlertingManager::listIntegrations() {
  std::vector<Integration> integrations;
  // TODO: Implement
  return integrations;
}

std::string AdvancedAlertingManager::triggerAlert(const std::string& integrationId,
                                                   const Alert& alert) {
  auto integration = getIntegration(integrationId);
  if (!integration || !integration->enabled) {
    return "";
  }

  bool success = false;
  std::string externalId = "";

  if (integration->type == "pagerduty") {
    success = triggerPagerDutyAlert(*integration, alert);
    // Extract external ID from response
  } else if (integration->type == "opsgenie") {
    success = triggerOpsgenieAlert(*integration, alert);
    // Extract external ID from response
  }

  if (success) {
    Incident incident;
    incident.id = "incident_" + std::to_string(std::time(nullptr)) + "_" + integrationId;
    incident.integrationId = integrationId;
    incident.externalId = externalId;
    incident.alertId = std::to_string(alert.id);
    incident.status = "triggered";
    incident.createdAt = std::chrono::system_clock::now();
    incident.metadata = json{{"alert_type", alert.alert_type},
                             {"severity", alert.severity},
                             {"title", alert.title}};

    saveIncidentToDatabase(incident);
    return incident.id;
  }

  return "";
}

bool AdvancedAlertingManager::triggerPagerDutyAlert(const Integration& integration,
                                                     const Alert& alert) {
  if (integration.integrationKey.empty()) {
    return false;
  }

  std::string severityStr = severityToString(alert.severity);
  std::string urgency = mapSeverityToUrgency(severityStr, integration.severityMapping);

  json payload;
  payload["routing_key"] = integration.integrationKey;
  payload["event_action"] = "trigger";
  payload["payload"]["summary"] = alert.title;
  payload["payload"]["severity"] = urgency;
  payload["payload"]["source"] = alert.source;
  payload["payload"]["custom_details"]["message"] = alert.message;
  payload["payload"]["custom_details"]["schema_name"] = alert.schema_name;
  payload["payload"]["custom_details"]["table_name"] = alert.table_name;

  std::string url = "https://events.pagerduty.com/v2/enqueue";
  std::string payloadStr = payload.dump();

  CURL* curl = curl_easy_init();
  if (!curl) {
    return false;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payloadStr.c_str());

  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode res = curl_easy_perform(curl);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  return res == CURLE_OK;
}

bool AdvancedAlertingManager::triggerOpsgenieAlert(const Integration& integration,
                                                    const Alert& alert) {
  if (integration.apiKey.empty()) {
    return false;
  }

  json payload;
  payload["message"] = alert.title;
  payload["description"] = alert.message;
  std::string severityStr = severityToString(alert.severity);
  payload["priority"] = mapSeverityToUrgency(severityStr, integration.severityMapping);
  payload["tags"] = json::array({alert.alert_type, alert.source});

  std::string url = "https://api.opsgenie.com/v2/alerts";
  std::string payloadStr = payload.dump();

  CURL* curl = curl_easy_init();
  if (!curl) {
    return false;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payloadStr.c_str());

  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  std::string authHeader = "Authorization: GenieKey " + integration.apiKey;
  headers = curl_slist_append(headers, authHeader.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode res = curl_easy_perform(curl);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  return res == CURLE_OK;
}

std::string AdvancedAlertingManager::mapSeverityToUrgency(
    const std::string& severity, const std::map<std::string, std::string>& mapping) {
  if (mapping.find(severity) != mapping.end()) {
    return mapping.at(severity);
  }

  // Default mapping
  if (severity == "CRITICAL" || severity == "ERROR") {
    return "critical";
  } else if (severity == "WARNING") {
    return "high";
  } else {
    return "low";
  }
}

bool AdvancedAlertingManager::saveIncidentToDatabase(const Incident& incident) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto createdAtT = std::chrono::system_clock::to_time_t(incident.createdAt);
    std::tm createdAtTm = *std::localtime(&createdAtT);
    std::ostringstream createdAtStr;
    createdAtStr << std::put_time(&createdAtTm, "%Y-%m-%d %H:%M:%S");

    std::string resolvedAtStr;
    if (incident.resolvedAt.time_since_epoch().count() > 0) {
      auto resolvedAtT = std::chrono::system_clock::to_time_t(incident.resolvedAt);
      std::tm resolvedAtTm = *std::localtime(&resolvedAtT);
      std::ostringstream resolvedAtOss;
      resolvedAtOss << std::put_time(&resolvedAtTm, "%Y-%m-%d %H:%M:%S");
      resolvedAtStr = resolvedAtOss.str();
    }

    txn.exec_params(
        "INSERT INTO metadata.alerting_incidents "
        "(incident_id, integration_id, external_id, alert_id, status, resolved_at, metadata) "
        "VALUES ($1, $2, $3, $4, $5, $6::timestamp, $7) "
        "ON CONFLICT (incident_id) DO UPDATE SET "
        "status = EXCLUDED.status, resolved_at = EXCLUDED.resolved_at, metadata = EXCLUDED.metadata",
        incident.id, incident.integrationId,
        incident.externalId.empty() ? nullptr : incident.externalId,
        incident.alertId.empty() ? nullptr : incident.alertId, incident.status,
        resolvedAtStr.empty() ? nullptr : resolvedAtStr,
        incident.metadata.dump());

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "AdvancedAlertingManager",
                  "Error saving incident: " + std::string(e.what()));
    return false;
  }
}

std::vector<AdvancedAlertingManager::Incident> AdvancedAlertingManager::getIncidents(
    const std::string& integrationId) {
  std::vector<Incident> incidents;
  // TODO: Implement
  return incidents;
}

bool AdvancedAlertingManager::acknowledgeIncident(const std::string& incidentId) {
  // TODO: Implement
  return false;
}

bool AdvancedAlertingManager::resolveIncident(const std::string& incidentId) {
  // TODO: Implement
  return false;
}
