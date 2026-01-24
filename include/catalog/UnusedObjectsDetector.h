#ifndef UNUSED_OBJECTS_DETECTOR_H
#define UNUSED_OBJECTS_DETECTOR_H

#include "core/logger.h"
#include "third_party/json.hpp"
#include <chrono>
#include <string>
#include <vector>

using json = nlohmann::json;

// UnusedObjectsDetector: Detección de objetos no usados
class UnusedObjectsDetector {
public:
  enum class ObjectType {
    TABLE,
    VIEW,
    MATERIALIZED_VIEW
  };

  struct ObjectUsage {
    int trackingId;
    ObjectType objectType;
    std::string schemaName;
    std::string objectName;
    std::chrono::system_clock::time_point lastAccessedAt;
    int64_t accessCount;
    std::string lastAccessType; // 'SELECT', 'INSERT', 'UPDATE', 'DELETE'
    std::string accessedByUser;
  };

  struct UnusedObject {
    ObjectType objectType;
    std::string schemaName;
    std::string objectName;
    int daysSinceLastAccess;
    std::vector<std::string> dependencies; // Objetos que dependen de este
    std::vector<std::string> recommendations;
  };

  struct UnusedObjectsReport {
    int reportId;
    std::chrono::system_clock::time_point generatedAt;
    int daysThreshold;
    std::vector<UnusedObject> unusedObjects;
    std::vector<std::string> recommendations;
    int totalUnusedCount;
    std::string generatedBy;
  };

  explicit UnusedObjectsDetector(const std::string& connectionString);
  ~UnusedObjectsDetector() = default;

  // Registrar acceso a objeto
  void trackAccess(ObjectType objectType, const std::string& schemaName,
                  const std::string& objectName, const std::string& accessType,
                  const std::string& userName = "");

  // Obtener tracking de uso de un objeto
  std::unique_ptr<ObjectUsage> getObjectUsage(ObjectType objectType,
                                              const std::string& schemaName,
                                              const std::string& objectName);

  // Detectar objetos no usados
  UnusedObjectsReport detectUnusedObjects(int daysThreshold,
                                         const std::string& generatedBy = "");

  // Obtener reporte
  std::unique_ptr<UnusedObjectsReport> getReport(int reportId);

  // Listar reportes históricos
  std::vector<UnusedObjectsReport> listReports(int limit = 100);

  // Analizar dependencias de un objeto
  std::vector<std::string> analyzeDependencies(ObjectType objectType,
                                               const std::string& schemaName,
                                               const std::string& objectName);

private:
  std::string connectionString_;

  std::string objectTypeToString(ObjectType type);
  ObjectType stringToObjectType(const std::string& str);
  bool saveUsageToDatabase(const ObjectUsage& usage);
  std::unique_ptr<ObjectUsage> loadUsageFromDatabase(ObjectType objectType,
                                                     const std::string& schemaName,
                                                     const std::string& objectName);
  bool saveReportToDatabase(const UnusedObjectsReport& report);
  std::unique_ptr<UnusedObjectsReport> loadReportFromDatabase(int reportId);
  std::vector<std::string> getDependenciesFromQueries(const std::string& schemaName,
                                                      const std::string& objectName);
  std::vector<std::string> getDependenciesFromWorkflows(const std::string& schemaName,
                                                        const std::string& objectName);
  std::vector<std::string> getDependenciesFromTransformations(const std::string& schemaName,
                                                             const std::string& objectName);
};

#endif // UNUSED_OBJECTS_DETECTOR_H
