#ifndef BOTTLENECK_DETECTOR_H
#define BOTTLENECK_DETECTOR_H

#include "monitoring/ResourceTracker.h"
#include "core/logger.h"
#include "third_party/json.hpp"
#include <chrono>
#include <string>
#include <vector>

using json = nlohmann::json;

// BottleneckDetector: Detección automática de cuellos de botella
class BottleneckDetector {
public:
  struct Bottleneck {
    std::string id;
    std::string resourceType; // "cpu", "memory", "io", "network", "database"
    std::string severity;     // "low", "medium", "high", "critical"
    std::string component;
    std::string description;
    std::vector<std::string> recommendations;
    std::chrono::system_clock::time_point detectedAt;
    json metadata;
  };

  explicit BottleneckDetector(const std::string& connectionString);
  ~BottleneckDetector() = default;

  // Analizar recursos y detectar bottlenecks
  std::vector<Bottleneck> analyze();

  // Obtener bottlenecks actuales
  std::vector<Bottleneck> getCurrentBottlenecks();

  // Obtener histórico de detecciones
  std::vector<Bottleneck> getHistory(int days = 7);

  // Detectar procesos bloqueantes
  std::vector<Bottleneck> detectBlockingProcesses();

private:
  std::string connectionString_;
  std::unique_ptr<ResourceTracker> resourceTracker_;

  Bottleneck analyzeCpu(const ResourceTracker::ResourceMetrics& metrics);
  Bottleneck analyzeMemory(const ResourceTracker::ResourceMetrics& metrics);
  Bottleneck analyzeIO(const ResourceTracker::ResourceMetrics& metrics);
  Bottleneck analyzeNetwork(const ResourceTracker::ResourceMetrics& metrics);
  Bottleneck analyzeDatabase(const ResourceTracker::ResourceMetrics& metrics);
  bool saveBottleneckToDatabase(const Bottleneck& bottleneck);
};

#endif // BOTTLENECK_DETECTOR_H
