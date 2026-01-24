#include "monitoring/BottleneckDetector.h"
#include "core/logger.h"
#include <pqxx/pqxx>
#include <sstream>
#include <ctime>

BottleneckDetector::BottleneckDetector(const std::string& connectionString)
    : connectionString_(connectionString) {
  resourceTracker_ = std::make_unique<ResourceTracker>(connectionString);

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.bottleneck_detections ("
        "id SERIAL PRIMARY KEY,"
        "bottleneck_id VARCHAR(255) UNIQUE NOT NULL,"
        "resource_type VARCHAR(50) NOT NULL,"
        "severity VARCHAR(20) NOT NULL,"
        "component VARCHAR(255),"
        "description TEXT,"
        "recommendations JSONB DEFAULT '[]'::jsonb,"
        "detected_at TIMESTAMP DEFAULT NOW(),"
        "metadata JSONB DEFAULT '{}'::jsonb"
        ")");

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "BottleneckDetector",
                  "Error creating tables: " + std::string(e.what()));
  }
}

std::vector<BottleneckDetector::Bottleneck> BottleneckDetector::analyze() {
  std::vector<Bottleneck> bottlenecks;
  auto metrics = resourceTracker_->collectCurrentMetrics();

  auto cpuBottleneck = analyzeCpu(metrics);
  if (!cpuBottleneck.id.empty()) {
    bottlenecks.push_back(cpuBottleneck);
  }

  auto memoryBottleneck = analyzeMemory(metrics);
  if (!memoryBottleneck.id.empty()) {
    bottlenecks.push_back(memoryBottleneck);
  }

  auto ioBottleneck = analyzeIO(metrics);
  if (!ioBottleneck.id.empty()) {
    bottlenecks.push_back(ioBottleneck);
  }

  auto networkBottleneck = analyzeNetwork(metrics);
  if (!networkBottleneck.id.empty()) {
    bottlenecks.push_back(networkBottleneck);
  }

  auto dbBottleneck = analyzeDatabase(metrics);
  if (!dbBottleneck.id.empty()) {
    bottlenecks.push_back(dbBottleneck);
  }

  for (const auto& bottleneck : bottlenecks) {
    saveBottleneckToDatabase(bottleneck);
  }

  return bottlenecks;
}

BottleneckDetector::Bottleneck BottleneckDetector::analyzeCpu(
    const ResourceTracker::ResourceMetrics& metrics) {
  Bottleneck bottleneck;
  if (metrics.cpuPercent > 90.0) {
    bottleneck.id = "cpu_critical_" + std::to_string(std::time(nullptr));
    bottleneck.resourceType = "cpu";
    bottleneck.severity = "critical";
    bottleneck.component = "system";
    bottleneck.description = "CPU usage is critically high: " + std::to_string(metrics.cpuPercent) + "%";
    bottleneck.recommendations = {"Scale horizontally", "Optimize queries", "Add caching"};
    bottleneck.detectedAt = std::chrono::system_clock::now();
  } else if (metrics.cpuPercent > 80.0) {
    bottleneck.id = "cpu_high_" + std::to_string(std::time(nullptr));
    bottleneck.resourceType = "cpu";
    bottleneck.severity = "high";
    bottleneck.component = "system";
    bottleneck.description = "CPU usage is high: " + std::to_string(metrics.cpuPercent) + "%";
    bottleneck.recommendations = {"Monitor closely", "Consider optimization"};
    bottleneck.detectedAt = std::chrono::system_clock::now();
  }
  return bottleneck;
}

BottleneckDetector::Bottleneck BottleneckDetector::analyzeMemory(
    const ResourceTracker::ResourceMetrics& metrics) {
  Bottleneck bottleneck;
  if (metrics.memoryPercent > 90.0) {
    bottleneck.id = "memory_critical_" + std::to_string(std::time(nullptr));
    bottleneck.resourceType = "memory";
    bottleneck.severity = "critical";
    bottleneck.component = "system";
    bottleneck.description = "Memory usage is critically high: " + std::to_string(metrics.memoryPercent) + "%";
    bottleneck.recommendations = {"Increase memory", "Optimize memory usage", "Add swap"};
    bottleneck.detectedAt = std::chrono::system_clock::now();
  }
  return bottleneck;
}

BottleneckDetector::Bottleneck BottleneckDetector::analyzeIO(
    const ResourceTracker::ResourceMetrics& metrics) {
  Bottleneck bottleneck;
  if (metrics.ioReadMBps > 100.0 || metrics.ioWriteMBps > 100.0) {
    bottleneck.id = "io_high_" + std::to_string(std::time(nullptr));
    bottleneck.resourceType = "io";
    bottleneck.severity = "medium";
    bottleneck.component = "disk";
    bottleneck.description = "High I/O activity detected";
    bottleneck.recommendations = {"Use SSD", "Optimize queries", "Add caching"};
    bottleneck.detectedAt = std::chrono::system_clock::now();
  }
  return bottleneck;
}

BottleneckDetector::Bottleneck BottleneckDetector::analyzeNetwork(
    const ResourceTracker::ResourceMetrics& metrics) {
  Bottleneck bottleneck;
  // TODO: Implement network analysis
  return bottleneck;
}

BottleneckDetector::Bottleneck BottleneckDetector::analyzeDatabase(
    const ResourceTracker::ResourceMetrics& metrics) {
  Bottleneck bottleneck;
  if (metrics.dbLocks > 10) {
    bottleneck.id = "db_locks_" + std::to_string(std::time(nullptr));
    bottleneck.resourceType = "database";
    bottleneck.severity = "high";
    bottleneck.component = "postgresql";
    bottleneck.description = "High number of database locks: " + std::to_string(metrics.dbLocks);
    bottleneck.recommendations = {"Review transaction isolation", "Optimize queries", "Reduce lock timeouts"};
    bottleneck.detectedAt = std::chrono::system_clock::now();
  }
  if (metrics.dbCacheHitRatio < 80.0) {
    bottleneck.id = "db_cache_" + std::to_string(std::time(nullptr));
    bottleneck.resourceType = "database";
    bottleneck.severity = "medium";
    bottleneck.component = "postgresql";
    bottleneck.description = "Low cache hit ratio: " + std::to_string(metrics.dbCacheHitRatio) + "%";
    bottleneck.recommendations = {"Increase shared_buffers", "Optimize queries", "Add indexes"};
    bottleneck.detectedAt = std::chrono::system_clock::now();
  }
  return bottleneck;
}

bool BottleneckDetector::saveBottleneckToDatabase(const Bottleneck& bottleneck) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    json recommendationsJson = json::array();
    for (const auto& rec : bottleneck.recommendations) {
      recommendationsJson.push_back(rec);
    }

    txn.exec_params(
        "INSERT INTO metadata.bottleneck_detections "
        "(bottleneck_id, resource_type, severity, component, description, recommendations, metadata) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7) "
        "ON CONFLICT (bottleneck_id) DO UPDATE SET "
        "severity = EXCLUDED.severity, description = EXCLUDED.description, "
        "recommendations = EXCLUDED.recommendations, detected_at = NOW()",
        bottleneck.id, bottleneck.resourceType, bottleneck.severity,
        bottleneck.component.empty() ? nullptr : bottleneck.component, bottleneck.description,
        recommendationsJson.dump(), bottleneck.metadata.dump());

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "BottleneckDetector",
                  "Error saving bottleneck: " + std::string(e.what()));
    return false;
  }
}

std::vector<BottleneckDetector::Bottleneck> BottleneckDetector::getCurrentBottlenecks() {
  std::vector<Bottleneck> bottlenecks;
  // TODO: Load from database
  return bottlenecks;
}

std::vector<BottleneckDetector::Bottleneck> BottleneckDetector::getHistory(int days) {
  std::vector<Bottleneck> bottlenecks;
  // TODO: Load from database
  return bottlenecks;
}

std::vector<BottleneckDetector::Bottleneck> BottleneckDetector::detectBlockingProcesses() {
  std::vector<Bottleneck> bottlenecks;
  // TODO: Implement
  return bottlenecks;
}
