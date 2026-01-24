#include "catalog/UnusedObjectsDetector.h"
#include <pqxx/pqxx>
#include <sstream>
#include <iomanip>
#include <ctime>

UnusedObjectsDetector::UnusedObjectsDetector(const std::string& connectionString)
    : connectionString_(connectionString) {
  // Crear tablas si no existen
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.object_usage_tracking ("
        "tracking_id SERIAL PRIMARY KEY,"
        "object_type VARCHAR(50) NOT NULL,"
        "schema_name VARCHAR(100) NOT NULL,"
        "object_name VARCHAR(100) NOT NULL,"
        "last_accessed_at TIMESTAMP,"
        "access_count BIGINT DEFAULT 0,"
        "last_access_type VARCHAR(20),"
        "accessed_by_user VARCHAR(100),"
        "created_at TIMESTAMP DEFAULT NOW(),"
        "updated_at TIMESTAMP DEFAULT NOW(),"
        "UNIQUE(object_type, schema_name, object_name)"
        ")");

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.unused_objects_report ("
        "report_id SERIAL PRIMARY KEY,"
        "generated_at TIMESTAMP DEFAULT NOW(),"
        "days_threshold INTEGER NOT NULL,"
        "unused_objects JSONB NOT NULL,"
        "recommendations JSONB DEFAULT '[]'::jsonb,"
        "total_unused_count INTEGER DEFAULT 0,"
        "generated_by VARCHAR(100)"
        ")");

    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_object_usage_tracking_object "
        "ON metadata.object_usage_tracking(object_type, schema_name, object_name)");

    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_object_usage_tracking_last_accessed "
        "ON metadata.object_usage_tracking(last_accessed_at)");

    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_unused_objects_report_generated_at "
        "ON metadata.unused_objects_report(generated_at)");

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::DATABASE, "UnusedObjectsDetector",
                  "Error creating tables: " + std::string(e.what()));
  }
}

std::string UnusedObjectsDetector::objectTypeToString(ObjectType type) {
  switch (type) {
    case ObjectType::TABLE:
      return "table";
    case ObjectType::VIEW:
      return "view";
    case ObjectType::MATERIALIZED_VIEW:
      return "materialized_view";
    default:
      return "table";
  }
}

UnusedObjectsDetector::ObjectType UnusedObjectsDetector::stringToObjectType(
    const std::string& str) {
  if (str == "view") {
    return ObjectType::VIEW;
  } else if (str == "materialized_view") {
    return ObjectType::MATERIALIZED_VIEW;
  }
  return ObjectType::TABLE;
}

void UnusedObjectsDetector::trackAccess(ObjectType objectType,
                                       const std::string& schemaName,
                                       const std::string& objectName,
                                       const std::string& accessType,
                                       const std::string& userName) {
  auto usage = getObjectUsage(objectType, schemaName, objectName);
  
  if (!usage) {
    usage = std::make_unique<ObjectUsage>();
    usage->objectType = objectType;
    usage->schemaName = schemaName;
    usage->objectName = objectName;
    usage->accessCount = 0;
  }

  usage->lastAccessedAt = std::chrono::system_clock::now();
  usage->accessCount++;
  usage->lastAccessType = accessType;
  usage->accessedByUser = userName;

  saveUsageToDatabase(*usage);
}

std::unique_ptr<UnusedObjectsDetector::ObjectUsage> UnusedObjectsDetector::getObjectUsage(
    ObjectType objectType, const std::string& schemaName, const std::string& objectName) {
  return loadUsageFromDatabase(objectType, schemaName, objectName);
}

std::unique_ptr<UnusedObjectsDetector::ObjectUsage> UnusedObjectsDetector::loadUsageFromDatabase(
    ObjectType objectType, const std::string& schemaName, const std::string& objectName) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params(
        "SELECT * FROM metadata.object_usage_tracking "
        "WHERE object_type = $1 AND schema_name = $2 AND object_name = $3",
        objectTypeToString(objectType), schemaName, objectName);

    if (result.empty()) {
      return nullptr;
    }

    auto row = result[0];
    auto usage = std::make_unique<ObjectUsage>();
    usage->trackingId = row["tracking_id"].as<int>();
    usage->objectType = stringToObjectType(row["object_type"].as<std::string>());
    usage->schemaName = row["schema_name"].as<std::string>();
    usage->objectName = row["object_name"].as<std::string>();
    if (!row["last_accessed_at"].is_null()) {
      auto lastAccessStr = row["last_accessed_at"].as<std::string>();
      std::tm tm = {};
      std::istringstream ss(lastAccessStr);
      ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
      usage->lastAccessedAt = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    }
    usage->accessCount = row["access_count"].as<int64_t>();
    if (!row["last_access_type"].is_null()) {
      usage->lastAccessType = row["last_access_type"].as<std::string>();
    }
    if (!row["accessed_by_user"].is_null()) {
      usage->accessedByUser = row["accessed_by_user"].as<std::string>();
    }

    return usage;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::DATABASE, "UnusedObjectsDetector",
                  "Error loading usage: " + std::string(e.what()));
    return nullptr;
  }
}

bool UnusedObjectsDetector::saveUsageToDatabase(const ObjectUsage& usage) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto lastAccessTimeT = std::chrono::system_clock::to_time_t(usage.lastAccessedAt);
    std::tm tm = *std::localtime(&lastAccessTimeT);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    std::string lastAccessStr = oss.str();

    txn.exec_params(
        "INSERT INTO metadata.object_usage_tracking "
        "(object_type, schema_name, object_name, last_accessed_at, access_count, "
        "last_access_type, accessed_by_user) "
        "VALUES ($1, $2, $3, $4::timestamp, $5, $6, $7) "
        "ON CONFLICT (object_type, schema_name, object_name) DO UPDATE SET "
        "last_accessed_at = EXCLUDED.last_accessed_at, "
        "access_count = EXCLUDED.access_count, "
        "last_access_type = EXCLUDED.last_access_type, "
        "accessed_by_user = EXCLUDED.accessed_by_user, "
        "updated_at = NOW()",
        objectTypeToString(usage.objectType), usage.schemaName, usage.objectName,
        lastAccessStr, usage.accessCount,
        usage.lastAccessType.empty() ? nullptr : usage.lastAccessType,
        usage.accessedByUser.empty() ? nullptr : usage.accessedByUser);

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::DATABASE, "UnusedObjectsDetector",
                  "Error saving usage: " + std::string(e.what()));
    return false;
  }
}

UnusedObjectsDetector::UnusedObjectsReport UnusedObjectsDetector::detectUnusedObjects(
    int daysThreshold, const std::string& generatedBy) {
  UnusedObjectsReport report = {};
  report.daysThreshold = daysThreshold;
  report.generatedBy = generatedBy;
  report.generatedAt = std::chrono::system_clock::now();

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Calcular fecha threshold
    auto thresholdTime = std::chrono::system_clock::now() - std::chrono::hours(24 * daysThreshold);
    auto thresholdTimeT = std::chrono::system_clock::to_time_t(thresholdTime);
    std::tm tm = *std::localtime(&thresholdTimeT);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    std::string thresholdStr = oss.str();

    // Obtener objetos sin acceso reciente o sin tracking
    auto result = txn.exec_params(
        "SELECT DISTINCT "
        "CASE WHEN t.object_type IS NOT NULL THEN t.object_type ELSE 'table' END as object_type, "
        "c.schema_name, c.table_name as object_name, "
        "COALESCE(t.last_accessed_at, c.created_at) as last_access "
        "FROM metadata.catalog c "
        "LEFT JOIN metadata.object_usage_tracking t "
        "ON c.schema_name = t.schema_name AND c.table_name = t.object_name "
        "WHERE (t.last_accessed_at IS NULL OR t.last_accessed_at < $1::timestamp) "
        "OR t.tracking_id IS NULL",
        thresholdStr);

    for (const auto& row : result) {
      UnusedObject unused;
      unused.objectType = stringToObjectType(row["object_type"].as<std::string>());
      unused.schemaName = row["schema_name"].as<std::string>();
      unused.objectName = row["object_name"].as<std::string>();

      if (!row["last_access"].is_null()) {
        auto lastAccessStr = row["last_access"].as<std::string>();
        std::tm lastAccessTm = {};
        std::istringstream ss(lastAccessStr);
        ss >> std::get_time(&lastAccessTm, "%Y-%m-%d %H:%M:%S");
        auto lastAccessTime = std::chrono::system_clock::from_time_t(std::mktime(&lastAccessTm));
        auto daysSince = std::chrono::duration_cast<std::chrono::hours>(
                            std::chrono::system_clock::now() - lastAccessTime)
                            .count() /
                        24;
        unused.daysSinceLastAccess = static_cast<int>(daysSince);
      } else {
        unused.daysSinceLastAccess = daysThreshold + 1; // Nunca accedido
      }

      // Analizar dependencias
      unused.dependencies = analyzeDependencies(unused.objectType, unused.schemaName,
                                                unused.objectName);

      // Generar recomendaciones
      if (unused.dependencies.empty()) {
        unused.recommendations.push_back("No dependencies found. Safe to archive or delete.");
      } else {
        unused.recommendations.push_back(
            "Has " + std::to_string(unused.dependencies.size()) +
            " dependencies. Review before archiving.");
      }

      if (unused.daysSinceLastAccess > daysThreshold * 2) {
        unused.recommendations.push_back("Consider archiving this object.");
      }

      report.unusedObjects.push_back(unused);
    }

    report.totalUnusedCount = report.unusedObjects.size();

    // Guardar reporte
    saveReportToDatabase(report);

  } catch (const std::exception& e) {
    Logger::error(LogCategory::DATABASE, "UnusedObjectsDetector",
                  "Error detecting unused objects: " + std::string(e.what()));
  }

  return report;
}

std::vector<std::string> UnusedObjectsDetector::analyzeDependencies(
    ObjectType objectType, const std::string& schemaName, const std::string& objectName) {
  std::vector<std::string> dependencies;

  // Buscar en queries
  auto queryDeps = getDependenciesFromQueries(schemaName, objectName);
  dependencies.insert(dependencies.end(), queryDeps.begin(), queryDeps.end());

  // Buscar en workflows
  auto workflowDeps = getDependenciesFromWorkflows(schemaName, objectName);
  dependencies.insert(dependencies.end(), workflowDeps.begin(), workflowDeps.end());

  // Buscar en transformations
  auto transformDeps = getDependenciesFromTransformations(schemaName, objectName);
  dependencies.insert(dependencies.end(), transformDeps.begin(), transformDeps.end());

  return dependencies;
}

std::vector<std::string> UnusedObjectsDetector::getDependenciesFromQueries(
    const std::string& schemaName, const std::string& objectName) {
  std::vector<std::string> deps;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Buscar en query_performance o similar
    std::string searchPattern = "%" + schemaName + "." + objectName + "%";
    auto result = txn.exec_params(
        "SELECT DISTINCT query_id FROM metadata.query_performance "
        "WHERE query_text ILIKE $1 LIMIT 10",
        searchPattern);

    for (const auto& row : result) {
      deps.push_back("query:" + row["query_id"].as<std::string>());
    }
  } catch (const std::exception& e) {
    // Si la tabla no existe, ignorar
  }

  return deps;
}

std::vector<std::string> UnusedObjectsDetector::getDependenciesFromWorkflows(
    const std::string& schemaName, const std::string& objectName) {
  std::vector<std::string> deps;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Buscar en workflows que referencien esta tabla
    std::string searchPattern = "%" + schemaName + "." + objectName + "%";
    auto result = txn.exec_params(
        "SELECT DISTINCT workflow_id FROM metadata.workflows "
        "WHERE workflow_config::text ILIKE $1 LIMIT 10",
        searchPattern);

    for (const auto& row : result) {
      deps.push_back("workflow:" + row["workflow_id"].as<std::string>());
    }
  } catch (const std::exception& e) {
    // Si la tabla no existe, ignorar
  }

  return deps;
}

std::vector<std::string> UnusedObjectsDetector::getDependenciesFromTransformations(
    const std::string& schemaName, const std::string& objectName) {
  std::vector<std::string> deps;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Buscar en transformations que usen esta tabla
    std::string searchPattern = "%" + schemaName + "." + objectName + "%";
    auto result = txn.exec_params(
        "SELECT DISTINCT transformation_id FROM metadata.transformations_catalog "
        "WHERE transformation_config::text ILIKE $1 LIMIT 10",
        searchPattern);

    for (const auto& row : result) {
      deps.push_back("transformation:" + row["transformation_id"].as<std::string>());
    }
  } catch (const std::exception& e) {
    // Si la tabla no existe, ignorar
  }

  return deps;
}

std::unique_ptr<UnusedObjectsDetector::UnusedObjectsReport> UnusedObjectsDetector::getReport(
    int reportId) {
  return loadReportFromDatabase(reportId);
}

std::unique_ptr<UnusedObjectsDetector::UnusedObjectsReport>
UnusedObjectsDetector::loadReportFromDatabase(int reportId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params(
        "SELECT * FROM metadata.unused_objects_report WHERE report_id = $1", reportId);

    if (result.empty()) {
      return nullptr;
    }

    auto row = result[0];
    auto report = std::make_unique<UnusedObjectsReport>();
    report->reportId = row["report_id"].as<int>();
    report->daysThreshold = row["days_threshold"].as<int>();
    report->totalUnusedCount = row["total_unused_count"].as<int>();
    if (!row["generated_by"].is_null()) {
      report->generatedBy = row["generated_by"].as<std::string>();
    }

    if (!row["generated_at"].is_null()) {
      auto generatedStr = row["generated_at"].as<std::string>();
      std::tm tm = {};
      std::istringstream ss(generatedStr);
      ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
      report->generatedAt = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    }

    // Parse unused_objects JSONB
    if (!row["unused_objects"].is_null()) {
      json unusedObjectsJson = json::parse(row["unused_objects"].as<std::string>());
      for (const auto& obj : unusedObjectsJson) {
        UnusedObject unused;
        unused.objectType = stringToObjectType(obj["object_type"].get<std::string>());
        unused.schemaName = obj["schema_name"].get<std::string>();
        unused.objectName = obj["object_name"].get<std::string>();
        unused.daysSinceLastAccess = obj.value("days_since_last_access", 0);
        if (obj.contains("dependencies")) {
          for (const auto& dep : obj["dependencies"]) {
            unused.dependencies.push_back(dep.get<std::string>());
          }
        }
        if (obj.contains("recommendations")) {
          for (const auto& rec : obj["recommendations"]) {
            unused.recommendations.push_back(rec.get<std::string>());
          }
        }
        report->unusedObjects.push_back(unused);
      }
    }

    // Parse recommendations JSONB
    if (!row["recommendations"].is_null()) {
      json recommendationsJson = json::parse(row["recommendations"].as<std::string>());
      for (const auto& rec : recommendationsJson) {
        report->recommendations.push_back(rec.get<std::string>());
      }
    }

    return report;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::DATABASE, "UnusedObjectsDetector",
                  "Error loading report: " + std::string(e.what()));
    return nullptr;
  }
}

std::vector<UnusedObjectsDetector::UnusedObjectsReport> UnusedObjectsDetector::listReports(
    int limit) {
  std::vector<UnusedObjectsReport> reports;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params(
        "SELECT report_id FROM metadata.unused_objects_report "
        "ORDER BY generated_at DESC LIMIT $1",
        limit);

    for (const auto& row : result) {
      int reportId = row["report_id"].as<int>();
      auto report = loadReportFromDatabase(reportId);
      if (report) {
        reports.push_back(*report);
      }
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::DATABASE, "UnusedObjectsDetector",
                  "Error listing reports: " + std::string(e.what()));
  }

  return reports;
}

bool UnusedObjectsDetector::saveReportToDatabase(const UnusedObjectsReport& report) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto generatedTimeT = std::chrono::system_clock::to_time_t(report.generatedAt);
    std::tm tm = *std::localtime(&generatedTimeT);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    std::string generatedStr = oss.str();

    // Serializar unused_objects
    json unusedObjectsJson = json::array();
    for (const auto& obj : report.unusedObjects) {
      json objJson;
      objJson["object_type"] = objectTypeToString(obj.objectType);
      objJson["schema_name"] = obj.schemaName;
      objJson["object_name"] = obj.objectName;
      objJson["days_since_last_access"] = obj.daysSinceLastAccess;
      objJson["dependencies"] = json::array();
      for (const auto& dep : obj.dependencies) {
        objJson["dependencies"].push_back(dep);
      }
      objJson["recommendations"] = json::array();
      for (const auto& rec : obj.recommendations) {
        objJson["recommendations"].push_back(rec);
      }
      unusedObjectsJson.push_back(objJson);
    }

    // Serializar recommendations
    json recommendationsJson = json::array();
    for (const auto& rec : report.recommendations) {
      recommendationsJson.push_back(rec);
    }

    if (report.reportId > 0) {
      // Update
      txn.exec_params(
          "UPDATE metadata.unused_objects_report SET "
          "unused_objects = $1, recommendations = $2, total_unused_count = $3 "
          "WHERE report_id = $4",
          unusedObjectsJson.dump(), recommendationsJson.dump(),
          report.totalUnusedCount, report.reportId);
    } else {
      // Insert
      auto result = txn.exec_params(
          "INSERT INTO metadata.unused_objects_report "
          "(generated_at, days_threshold, unused_objects, recommendations, "
          "total_unused_count, generated_by) "
          "VALUES ($1::timestamp, $2, $3, $4, $5, $6) "
          "RETURNING report_id",
          generatedStr, report.daysThreshold, unusedObjectsJson.dump(),
          recommendationsJson.dump(), report.totalUnusedCount,
          report.generatedBy.empty() ? nullptr : report.generatedBy);

      if (!result.empty()) {
        // reportId will be set by caller if needed
      }
    }

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::DATABASE, "UnusedObjectsDetector",
                  "Error saving report: " + std::string(e.what()));
    return false;
  }
}
