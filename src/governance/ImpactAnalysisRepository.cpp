#include "governance/ImpactAnalysisRepository.h"
#include "core/logger.h"

ImpactAnalysisRepository::ImpactAnalysisRepository(pqxx::connection& conn)
    : conn_(conn) {
}

int ImpactAnalysisRepository::saveAnalysisResult(
    const ImpactAnalyzer::ImpactResult& result,
    const std::string& changeType,
    const std::string& userId) {
  
  try {
    pqxx::work txn(conn_);

    // Crear ImpactAnalyzer temporal para generar el reporte
    // Necesitamos una connection string, pero no la tenemos aquí
    // Generar reporte manualmente
    json report;
    report["resource_type"] = result.resourceType;
    report["schema_name"] = result.schemaName;
    report["table_name"] = result.tableName;
    if (!result.columnName.empty()) {
      report["column_name"] = result.columnName;
    }
    report["downstream_impact"] = json::object();
    report["downstream_impact"]["affected_tables"] = result.affectedTables;
    report["downstream_impact"]["affected_workflows"] = result.affectedWorkflows;
    report["downstream_impact"]["affected_transformaciones"] = result.affectedTransformations;
    report["downstream_impact"]["affected_columns"] = result.affectedColumns;
    report["upstream_impact"] = json::object();
    report["upstream_impact"]["dependent_tables"] = result.dependentTables;
    report["upstream_impact"]["dependent_workflows"] = result.dependentWorkflows;
    report["upstream_impact"]["dependent_transformaciones"] = result.dependentTransformations;
    report["statistics"] = json::object();
    report["statistics"]["total_downstream"] = result.totalDownstreamImpact;
    report["statistics"]["total_upstream"] = result.totalUpstreamImpact;
    report["statistics"]["confidence_score"] = result.confidenceScore;
    
    auto row = txn.exec_params1(
        "INSERT INTO metadata.impact_analysis "
        "(schema_name, table_name, column_name, resource_type, change_type, "
        "analysis_result, downstream_count, upstream_count, confidence_score, "
        "created_by, created_at) "
        "VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, NOW()) "
        "RETURNING id",
        result.schemaName,
        result.tableName,
        result.columnName.empty() ? nullptr : result.columnName,
        result.resourceType,
        changeType.empty() ? nullptr : changeType,
        report.dump(),
        static_cast<int>(result.totalDownstreamImpact),
        static_cast<int>(result.totalUpstreamImpact),
        result.confidenceScore,
        userId.empty() ? nullptr : userId
    );
    int analysisId = row[0].as<int>();

    txn.commit();
    return analysisId;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "ImpactAnalysisRepository",
                  "Error saving analysis result: " + std::string(e.what()));
    return -1;
  }
}

ImpactAnalyzer::ImpactResult ImpactAnalysisRepository::getAnalysisResult(int analysisId) {
  ImpactAnalyzer::ImpactResult result;

  try {
    pqxx::work txn(conn_);

    auto row = txn.exec_params1(
        "SELECT * FROM metadata.impact_analysis WHERE id = $1",
        analysisId
    );

    result.schemaName = row["schema_name"].as<std::string>();
    result.tableName = row["table_name"].as<std::string>();
    if (!row["column_name"].is_null()) {
      result.columnName = row["column_name"].as<std::string>();
    }
    result.resourceType = row["resource_type"].as<std::string>();
    result.totalDownstreamImpact = row["downstream_count"].as<size_t>();
    result.totalUpstreamImpact = row["upstream_count"].as<size_t>();
    result.confidenceScore = row["confidence_score"].as<double>();

    // Parsear JSON de resultados
    json report = json::parse(row["analysis_result"].as<std::string>());
    if (report.contains("downstream_impact")) {
      const auto& downstream = report["downstream_impact"];
      if (downstream.contains("affected_tables")) {
        for (const auto& table : downstream["affected_tables"]) {
          result.affectedTables.push_back(table.get<std::string>());
        }
      }
      if (downstream.contains("affected_workflows")) {
        for (const auto& workflow : downstream["affected_workflows"]) {
          result.affectedWorkflows.push_back(workflow.get<std::string>());
        }
      }
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "ImpactAnalysisRepository",
                  "Error getting analysis result: " + std::string(e.what()));
  }

  return result;
}

std::vector<json> ImpactAnalysisRepository::getAnalysisHistory(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    int limit) {
  
  std::vector<json> history;

  try {
    pqxx::work txn(conn_);

    std::string query = 
        "SELECT id, schema_name, table_name, column_name, resource_type, "
        "change_type, downstream_count, upstream_count, confidence_score, "
        "created_at, created_by "
        "FROM metadata.impact_analysis "
        "WHERE schema_name = $1 AND table_name = $2";

    std::vector<std::string> params = {schemaName, tableName};

    if (!columnName.empty()) {
      query += " AND column_name = $3";
      params.push_back(columnName);
    }

    query += " ORDER BY created_at DESC LIMIT $" + std::to_string(params.size() + 1);
    params.push_back(std::to_string(limit));

    auto result = txn.exec_params(query, params);
    for (const auto& row : result) {
      json entry;
      entry["id"] = row["id"].as<int>();
      entry["schema_name"] = row["schema_name"].as<std::string>();
      entry["table_name"] = row["table_name"].as<std::string>();
      if (!row["column_name"].is_null()) {
        entry["column_name"] = row["column_name"].as<std::string>();
      }
      entry["resource_type"] = row["resource_type"].as<std::string>();
      if (!row["change_type"].is_null()) {
        entry["change_type"] = row["change_type"].as<std::string>();
      }
      entry["downstream_count"] = row["downstream_count"].as<int>();
      entry["upstream_count"] = row["upstream_count"].as<int>();
      entry["confidence_score"] = row["confidence_score"].as<double>();
      entry["created_at"] = row["created_at"].as<std::string>();
      if (!row["created_by"].is_null()) {
        entry["created_by"] = row["created_by"].as<std::string>();
      }
      history.push_back(entry);
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "ImpactAnalysisRepository",
                  "Error getting analysis history: " + std::string(e.what()));
  }

  return history;
}

void ImpactAnalysisRepository::cleanupOldAnalyses(int daysToKeep) {
  try {
    pqxx::work txn(conn_);

    auto result = txn.exec_params(
        "DELETE FROM metadata.impact_analysis "
        "WHERE created_at < NOW() - INTERVAL '" + std::to_string(daysToKeep) + " days'"
    );

    txn.commit();

    if (result.affected_rows() > 0) {
      Logger::info(LogCategory::GOVERNANCE, "ImpactAnalysisRepository",
                   "Cleaned up " + std::to_string(result.affected_rows()) + 
                   " old analysis results");
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "ImpactAnalysisRepository",
                  "Error cleaning up old analyses: " + std::string(e.what()));
  }
}

void ImpactAnalysisRepository::initializeTables(pqxx::connection& conn) {
  try {
    pqxx::work txn(conn);

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.impact_analysis ("
        "id SERIAL PRIMARY KEY,"
        "schema_name VARCHAR(255) NOT NULL,"
        "table_name VARCHAR(255) NOT NULL,"
        "column_name VARCHAR(255),"
        "resource_type VARCHAR(50) NOT NULL,"
        "change_type VARCHAR(50),"
        "analysis_result JSONB NOT NULL,"
        "downstream_count INTEGER DEFAULT 0,"
        "upstream_count INTEGER DEFAULT 0,"
        "confidence_score DECIMAL(3,2) DEFAULT 1.0,"
        "created_by VARCHAR(255),"
        "created_at TIMESTAMP DEFAULT NOW()"
        ")"
    );

    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_impact_analysis_table "
        "ON metadata.impact_analysis(schema_name, table_name)"
    );

    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_impact_analysis_column "
        "ON metadata.impact_analysis(schema_name, table_name, column_name)"
    );

    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_impact_analysis_created "
        "ON metadata.impact_analysis(created_at)"
    );

    txn.commit();
    Logger::info(LogCategory::GOVERNANCE, "ImpactAnalysisRepository",
                 "Impact analysis tables initialized");
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "ImpactAnalysisRepository",
                  "Error initializing tables: " + std::string(e.what()));
    throw;
  }
}
