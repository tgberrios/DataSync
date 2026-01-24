#include "governance/ImpactAnalyzer.h"
#include <pqxx/pqxx>
#include <algorithm>
#include <sstream>

ImpactAnalyzer::ImpactAnalyzer(const std::string& connectionString)
    : connectionString_(connectionString) {
}

ImpactAnalyzer::ImpactResult ImpactAnalyzer::analyzeDownstreamImpact(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    const AnalysisConfig& config) {
  
  ImpactResult result;
  result.resourceType = columnName.empty() ? "table" : "column";
  result.schemaName = schemaName;
  result.tableName = tableName;
  result.columnName = columnName;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Encontrar tablas afectadas desde lineage
    if (config.includeLineage) {
      result.affectedTables = findAffectedTablesFromLineage(
          schemaName, tableName, columnName, true);
    }

    // Encontrar workflows afectados
    if (config.includeWorkflows) {
      result.affectedWorkflows = findAffectedWorkflows(
          schemaName, tableName, columnName);
    }

    // Encontrar transformaciones afectadas
    if (config.includeTransformations) {
      result.affectedTransformations = findAffectedTransformations(
          schemaName, tableName, columnName);
    }

    // Encontrar columnas afectadas
    if (config.includeColumns && !columnName.empty()) {
      result.affectedColumns = findAffectedColumns(
          schemaName, tableName, columnName);
    }

    result.totalDownstreamImpact = 
        result.affectedTables.size() +
        result.affectedWorkflows.size() +
        result.affectedTransformations.size() +
        result.affectedColumns.size();

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "ImpactAnalyzer",
                  "Error analyzing downstream impact: " + std::string(e.what()));
  }

  return result;
}

ImpactAnalyzer::ImpactResult ImpactAnalyzer::analyzeUpstreamImpact(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    const AnalysisConfig& config) {
  
  ImpactResult result;
  result.resourceType = columnName.empty() ? "table" : "column";
  result.schemaName = schemaName;
  result.tableName = tableName;
  result.columnName = columnName;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Encontrar tablas dependientes desde lineage (upstream)
    if (config.includeLineage) {
      result.dependentTables = findAffectedTablesFromLineage(
          schemaName, tableName, columnName, false);
    }

    // Encontrar workflows que dependen de este recurso
    if (config.includeWorkflows) {
      // Similar a downstream pero buscando dependencias inversas
      result.dependentWorkflows = findAffectedWorkflows(
          schemaName, tableName, columnName);
    }

    result.totalUpstreamImpact = 
        result.dependentTables.size() +
        result.dependentWorkflows.size();

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "ImpactAnalyzer",
                  "Error analyzing upstream impact: " + std::string(e.what()));
  }

  return result;
}

ImpactAnalyzer::ImpactResult ImpactAnalyzer::analyzeFullImpact(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    const AnalysisConfig& config) {
  
  ImpactResult downstream = analyzeDownstreamImpact(
      schemaName, tableName, columnName, config);
  ImpactResult upstream = analyzeUpstreamImpact(
      schemaName, tableName, columnName, config);

  // Combinar resultados
  ImpactResult result = downstream;
  result.dependentTables = upstream.dependentTables;
  result.dependentWorkflows = upstream.dependentWorkflows;
  result.totalUpstreamImpact = upstream.totalUpstreamImpact;

  return result;
}

ImpactAnalyzer::ImpactResult ImpactAnalyzer::analyzeSchemaChangeImpact(
    const std::string& schemaName,
    const std::string& changeType,
    const std::string& tableName,
    const std::string& columnName) {
  
  ImpactResult result;
  result.resourceType = "schema_change";
  result.schemaName = schemaName;
  result.tableName = tableName;
  result.columnName = columnName;

  // Analizar impacto según tipo de cambio
  if (changeType == "drop_table" || changeType == "rename_table") {
    result = analyzeFullImpact(schemaName, tableName, "", AnalysisConfig());
  } else if (changeType == "alter_column" || changeType == "drop_column") {
    result = analyzeFullImpact(schemaName, tableName, columnName, AnalysisConfig());
  } else if (changeType == "add_column") {
    // Nuevas columnas típicamente no tienen impacto downstream
    result = analyzeUpstreamImpact(schemaName, tableName, columnName, AnalysisConfig());
  }

  return result;
}

json ImpactAnalyzer::generateImpactReport(const ImpactResult& result) {
  json report;
  report["resource_type"] = result.resourceType;
  report["schema_name"] = result.schemaName;
  report["table_name"] = result.tableName;
  report["column_name"] = result.columnName;
  
  report["downstream_impact"] = json::object();
  report["downstream_impact"]["affected_tables"] = result.affectedTables;
  report["downstream_impact"]["affected_workflows"] = result.affectedWorkflows;
  report["downstream_impact"]["affected_transformations"] = result.affectedTransformations;
  report["downstream_impact"]["affected_columns"] = result.affectedColumns;
  report["downstream_impact"]["total_count"] = result.totalDownstreamImpact;
  
  report["upstream_impact"] = json::object();
  report["upstream_impact"]["dependent_tables"] = result.dependentTables;
  report["upstream_impact"]["dependent_workflows"] = result.dependentWorkflows;
  report["upstream_impact"]["total_count"] = result.totalUpstreamImpact;
  
  report["confidence_score"] = result.confidenceScore;
  report["dependency_details"] = result.dependencyDetails;

  return report;
}

std::vector<std::string> ImpactAnalyzer::findAffectedWorkflows(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  std::vector<std::string> workflows;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Buscar workflows que referencian esta tabla
    std::string query = 
        "SELECT DISTINCT w.workflow_name "
        "FROM metadata.workflows w "
        "JOIN metadata.workflow_tasks wt ON w.workflow_name = wt.workflow_name "
        "WHERE wt.task_config::text LIKE '%" + schemaName + "." + tableName + "%'";

    auto result = txn.exec(query);
    for (const auto& row : result) {
      workflows.push_back(row["workflow_name"].as<std::string>());
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "ImpactAnalyzer",
                  "Error finding affected workflows: " + std::string(e.what()));
  }

  return workflows;
}

std::vector<std::string> ImpactAnalyzer::findAffectedTransformations(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  std::vector<std::string> transformations;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Buscar transformaciones que usan esta tabla/columna
    std::string query = 
        "SELECT DISTINCT transformation_name "
        "FROM metadata.transformations "
        "WHERE transformation_config::text LIKE '%" + schemaName + "." + tableName + "%'";

    if (!columnName.empty()) {
      query += " AND transformation_config::text LIKE '%" + columnName + "%'";
    }

    auto result = txn.exec(query);
    for (const auto& row : result) {
      transformations.push_back(row["transformation_name"].as<std::string>());
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "ImpactAnalyzer",
                  "Error finding affected transformations: " + std::string(e.what()));
  }

  return transformations;
}

std::vector<std::string> ImpactAnalyzer::findAffectedTablesFromLineage(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    bool downstream) {
  
  std::vector<std::string> tables;
  std::set<std::string> visited;
  
  traverseLineage(schemaName, tableName, columnName, visited, tables,
                  downstream, 0, 10);

  return tables;
}

std::vector<std::string> ImpactAnalyzer::findAffectedColumns(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  std::vector<std::string> columns;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Buscar columnas que dependen de esta columna desde lineage
    std::string query = 
        "SELECT DISTINCT target_column_name "
        "FROM metadata.data_lineage_mssql "
        "WHERE schema_name = $1 AND object_name = $2 AND column_name = $3 "
        "UNION "
        "SELECT DISTINCT target_column_name "
        "FROM metadata.data_lineage_mariadb "
        "WHERE schema_name = $1 AND object_name = $2 AND column_name = $3";

    auto result = txn.exec_params(query, schemaName, tableName, columnName);
    for (const auto& row : result) {
      if (!row["target_column_name"].is_null()) {
        std::string targetCol = row["target_column_name"].as<std::string>();
        columns.push_back(targetCol);
      }
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "ImpactAnalyzer",
                  "Error finding affected columns: " + std::string(e.what()));
  }

  return columns;
}

void ImpactAnalyzer::traverseLineage(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    std::set<std::string>& visited,
    std::vector<std::string>& results,
    bool downstream,
    int depth,
    int maxDepth) {
  
  if (depth >= maxDepth) {
    return;
  }

  std::string key = schemaName + "." + tableName;
  if (visited.find(key) != visited.end()) {
    return;  // Evitar ciclos
  }
  visited.insert(key);

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Buscar en diferentes tablas de lineage según dirección
    std::string query;
    if (downstream) {
      // Buscar tablas que dependen de esta (target)
      query = 
          "SELECT DISTINCT target_schema_name, target_table_name "
          "FROM ("
          "  SELECT target_schema_name, target_table_name "
          "  FROM metadata.data_lineage_mssql "
          "  WHERE schema_name = $1 AND object_name = $2 "
          "  UNION "
          "  SELECT target_schema_name, target_table_name "
          "  FROM metadata.data_lineage_mariadb "
          "  WHERE schema_name = $1 AND object_name = $2"
          ") AS lineage";
    } else {
      // Buscar tablas de las que depende esta (source)
      query = 
          "SELECT DISTINCT schema_name, object_name "
          "FROM ("
          "  SELECT schema_name, object_name "
          "  FROM metadata.data_lineage_mssql "
          "  WHERE target_schema_name = $1 AND target_object_name = $2 "
          "  UNION "
          "  SELECT schema_name, object_name "
          "  FROM metadata.data_lineage_mariadb "
          "  WHERE target_schema_name = $1 AND target_object_name = $2"
          ") AS lineage";
    }

    auto result = txn.exec_params(query, schemaName, tableName);
    for (const auto& row : result) {
      std::string targetSchema = row[downstream ? "target_schema_name" : "schema_name"].as<std::string>();
      std::string targetTable = row[downstream ? "target_table_name" : "object_name"].as<std::string>();
      std::string targetKey = targetSchema + "." + targetTable;
      
      if (visited.find(targetKey) == visited.end()) {
        results.push_back(targetKey);
        traverseLineage(targetSchema, targetTable, "", visited, results,
                       downstream, depth + 1, maxDepth);
      }
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "ImpactAnalyzer",
                  "Error traversing lineage: " + std::string(e.what()));
  }
}
