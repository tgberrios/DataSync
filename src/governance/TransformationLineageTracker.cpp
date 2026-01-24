#include "governance/TransformationLineageTracker.h"
#include <pqxx/pqxx>
#include "core/logger.h"
#include <sstream>
#include <iomanip>

TransformationLineageTracker::TransformationLineageTracker(
    const std::string& connectionString)
    : connectionString_(connectionString) {
}

void TransformationLineageTracker::recordTransformation(
    const TransformationRecord& record) {
  
  saveToDatabase(record);
  
  Logger::debug(LogCategory::GOVERNANCE, "TransformationLineageTracker",
                "Recorded transformation: " + record.transformationType + 
                " for workflow: " + record.workflowName);
}

std::vector<TransformationLineageTracker::TransformationRecord> 
TransformationLineageTracker::getTransformationLineage(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  std::vector<TransformationRecord> records;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = 
        "SELECT * FROM metadata.transformation_lineage "
        "WHERE output_schema = $1 AND output_table = $2";

    std::vector<std::string> params = {schemaName, tableName};

    if (!columnName.empty()) {
      query += " AND $3 = ANY(output_columns)";
      params.push_back(columnName);
    }

    query += " ORDER BY executed_at DESC";

    auto result = txn.exec_params(query, params);
    for (const auto& row : result) {
      records.push_back(loadFromDatabaseRow(row));
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TransformationLineageTracker",
                  "Error getting transformation lineage: " + std::string(e.what()));
  }

  return records;
}

std::vector<TransformationLineageTracker::TransformationRecord> 
TransformationLineageTracker::getWorkflowTransformationHistory(
    const std::string& workflowName,
    int64_t executionId) {
  
  std::vector<TransformationRecord> records;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = 
        "SELECT * FROM metadata.transformation_lineage "
        "WHERE workflow_name = $1";

    std::vector<std::string> params = {workflowName};

    if (executionId > 0) {
      query += " AND workflow_execution_id = $2";
      params.push_back(std::to_string(executionId));
    }

    query += " ORDER BY executed_at ASC";

    auto result = txn.exec_params(query, params);
    for (const auto& row : result) {
      records.push_back(loadFromDatabaseRow(row));
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TransformationLineageTracker",
                  "Error getting workflow transformation history: " + 
                  std::string(e.what()));
  }

  return records;
}

std::vector<TransformationLineageTracker::TransformationRecord> 
TransformationLineageTracker::getTransformationPipeline(
    const std::string& workflowName,
    int64_t executionId) {
  
  // Similar a getWorkflowTransformationHistory pero ordenado por dependencias
  return getWorkflowTransformationHistory(workflowName, executionId);
}

std::vector<TransformationLineageTracker::TransformationRecord> 
TransformationLineageTracker::getProducingTransformations(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  return getTransformationLineage(schemaName, tableName, columnName);
}

std::vector<TransformationLineageTracker::TransformationRecord> 
TransformationLineageTracker::getConsumingTransformations(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  std::vector<TransformationRecord> records;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = 
        "SELECT * FROM metadata.transformation_lineage "
        "WHERE $1 = ANY(input_schemas) AND $2 = ANY(input_tables)";

    std::vector<std::string> params = {schemaName, tableName};

    if (!columnName.empty()) {
      query += " AND $3 = ANY(input_columns)";
      params.push_back(columnName);
    }

    query += " ORDER BY executed_at DESC";

    auto result = txn.exec_params(query, params);
    for (const auto& row : result) {
      records.push_back(loadFromDatabaseRow(row));
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TransformationLineageTracker",
                  "Error getting consuming transformations: " + 
                  std::string(e.what()));
  }

  return records;
}

void TransformationLineageTracker::saveToDatabase(
    const TransformationRecord& record) {
  
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Convertir vectores a arrays PostgreSQL
    std::stringstream inputSchemasStr, inputTablesStr, inputColumnsStr;
    std::stringstream outputSchemasStr, outputTablesStr, outputColumnsStr;

    for (size_t i = 0; i < record.inputSchemas.size(); ++i) {
      if (i > 0) inputSchemasStr << ",";
      inputSchemasStr << "\"" << record.inputSchemas[i] << "\"";
    }

    for (size_t i = 0; i < record.inputTables.size(); ++i) {
      if (i > 0) inputTablesStr << ",";
      inputTablesStr << "\"" << record.inputTables[i] << "\"";
    }

    for (size_t i = 0; i < record.inputColumns.size(); ++i) {
      if (i > 0) inputColumnsStr << ",";
      inputColumnsStr << "\"" << record.inputColumns[i] << "\"";
    }

    for (size_t i = 0; i < record.outputSchemas.size(); ++i) {
      if (i > 0) outputSchemasStr << ",";
      outputSchemasStr << "\"" << record.outputSchemas[i] << "\"";
    }

    for (size_t i = 0; i < record.outputTables.size(); ++i) {
      if (i > 0) outputTablesStr << ",";
      outputTablesStr << "\"" << record.outputTables[i] << "\"";
    }

    for (size_t i = 0; i < record.outputColumns.size(); ++i) {
      if (i > 0) outputColumnsStr << ",";
      outputColumnsStr << "\"" << record.outputColumns[i] << "\"";
    }

    auto executedAtTimeT = std::chrono::system_clock::to_time_t(record.executedAt);

    txn.exec_params(
        "INSERT INTO metadata.transformation_lineage "
        "(transformation_id, transformation_type, transformation_config, "
        "workflow_name, task_name, workflow_execution_id, task_execution_id, "
        "input_schemas, input_tables, input_columns, "
        "output_schemas, output_tables, output_columns, "
        "rows_processed, execution_time_ms, success, error_message, executed_at) "
        "VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, "
        "ARRAY[" + inputSchemasStr.str() + "]::text[], "
        "ARRAY[" + inputTablesStr.str() + "]::text[], "
        "ARRAY[" + inputColumnsStr.str() + "]::text[], "
        "ARRAY[" + outputSchemasStr.str() + "]::text[], "
        "ARRAY[" + outputTablesStr.str() + "]::text[], "
        "ARRAY[" + outputColumnsStr.str() + "]::text[], "
        "$8, $9, $10, $11, to_timestamp($12))",
        record.transformationId,
        record.transformationType,
        record.transformationConfig.dump(),
        record.workflowName,
        record.taskName.empty() ? nullptr : record.taskName,
        record.workflowExecutionId,
        record.taskExecutionId,
        static_cast<int64_t>(record.rowsProcessed),
        record.executionTimeMs,
        record.success,
        record.errorMessage.empty() ? nullptr : record.errorMessage,
        executedAtTimeT
    );

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TransformationLineageTracker",
                  "Error saving transformation record: " + std::string(e.what()));
  }
}

TransformationLineageTracker::TransformationRecord 
TransformationLineageTracker::loadFromDatabaseRow(const pqxx::row& row) {
  
  TransformationRecord record;
  
  record.transformationId = row["transformation_id"].as<std::string>();
  record.transformationType = row["transformation_type"].as<std::string>();
  record.workflowName = row["workflow_name"].as<std::string>();
  if (!row["task_name"].is_null()) {
    record.taskName = row["task_name"].as<std::string>();
  }
  record.workflowExecutionId = row["workflow_execution_id"].as<int64_t>();
  record.taskExecutionId = row["task_execution_id"].as<int64_t>();
  
  // Parsear JSON config
  if (!row["transformation_config"].is_null()) {
    record.transformationConfig = json::parse(
        row["transformation_config"].as<std::string>());
  }

  // Parsear arrays - leer como string y parsear manualmente
  auto parseArray = [](const std::string& arrayStr) -> std::vector<std::string> {
    std::vector<std::string> result;
    if (arrayStr.empty() || arrayStr == "{}") {
      return result;
    }
    // Formato: {val1,val2,val3} o {"val1","val2","val3"}
    std::string content = arrayStr;
    if (content.front() == '{' && content.back() == '}') {
      content = content.substr(1, content.length() - 2);
    }
    std::istringstream iss(content);
    std::string item;
    while (std::getline(iss, item, ',')) {
      // Remover comillas si existen
      if (!item.empty()) {
        if (item.front() == '"' && item.back() == '"') {
          item = item.substr(1, item.length() - 2);
        }
        result.push_back(item);
      }
    }
    return result;
  };

  if (!row["input_schemas"].is_null()) {
    record.inputSchemas = parseArray(row["input_schemas"].as<std::string>());
  }

  if (!row["input_tables"].is_null()) {
    record.inputTables = parseArray(row["input_tables"].as<std::string>());
  }

  if (!row["input_columns"].is_null()) {
    record.inputColumns = parseArray(row["input_columns"].as<std::string>());
  }

  if (!row["output_schemas"].is_null()) {
    record.outputSchemas = parseArray(row["output_schemas"].as<std::string>());
  }

  if (!row["output_tables"].is_null()) {
    record.outputTables = parseArray(row["output_tables"].as<std::string>());
  }

  if (!row["output_columns"].is_null()) {
    record.outputColumns = parseArray(row["output_columns"].as<std::string>());
  }

  record.rowsProcessed = row["rows_processed"].as<size_t>();
  record.executionTimeMs = row["execution_time_ms"].as<double>();
  record.success = row["success"].as<bool>();
  if (!row["error_message"].is_null()) {
    record.errorMessage = row["error_message"].as<std::string>();
  }

  auto executedAtTimeT = row["executed_at"].as<std::time_t>();
  record.executedAt = std::chrono::system_clock::from_time_t(executedAtTimeT);

  return record;
}
