#include "transformations/spark_translator.h"
#include "core/logger.h"
#include <sstream>
#include <algorithm>
#include <regex>

SparkTranslator::TranslationResult
SparkTranslator::translateTransformation(const json& transformationConfig) {
  TranslationResult result;
  
  if (!transformationConfig.contains("type") || !transformationConfig["type"].is_string()) {
    Logger::error(LogCategory::TRANSFORM, "SparkTranslator",
                 "Transformation config missing 'type' field");
    result.sparkSQL = "SELECT * FROM input_table";
    return result;
  }

  std::string type = transformationConfig["type"].get<std::string>();
  json config = transformationConfig.contains("config") ? 
                transformationConfig["config"] : json::object();

  if (type == "join") {
    result.sparkSQL = translateJoin(config);
  } else if (type == "aggregate") {
    result.sparkSQL = translateAggregate(config);
  } else if (type == "filter") {
    result.sparkSQL = translateFilter(config);
  } else if (type == "sort" || type == "sorter") {
    result.sparkSQL = translateSort(config);
  } else if (type == "expression") {
    result.sparkSQL = translateExpression(config);
  } else if (type == "lookup") {
    result.sparkSQL = translateLookup(config);
  } else if (type == "union") {
    result.sparkSQL = translateUnion(config);
  } else if (type == "window" || type == "window_functions") {
    result.sparkSQL = translateWindowFunction(config);
  } else if (type == "deduplication" || type == "deduplicate") {
    result.sparkSQL = translateDeduplication(config);
  } else if (type == "router") {
    result.sparkSQL = translateRouter(config);
  } else if (type == "normalizer" || type == "normalize") {
    result.sparkSQL = translateNormalizer(config);
  } else if (type == "data_cleansing" || type == "cleansing") {
    result.sparkSQL = translateDataCleansing(config);
  } else if (type == "data_validation" || type == "validation") {
    result.sparkSQL = translateDataValidation(config);
  } else {
    Logger::warning(LogCategory::TRANSFORM, "SparkTranslator",
                   "Unknown transformation type: " + type + ", using passthrough");
    result.sparkSQL = "SELECT * FROM input_table";
  }

  result.metadata["transformation_type"] = type;
  result.metadata["translated"] = true;
  
  return result;
}

SparkTranslator::TranslationResult
SparkTranslator::translatePipeline(const json& pipelineConfig) {
  TranslationResult result;
  
  if (!pipelineConfig.contains("transformations") || 
      !pipelineConfig["transformations"].is_array()) {
    Logger::error(LogCategory::TRANSFORM, "SparkTranslator",
                 "Pipeline config missing 'transformations' array");
    result.sparkSQL = "SELECT * FROM input_table";
    return result;
  }

  std::vector<std::string> sqlParts;
  std::string currentTable = "input_table";
  int step = 0;

  for (const auto& transformConfig : pipelineConfig["transformations"]) {
    TranslationResult stepResult = translateTransformation(transformConfig);
    
    // Crear temp view para el resultado de este paso
    std::string tempView = "temp_table_" + std::to_string(step);
    result.tempViews.push_back(tempView);
    
    // Reemplazar referencias a tablas en el SQL generado
    std::string stepSQL = stepResult.sparkSQL;
    std::regex tableRegex(R"(\b(input_table|left_table|right_table)\b)");
    stepSQL = std::regex_replace(stepSQL, tableRegex, currentTable);
    
    // Agregar CREATE TEMP VIEW
    sqlParts.push_back("CREATE OR REPLACE TEMP VIEW " + tempView + " AS\n" + stepSQL);
    
    currentTable = tempView;
    step++;
  }
  
  // SQL final selecciona de la última temp view
  sqlParts.push_back("SELECT * FROM " + currentTable);
  
  // Combinar todas las partes
  std::ostringstream finalSQL;
  for (size_t i = 0; i < sqlParts.size(); ++i) {
    finalSQL << sqlParts[i];
    if (i < sqlParts.size() - 1) {
      finalSQL << ";\n\n";
    }
  }
  
  result.sparkSQL = finalSQL.str();
  result.metadata["steps"] = step;
  result.metadata["temp_views_created"] = result.tempViews.size();
  
  return result;
}

std::string SparkTranslator::translateJoin(const json& config) {
  std::ostringstream sql;
  
  std::string joinType = config.value("join_type", "inner");
  std::string leftTable = config.value("left_table", "input_table");
  std::string rightTable = config.value("right_table", "lookup_table");
  
  std::vector<std::string> leftColumns;
  std::vector<std::string> rightColumns;
  
  if (config.contains("left_columns") && config["left_columns"].is_array()) {
    for (const auto& col : config["left_columns"]) {
      leftColumns.push_back(col.get<std::string>());
    }
  }
  
  if (config.contains("right_columns") && config["right_columns"].is_array()) {
    for (const auto& col : config["right_columns"]) {
      rightColumns.push_back(col.get<std::string>());
    }
  }
  
  sql << "SELECT ";
  
  // Columnas del left table
  if (config.contains("select_columns") && config["select_columns"].is_array()) {
    bool first = true;
    for (const auto& col : config["select_columns"]) {
      if (!first) sql << ", ";
      sql << escapeSQLIdentifier(col.get<std::string>());
      first = false;
    }
  } else {
    sql << leftTable << ".*";
    if (joinType != "inner") {
      sql << ", " << rightTable << ".*";
    }
  }
  
  sql << "\nFROM " << escapeSQLIdentifier(leftTable) << "\n";
  
  // Tipo de join
  if (joinType == "left" || joinType == "left_outer") {
    sql << "LEFT JOIN ";
  } else if (joinType == "right" || joinType == "right_outer") {
    sql << "RIGHT JOIN ";
  } else if (joinType == "full" || joinType == "full_outer") {
    sql << "FULL OUTER JOIN ";
  } else {
    sql << "INNER JOIN ";
  }
  
  sql << escapeSQLIdentifier(rightTable) << "\n";
  sql << "ON ";
  
  // Condición de join
  if (config.contains("join_condition")) {
    sql << config["join_condition"].get<std::string>();
  } else if (!leftColumns.empty() && !rightColumns.empty() && 
             leftColumns.size() == rightColumns.size()) {
    // Join por columnas correspondientes
    for (size_t i = 0; i < leftColumns.size(); ++i) {
      if (i > 0) sql << " AND ";
      sql << leftTable << "." << escapeSQLIdentifier(leftColumns[i]) << " = "
          << rightTable << "." << escapeSQLIdentifier(rightColumns[i]);
    }
  } else {
    sql << "1=1"; // Fallback
  }
  
  return sql.str();
}

std::string SparkTranslator::translateAggregate(const json& config) {
  std::ostringstream sql;
  
  sql << "SELECT ";
  
  // GROUP BY columns
  std::vector<std::string> groupByCols;
  if (config.contains("group_by") && config["group_by"].is_array()) {
    for (const auto& col : config["group_by"]) {
      groupByCols.push_back(col.get<std::string>());
    }
  }
  
  // Agregar columnas de GROUP BY
  bool first = true;
  for (const auto& col : groupByCols) {
    if (!first) sql << ", ";
    sql << escapeSQLIdentifier(col);
    first = false;
  }
  
  // Aggregate functions
  if (config.contains("aggregations") && config["aggregations"].is_array()) {
    for (const auto& agg : config["aggregations"]) {
      if (!first) sql << ", ";
      
      std::string func = agg.value("function", "SUM");
      std::string column = agg.value("column", "");
      std::string alias = agg.value("alias", "");
      
      std::transform(func.begin(), func.end(), func.begin(), ::toupper);
      
      sql << func << "(" << escapeSQLIdentifier(column) << ")";
      if (!alias.empty()) {
        sql << " AS " << escapeSQLIdentifier(alias);
      }
      first = false;
    }
  }
  
  sql << "\nFROM input_table\n";
  
  if (!groupByCols.empty()) {
    sql << "GROUP BY ";
    for (size_t i = 0; i < groupByCols.size(); ++i) {
      if (i > 0) sql << ", ";
      sql << escapeSQLIdentifier(groupByCols[i]);
    }
  }
  
  return sql.str();
}

std::string SparkTranslator::translateFilter(const json& config) {
  std::ostringstream sql;
  
  sql << "SELECT *\nFROM input_table\nWHERE ";
  
  if (config.contains("condition")) {
    sql << config["condition"].get<std::string>();
  } else if (config.contains("filter_expression")) {
    sql << config["filter_expression"].get<std::string>();
  } else {
    sql << "1=1"; // No filter
  }
  
  return sql.str();
}

std::string SparkTranslator::translateSort(const json& config) {
  std::ostringstream sql;
  
  sql << "SELECT *\nFROM input_table\nORDER BY ";
  
  std::vector<std::string> sortColumns;
  if (config.contains("sort_columns") && config["sort_columns"].is_array()) {
    for (const auto& col : config["sort_columns"]) {
      sortColumns.push_back(col.get<std::string>());
    }
  } else if (config.contains("order_by") && config["order_by"].is_array()) {
    for (const auto& col : config["order_by"]) {
      sortColumns.push_back(col.get<std::string>());
    }
  }
  
  std::string order = config.value("order", "ASC");
  std::transform(order.begin(), order.end(), order.begin(), ::toupper);
  
  for (size_t i = 0; i < sortColumns.size(); ++i) {
    if (i > 0) sql << ", ";
    sql << escapeSQLIdentifier(sortColumns[i]) << " " << order;
  }
  
  if (sortColumns.empty()) {
    sql << "1"; // Fallback
  }
  
  return sql.str();
}

std::string SparkTranslator::translateExpression(const json& config) {
  std::ostringstream sql;
  
  sql << "SELECT ";
  
  if (config.contains("expressions") && config["expressions"].is_array()) {
    bool first = true;
    for (const auto& expr : config["expressions"]) {
      if (!first) sql << ", ";
      
      if (expr.is_string()) {
        sql << expr.get<std::string>();
      } else if (expr.is_object()) {
        std::string expression = expr.value("expression", "");
        std::string alias = expr.value("alias", "");
        sql << expression;
        if (!alias.empty()) {
          sql << " AS " << escapeSQLIdentifier(alias);
        }
      }
      first = false;
    }
  } else {
    sql << "*";
  }
  
  sql << "\nFROM input_table";
  
  return sql.str();
}

std::string SparkTranslator::translateLookup(const json& config) {
  // Lookup es similar a un join LEFT
  json joinConfig = config;
  joinConfig["join_type"] = "left";
  joinConfig["left_table"] = "input_table";
  joinConfig["right_table"] = config.value("lookup_table", "lookup_table");
  
  return translateJoin(joinConfig);
}

std::string SparkTranslator::translateUnion(const json& config) {
  std::ostringstream sql;
  
  std::vector<std::string> tables;
  if (config.contains("tables") && config["tables"].is_array()) {
    for (const auto& table : config["tables"]) {
      tables.push_back(table.get<std::string>());
    }
  } else {
    tables.push_back("input_table");
    tables.push_back("input_table_2");
  }
  
  std::string unionType = config.value("union_type", "ALL");
  std::transform(unionType.begin(), unionType.end(), unionType.begin(), ::toupper);
  
  for (size_t i = 0; i < tables.size(); ++i) {
    if (i > 0) {
      sql << "\nUNION " << unionType << "\n";
    }
    sql << "SELECT * FROM " << escapeSQLIdentifier(tables[i]);
  }
  
  return sql.str();
}

std::string SparkTranslator::translateWindowFunction(const json& config) {
  std::ostringstream sql;
  
  sql << "SELECT ";
  
  if (config.contains("window_columns") && config["window_columns"].is_array()) {
    bool first = true;
    for (const auto& col : config["window_columns"]) {
      if (!first) sql << ", ";
      sql << escapeSQLIdentifier(col.get<std::string>());
      first = false;
    }
  } else {
    sql << "*";
  }
  
  // Window function
  if (config.contains("window_function")) {
    std::string func = config["window_function"].get<std::string>();
    std::string column = config.value("column", "");
    std::string alias = config.value("alias", "");
    
    sql << ", " << func << "(" << escapeSQLIdentifier(column) << ") OVER (";
    
    // PARTITION BY
    if (config.contains("partition_by") && config["partition_by"].is_array()) {
      sql << "PARTITION BY ";
      for (size_t i = 0; i < config["partition_by"].size(); ++i) {
        if (i > 0) sql << ", ";
        sql << escapeSQLIdentifier(config["partition_by"][i].get<std::string>());
      }
    }
    
    // ORDER BY
    if (config.contains("order_by") && config["order_by"].is_array()) {
      sql << " ORDER BY ";
      for (size_t i = 0; i < config["order_by"].size(); ++i) {
        if (i > 0) sql << ", ";
        sql << escapeSQLIdentifier(config["order_by"][i].get<std::string>());
      }
    }
    
    sql << ")";
    if (!alias.empty()) {
      sql << " AS " << escapeSQLIdentifier(alias);
    }
  }
  
  sql << "\nFROM input_table";
  
  return sql.str();
}

std::string SparkTranslator::translateDeduplication(const json& config) {
  std::ostringstream sql;
  
  std::vector<std::string> keyColumns;
  if (config.contains("key_columns") && config["key_columns"].is_array()) {
    for (const auto& col : config["key_columns"]) {
      keyColumns.push_back(col.get<std::string>());
    }
  }
  
  if (keyColumns.empty()) {
    // Deduplicar por todas las columnas
    sql << "SELECT DISTINCT *\nFROM input_table";
  } else {
    // Usar ROW_NUMBER para mantener solo la primera ocurrencia
    sql << "SELECT *\nFROM (\n";
    sql << "  SELECT *, ROW_NUMBER() OVER (PARTITION BY ";
    for (size_t i = 0; i < keyColumns.size(); ++i) {
      if (i > 0) sql << ", ";
      sql << escapeSQLIdentifier(keyColumns[i]);
    }
    sql << " ORDER BY ";
    
    std::string orderColumn = config.value("order_column", keyColumns[0]);
    sql << escapeSQLIdentifier(orderColumn);
    sql << ") AS rn\n";
    sql << "  FROM input_table\n";
    sql << ") WHERE rn = 1";
  }
  
  return sql.str();
}

std::string SparkTranslator::translateRouter(const json& config) {
  // Router divide datos en múltiples outputs basado en condiciones
  // En Spark, esto se puede hacer con CASE WHEN o múltiples queries
  // Por ahora, retornamos una query que agrega una columna de routing
  std::ostringstream sql;
  
  sql << "SELECT *, CASE\n";
  
  if (config.contains("routes") && config["routes"].is_array()) {
    for (const auto& route : config["routes"]) {
      std::string condition = route.value("condition", "1=1");
      std::string output = route.value("output", "default");
      sql << "  WHEN " << condition << " THEN '" << output << "'\n";
    }
  }
  
  sql << "  ELSE 'default'\n";
  sql << "END AS route_output\n";
  sql << "FROM input_table";
  
  return sql.str();
}

std::string SparkTranslator::translateNormalizer(const json& config) {
  // Normalizer típicamente normaliza datos anidados (JSON, arrays)
  // Por ahora, retornamos passthrough
  return "SELECT * FROM input_table";
}

std::string SparkTranslator::translateDataCleansing(const json& config) {
  std::ostringstream sql;
  
  sql << "SELECT ";
  
  // Aplicar funciones de limpieza a columnas
  if (config.contains("cleansing_rules") && config["cleansing_rules"].is_array()) {
    bool first = true;
    for (const auto& rule : config["cleansing_rules"]) {
      if (!first) sql << ", ";
      
      std::string column = rule.value("column", "");
      std::string operation = rule.value("operation", "");
      
      if (operation == "trim") {
        sql << "TRIM(" << escapeSQLIdentifier(column) << ") AS " << escapeSQLIdentifier(column);
      } else if (operation == "upper") {
        sql << "UPPER(" << escapeSQLIdentifier(column) << ") AS " << escapeSQLIdentifier(column);
      } else if (operation == "lower") {
        sql << "LOWER(" << escapeSQLIdentifier(column) << ") AS " << escapeSQLIdentifier(column);
      } else {
        sql << escapeSQLIdentifier(column);
      }
      first = false;
    }
  } else {
    sql << "*";
  }
  
  sql << "\nFROM input_table";
  
  return sql.str();
}

std::string SparkTranslator::translateDataValidation(const json& config) {
  // Data validation agrega columnas de validación
  std::ostringstream sql;
  
  sql << "SELECT *, ";
  
  if (config.contains("validation_rules") && config["validation_rules"].is_array()) {
    bool first = true;
    for (const auto& rule : config["validation_rules"]) {
      if (!first) sql << ", ";
      
      std::string column = rule.value("column", "");
      std::string validation = rule.value("validation", "");
      std::string alias = "is_valid_" + column;
      
      sql << "CASE WHEN " << validation << " THEN true ELSE false END AS " 
          << escapeSQLIdentifier(alias);
      first = false;
    }
  } else {
    sql << "true AS is_valid";
  }
  
  sql << "\nFROM input_table";
  
  return sql.str();
}

std::string SparkTranslator::escapeSQLIdentifier(const std::string& identifier) {
  // Escapar identificadores SQL (agregar backticks si contiene caracteres especiales)
  if (identifier.find(' ') != std::string::npos || 
      identifier.find('-') != std::string::npos ||
      identifier.find('.') != std::string::npos) {
    return "`" + identifier + "`";
  }
  return identifier;
}

std::string SparkTranslator::escapeSQLValue(const std::string& value) {
  // Escapar valores SQL
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return "'" + escaped + "'";
}

std::string SparkTranslator::buildColumnList(const std::vector<std::string>& columns) {
  std::ostringstream result;
  for (size_t i = 0; i < columns.size(); ++i) {
    if (i > 0) result << ", ";
    result << escapeSQLIdentifier(columns[i]);
  }
  return result.str();
}

std::string SparkTranslator::optimizeSQL(const std::string& sql) {
  // Placeholder para optimizaciones de SQL
  // En una implementación real, se podría:
  // - Eliminar subqueries innecesarias
  // - Reordenar joins
  // - Aplicar predicados pushdown
  // - Optimizar agregaciones
  return sql;
}

std::string SparkTranslator::generateDataFrameCode(const json& transformationConfig) {
  // Placeholder para generación de código DataFrame API
  // En una implementación real, se generaría código Python para PySpark
  return "# DataFrame API code generation not yet implemented";
}
