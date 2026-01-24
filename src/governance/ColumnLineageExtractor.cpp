#include "governance/ColumnLineageExtractor.h"
#include <algorithm>
#include <regex>
#include <sstream>

std::vector<ColumnLineageExtractor::ColumnLineage> 
ColumnLineageExtractor::extractFromQuery(
    const std::string& query,
    const std::string& targetSchema,
    const std::string& targetTable) {
  
  std::vector<ColumnLineage> lineage;

  try {
    // Parsear SELECT clause para obtener columnas destino
    std::vector<std::string> selectColumns = parseSelectClause(query);
    std::vector<std::string> fromTables = parseFromClause(query);
    std::vector<std::string> joinClauses = parseJoinClause(query);

    // Para cada columna en SELECT, encontrar sus fuentes
    for (const auto& selectCol : selectColumns) {
      ColumnLineage colLineage;
      
      // Parsear columna destino
      auto [targetCol, alias] = parseTableColumn(selectCol);
      colLineage.targetSchema = targetSchema;
      colLineage.targetTable = targetTable;
      colLineage.targetColumn = targetCol;

      // Extraer columnas fuente desde la expresión
      std::vector<std::pair<std::string, std::string>> sourceCols = 
          extractSourceColumns(selectCol);

      // Si no hay columnas fuente explícitas, usar la primera tabla FROM
      if (sourceCols.empty() && !fromTables.empty()) {
        auto [schema, table] = parseTableColumn(fromTables[0]);
        colLineage.sourceSchema = schema;
        colLineage.sourceTable = table;
        colLineage.sourceColumn = targetCol;  // Mismo nombre por defecto
      } else {
        // Usar primera columna fuente encontrada
        if (!sourceCols.empty()) {
          auto [schema, table] = parseTableColumn(sourceCols[0].first);
          colLineage.sourceSchema = schema;
          colLineage.sourceTable = table;
          colLineage.sourceColumn = sourceCols[0].second;
        }
      }

      colLineage.transformationType = detectTransformationType(selectCol);
      colLineage.transformationExpression = selectCol;

      lineage.push_back(colLineage);
    }

  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "ColumnLineageExtractor",
                  "Error extracting column lineage: " + std::string(e.what()));
  }

  return lineage;
}

std::vector<ColumnLineageExtractor::ColumnLineage> 
ColumnLineageExtractor::extractFromPipeline(
    const std::vector<std::string>& queries) {
  
  std::vector<ColumnLineage> allLineage;

  std::string prevSchema = "";
  std::string prevTable = "";

  for (size_t i = 0; i < queries.size(); ++i) {
    std::vector<ColumnLineage> queryLineage = extractFromQuery(
        queries[i], prevSchema, prevTable);

    // Actualizar schema/table para siguiente query
    if (!queryLineage.empty()) {
      prevSchema = queryLineage[0].targetSchema;
      prevTable = queryLineage[0].targetTable;
    }

    allLineage.insert(allLineage.end(), 
                     queryLineage.begin(), queryLineage.end());
  }

  return allLineage;
}

std::vector<std::pair<std::string, std::string>> 
ColumnLineageExtractor::extractSourceColumns(
    const std::string& expression,
    const std::string& defaultSchema) {
  
  std::vector<std::pair<std::string, std::string>> columns;

  // Patrón para encontrar referencias de tabla.columna
  std::regex colPattern(R"((\w+\.)?(\w+)\.(\w+))");
  std::sregex_iterator iter(expression.begin(), expression.end(), colPattern);
  std::sregex_iterator end;

  for (; iter != end; ++iter) {
    std::string fullRef = iter->str(0);
    std::string schema = iter->str(1).empty() ? defaultSchema : iter->str(1);
    std::string table = iter->str(2);
    std::string column = iter->str(3);

    columns.push_back({schema + "." + table, column});
  }

  return columns;
}

std::string ColumnLineageExtractor::detectTransformationType(
    const std::string& expression) {
  
  std::string lowerExpr = expression;
  std::transform(lowerExpr.begin(), lowerExpr.end(), lowerExpr.begin(), ::tolower);

  // Detectar agregaciones
  if (lowerExpr.find("count(") != std::string::npos ||
      lowerExpr.find("sum(") != std::string::npos ||
      lowerExpr.find("avg(") != std::string::npos ||
      lowerExpr.find("max(") != std::string::npos ||
      lowerExpr.find("min(") != std::string::npos) {
    return "aggregation";
  }

  // Detectar expresiones complejas
  if (lowerExpr.find("case") != std::string::npos ||
      lowerExpr.find("when") != std::string::npos ||
      lowerExpr.find("+") != std::string::npos ||
      lowerExpr.find("-") != std::string::npos ||
      lowerExpr.find("*") != std::string::npos ||
      lowerExpr.find("/") != std::string::npos) {
    return "expression";
  }

  // Detectar casts
  if (lowerExpr.find("cast(") != std::string::npos ||
      lowerExpr.find("::") != std::string::npos) {
    return "cast";
  }

  // Por defecto, mapeo directo
  return "direct";
}

std::vector<std::string> ColumnLineageExtractor::parseSelectClause(
    const std::string& query) {
  
  std::vector<std::string> columns;

  std::regex selectRegex(R"(\bselect\s+(.+?)\s+from)", std::regex_constants::icase);
  std::smatch match;

  if (std::regex_search(query, match, selectRegex)) {
    std::string selectClause = match.str(1);

    // Si es *, retornar vacío (todas las columnas)
    if (selectClause.find("*") != std::string::npos) {
      return columns;
    }

    // Parsear columnas separadas por coma
    std::regex colRegex(R"(([^,]+))");
    std::sregex_iterator iter(selectClause.begin(), selectClause.end(), colRegex);
    std::sregex_iterator end;

    for (; iter != end; ++iter) {
      std::string col = iter->str();
      // Trim y remover AS alias
      std::regex aliasRegex(R"(\s+as\s+(\w+))", std::regex_constants::icase);
      col = std::regex_replace(col, aliasRegex, "");
      
      // Trim whitespace
      col.erase(0, col.find_first_not_of(" \t"));
      col.erase(col.find_last_not_of(" \t") + 1);
      
      if (!col.empty()) {
        columns.push_back(col);
      }
    }
  }

  return columns;
}

std::vector<std::string> ColumnLineageExtractor::parseFromClause(
    const std::string& query) {
  
  std::vector<std::string> tables;

  std::regex fromRegex(R"(\bfrom\s+(\w+(?:\.\w+)?))", std::regex_constants::icase);
  std::sregex_iterator iter(query.begin(), query.end(), fromRegex);
  std::sregex_iterator end;

  for (; iter != end; ++iter) {
    tables.push_back(iter->str(1));
  }

  return tables;
}

std::vector<std::string> ColumnLineageExtractor::parseJoinClause(
    const std::string& query) {
  
  std::vector<std::string> joins;

  std::regex joinRegex(R"(\bjoin\s+(\w+(?:\.\w+)?)\s+on\s+([^join]+?)(?:\s+join|\s+where|\s+group|\s+order|\s+limit|$))",
                       std::regex_constants::icase);
  std::sregex_iterator iter(query.begin(), query.end(), joinRegex);
  std::sregex_iterator end;

  for (; iter != end; ++iter) {
    joins.push_back(iter->str(0));
  }

  return joins;
}

std::string ColumnLineageExtractor::normalizeColumnReference(
    const std::string& columnRef) {
  
  std::string normalized = columnRef;
  std::transform(normalized.begin(), normalized.end(), normalized.begin(), ::tolower);
  
  // Remover espacios y normalizar
  normalized.erase(std::remove(normalized.begin(), normalized.end(), ' '), normalized.end());
  
  return normalized;
}

std::pair<std::string, std::string> ColumnLineageExtractor::parseTableColumn(
    const std::string& reference) {
  
  // Formato esperado: schema.table o table o schema.table.column
  std::regex pattern(R"((\w+)?\.?(\w+)?\.?(\w+)?)");
  std::smatch match;

  if (std::regex_match(reference, match, pattern)) {
    if (match.size() == 4) {
      std::string part1 = match.str(1);
      std::string part2 = match.str(2);
      std::string part3 = match.str(3);

      if (!part3.empty()) {
        // schema.table.column
        return {part1 + "." + part2, part3};
      } else if (!part2.empty()) {
        // schema.table o table.column
        if (part1.find(".") != std::string::npos) {
          return {part1, part2};
        } else {
          return {"", part1 + "." + part2};
        }
      } else {
        // solo table o column
        return {"", part1};
      }
    }
  }

  return {"", reference};
}
