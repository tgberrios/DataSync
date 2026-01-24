#include "sync/JoinOptimizer.h"
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <sstream>

JoinOptimizer::JoinAlgorithm JoinOptimizer::detectBestAlgorithm(const JoinConfig& config) {
  // Si hay un algoritmo preferido y es válido, usarlo
  if (config.preferredAlgorithm != JoinAlgorithm::AUTO) {
    return config.preferredAlgorithm;
  }

  size_t leftSize = config.leftStats.estimatedRows;
  size_t rightSize = config.rightStats.estimatedRows;
  size_t leftBytes = config.leftStats.estimatedSizeBytes;
  size_t rightBytes = config.rightStats.estimatedSizeBytes;

  // Si una tabla es mucho más pequeña, usar hash join
  if (rightSize < 10000 || (rightBytes < 1024 * 1024 && rightSize < leftSize / 10)) {
    return JoinAlgorithm::HASH_JOIN;
  }

  // Si ambas tablas están ordenadas, usar sort-merge
  if (config.leftStats.isSorted && config.rightStats.isSorted) {
    // Verificar que las columnas de join coincidan con las columnas ordenadas
    bool leftSortedOnJoin = false;
    if (!config.leftColumns.empty() && !config.leftStats.sortColumn.empty()) {
      leftSortedOnJoin = (config.leftStats.sortColumn == config.leftColumns[0]);
    }
    bool rightSortedOnJoin = false;
    if (!config.rightColumns.empty() && !config.rightStats.sortColumn.empty()) {
      rightSortedOnJoin = (config.rightStats.sortColumn == config.rightColumns[0]);
    }
    
    if (leftSortedOnJoin && rightSortedOnJoin) {
      return JoinAlgorithm::SORT_MERGE_JOIN;
    }
  }

  // Si las tablas son muy grandes, preferir sort-merge
  if (leftSize > 1000000 || rightSize > 1000000) {
    return JoinAlgorithm::SORT_MERGE_JOIN;
  }

  // Por defecto, hash join para tablas medianas
  if (leftSize < 100000 && rightSize < 100000) {
    return JoinAlgorithm::HASH_JOIN;
  }

  // Fallback a nested loop solo para tablas muy pequeñas
  if (leftSize < 1000 && rightSize < 1000) {
    return JoinAlgorithm::NESTED_LOOP;
  }

  // Default: sort-merge para casos grandes
  return JoinAlgorithm::SORT_MERGE_JOIN;
}

JoinOptimizer::JoinResult JoinOptimizer::executeHashJoin(
    const JoinConfig& config,
    const std::vector<json>& leftData,
    const std::vector<json>& rightData) {
  
  JoinResult result;
  result.algorithmUsed = JoinAlgorithm::HASH_JOIN;
  auto startTime = std::chrono::high_resolution_clock::now();

  try {
    // Construir hash table de la tabla derecha (más pequeña)
    std::unordered_map<std::string, std::vector<json>> hashTable;
    
    for (const auto& rightRow : rightData) {
      std::string key = generateJoinKey(rightRow, config.rightColumns);
      hashTable[key].push_back(rightRow);
    }

    // Procesar tabla izquierda y hacer lookup en hash table
    for (const auto& leftRow : leftData) {
      std::string key = generateJoinKey(leftRow, config.leftColumns);
      
      auto it = hashTable.find(key);
      if (it != hashTable.end()) {
        for (const auto& rightRow : it->second) {
          if (matchesJoinCondition(leftRow, rightRow, config)) {
            result.resultRows.push_back(mergeRows(leftRow, rightRow, config));
          }
        }
      } else if (config.joinType == "left" || config.joinType == "full_outer") {
        // Left join: incluir filas sin match
        result.resultRows.push_back(mergeRows(leftRow, json(nullptr), config));
      }
    }

    // Right/Full outer: agregar filas de right sin match
    if (config.joinType == "right" || config.joinType == "full_outer") {
      std::unordered_set<std::string> matchedRightKeys;
      for (const auto& leftRow : leftData) {
        std::string key = generateJoinKey(leftRow, config.leftColumns);
        auto it = hashTable.find(key);
        if (it != hashTable.end()) {
          for (const auto& rightRow : it->second) {
            if (matchesJoinCondition(leftRow, rightRow, config)) {
              matchedRightKeys.insert(generateJoinKey(rightRow, config.rightColumns));
            }
          }
        }
      }

      for (const auto& rightRow : rightData) {
        std::string key = generateJoinKey(rightRow, config.rightColumns);
        if (matchedRightKeys.find(key) == matchedRightKeys.end()) {
          result.resultRows.push_back(mergeRows(json(nullptr), rightRow, config));
        }
      }
    }

    result.success = true;
    result.rowsProcessed = leftData.size() + rightData.size();
  } catch (const std::exception& e) {
    result.success = false;
    result.errorMessage = e.what();
    Logger::error(LogCategory::SYSTEM, "JoinOptimizer",
                  "Hash join error: " + result.errorMessage);
  }

  auto endTime = std::chrono::high_resolution_clock::now();
  result.executionTimeMs = std::chrono::duration<double, std::milli>(endTime - startTime).count();

  return result;
}

JoinOptimizer::JoinResult JoinOptimizer::executeSortMergeJoin(
    const JoinConfig& config,
    const std::vector<json>& leftData,
    const std::vector<json>& rightData) {
  
  JoinResult result;
  result.algorithmUsed = JoinAlgorithm::SORT_MERGE_JOIN;
  auto startTime = std::chrono::high_resolution_clock::now();

  try {
    // Crear copias y ordenar
    std::vector<json> sortedLeft = leftData;
    std::vector<json> sortedRight = rightData;

    // Ordenar por join key
    std::sort(sortedLeft.begin(), sortedLeft.end(),
              [&config](const json& a, const json& b) {
                return compareJoinKeys(generateJoinKey(a, config.leftColumns),
                                      generateJoinKey(b, config.leftColumns)) < 0;
              });

    std::sort(sortedRight.begin(), sortedRight.end(),
              [&config](const json& a, const json& b) {
                return compareJoinKeys(generateJoinKey(a, config.rightColumns),
                                      generateJoinKey(b, config.rightColumns)) < 0;
              });

    // Merge
    size_t leftIdx = 0;
    size_t rightIdx = 0;

    while (leftIdx < sortedLeft.size() && rightIdx < sortedRight.size()) {
      std::string leftKey = generateJoinKey(sortedLeft[leftIdx], config.leftColumns);
      std::string rightKey = generateJoinKey(sortedRight[rightIdx], config.rightColumns);

      int cmp = compareJoinKeys(leftKey, rightKey);

      if (cmp < 0) {
        // Left key menor, avanzar left
        if (config.joinType == "left" || config.joinType == "full_outer") {
          result.resultRows.push_back(mergeRows(sortedLeft[leftIdx], json(nullptr), config));
        }
        leftIdx++;
      } else if (cmp > 0) {
        // Right key menor, avanzar right
        if (config.joinType == "right" || config.joinType == "full_outer") {
          result.resultRows.push_back(mergeRows(json(nullptr), sortedRight[rightIdx], config));
        }
        rightIdx++;
      } else {
        // Keys iguales, hacer join
        size_t rightStart = rightIdx;
        
        // Encontrar todos los matches en right
        while (rightIdx < sortedRight.size() &&
               generateJoinKey(sortedRight[rightIdx], config.rightColumns) == rightKey) {
          rightIdx++;
        }

        // Hacer join con todos los matches
        size_t leftStart = leftIdx;
        while (leftIdx < sortedLeft.size() &&
               generateJoinKey(sortedLeft[leftIdx], config.leftColumns) == leftKey) {
          for (size_t r = rightStart; r < rightIdx; ++r) {
            if (matchesJoinCondition(sortedLeft[leftIdx], sortedRight[r], config)) {
              result.resultRows.push_back(mergeRows(sortedLeft[leftIdx], sortedRight[r], config));
            }
          }
          leftIdx++;
        }
      }
    }

    // Procesar restantes para left/right outer
    while (leftIdx < sortedLeft.size() &&
           (config.joinType == "left" || config.joinType == "full_outer")) {
      result.resultRows.push_back(mergeRows(sortedLeft[leftIdx], json(nullptr), config));
      leftIdx++;
    }

    while (rightIdx < sortedRight.size() &&
           (config.joinType == "right" || config.joinType == "full_outer")) {
      result.resultRows.push_back(mergeRows(json(nullptr), sortedRight[rightIdx], config));
      rightIdx++;
    }

    result.success = true;
    result.rowsProcessed = leftData.size() + rightData.size();
  } catch (const std::exception& e) {
    result.success = false;
    result.errorMessage = e.what();
    Logger::error(LogCategory::SYSTEM, "JoinOptimizer",
                  "Sort-merge join error: " + result.errorMessage);
  }

  auto endTime = std::chrono::high_resolution_clock::now();
  result.executionTimeMs = std::chrono::duration<double, std::milli>(endTime - startTime).count();

  return result;
}

JoinOptimizer::JoinResult JoinOptimizer::executeNestedLoopJoin(
    const JoinConfig& config,
    const std::vector<json>& leftData,
    const std::vector<json>& rightData) {
  
  JoinResult result;
  result.algorithmUsed = JoinAlgorithm::NESTED_LOOP;
  auto startTime = std::chrono::high_resolution_clock::now();

  try {
    for (const auto& leftRow : leftData) {
      bool hasMatch = false;

      for (const auto& rightRow : rightData) {
        if (matchesJoinCondition(leftRow, rightRow, config)) {
          result.resultRows.push_back(mergeRows(leftRow, rightRow, config));
          hasMatch = true;
        }
      }

      // Left outer: incluir sin match
      if (!hasMatch && (config.joinType == "left" || config.joinType == "full_outer")) {
        result.resultRows.push_back(mergeRows(leftRow, json(nullptr), config));
      }
    }

    // Right/Full outer: agregar right sin match
    if (config.joinType == "right" || config.joinType == "full_outer") {
      for (const auto& rightRow : rightData) {
        bool hasMatch = false;
        for (const auto& leftRow : leftData) {
          if (matchesJoinCondition(leftRow, rightRow, config)) {
            hasMatch = true;
            break;
          }
        }
        if (!hasMatch) {
          result.resultRows.push_back(mergeRows(json(nullptr), rightRow, config));
        }
      }
    }

    result.success = true;
    result.rowsProcessed = leftData.size() * rightData.size();
  } catch (const std::exception& e) {
    result.success = false;
    result.errorMessage = e.what();
    Logger::error(LogCategory::SYSTEM, "JoinOptimizer",
                  "Nested loop join error: " + result.errorMessage);
  }

  auto endTime = std::chrono::high_resolution_clock::now();
  result.executionTimeMs = std::chrono::duration<double, std::milli>(endTime - startTime).count();

  return result;
}

JoinOptimizer::JoinResult JoinOptimizer::executeJoin(
    const JoinConfig& config,
    const std::vector<json>& leftData,
    const std::vector<json>& rightData) {
  
  JoinAlgorithm algorithm = detectBestAlgorithm(config);

  switch (algorithm) {
    case JoinAlgorithm::HASH_JOIN:
      return executeHashJoin(config, leftData, rightData);
    
    case JoinAlgorithm::SORT_MERGE_JOIN:
      return executeSortMergeJoin(config, leftData, rightData);
    
    case JoinAlgorithm::NESTED_LOOP:
      return executeNestedLoopJoin(config, leftData, rightData);
    
    default:
      return executeHashJoin(config, leftData, rightData);
  }
}

JoinOptimizer::TableStats JoinOptimizer::estimateTableStats(
    const std::string& tableName,
    const std::vector<json>& sampleData) {
  
  TableStats stats;
  stats.tableName = tableName;
  stats.estimatedRows = sampleData.size();

  // Estimar tamaño (simplificado)
  if (!sampleData.empty()) {
    size_t sampleSize = sampleData[0].dump().size();
    stats.estimatedSizeBytes = sampleSize * sampleData.size();
  }

  return stats;
}

std::string JoinOptimizer::generateJoinKey(const json& row, const std::vector<std::string>& columns) {
  if (row.is_null()) {
    return "";
  }

  std::stringstream key;
  for (size_t i = 0; i < columns.size(); ++i) {
    if (i > 0) key << "|";
    if (row.contains(columns[i])) {
      key << row[columns[i]].dump();
    }
  }
  return key.str();
}

bool JoinOptimizer::matchesJoinCondition(const json& leftRow, const json& rightRow,
                                          const JoinConfig& config) {
  if (leftRow.is_null() || rightRow.is_null()) {
    return false;
  }

  // Verificar que las columnas de join coincidan
  if (config.leftColumns.size() != config.rightColumns.size()) {
    return false;
  }

  for (size_t i = 0; i < config.leftColumns.size(); ++i) {
    if (!leftRow.contains(config.leftColumns[i]) ||
        !rightRow.contains(config.rightColumns[i])) {
      return false;
    }

    json leftVal = leftRow[config.leftColumns[i]];
    json rightVal = rightRow[config.rightColumns[i]];

    if (leftVal != rightVal) {
      return false;
    }
  }

  return true;
}

json JoinOptimizer::mergeRows(const json& leftRow, const json& rightRow, const JoinConfig& config) {
  json merged;

  if (!leftRow.is_null()) {
    for (auto& [key, value] : leftRow.items()) {
      merged["left_" + key] = value;
    }
  }

  if (!rightRow.is_null()) {
    for (auto& [key, value] : rightRow.items()) {
      merged["right_" + key] = value;
    }
  }

  return merged;
}

int JoinOptimizer::compareJoinKeys(const std::string& key1, const std::string& key2) {
  if (key1 < key2) return -1;
  if (key1 > key2) return 1;
  return 0;
}
