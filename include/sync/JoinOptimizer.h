#ifndef JOIN_OPTIMIZER_H
#define JOIN_OPTIMIZER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <memory>
#include <optional>

using json = nlohmann::json;

// JoinOptimizer: Optimiza joins para queries no-distribuidas
class JoinOptimizer {
public:
  enum class JoinAlgorithm {
    AUTO,           // Detección automática
    HASH_JOIN,      // Hash join
    SORT_MERGE_JOIN,// Sort-merge join
    NESTED_LOOP     // Nested loop join (fallback)
  };

  struct TableStats {
    std::string tableName;
    size_t estimatedRows{0};
    size_t estimatedSizeBytes{0};
    bool isSorted{false};
    std::string sortColumn;
    std::vector<std::string> indexedColumns;
  };

  struct JoinConfig {
    std::string leftTable;
    std::string rightTable;
    std::vector<std::string> leftColumns;
    std::vector<std::string> rightColumns;
    std::string joinType;  // "inner", "left", "right", "full_outer"
    std::string joinCondition;
    TableStats leftStats;
    TableStats rightStats;
    JoinAlgorithm preferredAlgorithm{JoinAlgorithm::AUTO};
  };

  struct JoinResult {
    bool success{false};
    std::vector<json> resultRows;
    JoinAlgorithm algorithmUsed;
    size_t rowsProcessed{0};
    double executionTimeMs{0.0};
    std::string errorMessage;
  };

  // Detectar mejor algoritmo de join
  static JoinAlgorithm detectBestAlgorithm(const JoinConfig& config);

  // Ejecutar hash join
  static JoinResult executeHashJoin(
      const JoinConfig& config,
      const std::vector<json>& leftData,
      const std::vector<json>& rightData
  );

  // Ejecutar sort-merge join
  static JoinResult executeSortMergeJoin(
      const JoinConfig& config,
      const std::vector<json>& leftData,
      const std::vector<json>& rightData
  );

  // Ejecutar nested loop join
  static JoinResult executeNestedLoopJoin(
      const JoinConfig& config,
      const std::vector<json>& leftData,
      const std::vector<json>& rightData
  );

  // Ejecutar join con algoritmo automático
  static JoinResult executeJoin(
      const JoinConfig& config,
      const std::vector<json>& leftData,
      const std::vector<json>& rightData
  );

  // Obtener estadísticas de tabla (estimación)
  static TableStats estimateTableStats(
      const std::string& tableName,
      const std::vector<json>& sampleData
  );

private:
  // Helper methods
  static std::string generateJoinKey(const json& row, const std::vector<std::string>& columns);
  static bool matchesJoinCondition(const json& leftRow, const json& rightRow,
                                    const JoinConfig& config);
  static json mergeRows(const json& leftRow, const json& rightRow, const JoinConfig& config);
  static int compareJoinKeys(const std::string& key1, const std::string& key2);
};

#endif // JOIN_OPTIMIZER_H
