#ifndef RANK_TRANSFORMATION_H
#define RANK_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <vector>

// Rank transformation: TOP N, BOTTOM N with optional partitioning
class RankTransformation : public Transformation {
public:
  enum class RankType {
    TOP_N,
    BOTTOM_N,
    RANK,
    DENSE_RANK,
    ROW_NUMBER
  };
  
  RankTransformation();
  ~RankTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "rank"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Perform ranking operation
  std::vector<json> performRanking(
    const std::vector<json>& inputData,
    RankType rankType,
    int n,
    const std::string& orderColumn,
    const std::vector<std::string>& partitionColumns
  );
  
  // Compare rows for ranking
  bool compareForRanking(const json& row1, const json& row2, const std::string& orderColumn);
  
  // Parse rank type from string
  RankType parseRankType(const std::string& typeStr);
};

#endif // RANK_TRANSFORMATION_H
