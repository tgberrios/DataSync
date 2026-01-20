#ifndef JOIN_TRANSFORMATION_H
#define JOIN_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <vector>
#include <map>

// Join transformation: Join two data streams (INNER, LEFT, RIGHT, FULL OUTER)
class JoinTransformation : public Transformation {
public:
  enum class JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL_OUTER
  };
  
  JoinTransformation();
  ~JoinTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "join"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Perform inner join
  std::vector<json> performInnerJoin(
    const std::vector<json>& leftData,
    const std::vector<json>& rightData,
    const std::vector<std::string>& leftColumns,
    const std::vector<std::string>& rightColumns
  );
  
  // Perform left join
  std::vector<json> performLeftJoin(
    const std::vector<json>& leftData,
    const std::vector<json>& rightData,
    const std::vector<std::string>& leftColumns,
    const std::vector<std::string>& rightColumns
  );
  
  // Perform right join
  std::vector<json> performRightJoin(
    const std::vector<json>& leftData,
    const std::vector<json>& rightData,
    const std::vector<std::string>& leftColumns,
    const std::vector<std::string>& rightColumns
  );
  
  // Perform full outer join
  std::vector<json> performFullOuterJoin(
    const std::vector<json>& leftData,
    const std::vector<json>& rightData,
    const std::vector<std::string>& leftColumns,
    const std::vector<std::string>& rightColumns
  );
  
  // Merge two JSON objects with column prefixing
  json mergeRows(const json& leftRow, const json& rightRow, 
                 const std::string& leftPrefix = "", 
                 const std::string& rightPrefix = "");
  
  // Create join key from row
  std::string createJoinKey(const json& row, const std::vector<std::string>& columns);
  
  // Parse join type from string
  JoinType parseJoinType(const std::string& joinTypeStr);
  
  // Generate SQL for join (alternative approach)
  std::string generateJoinSQL(
    const std::string& leftQuery,
    const std::string& rightQuery,
    JoinType joinType,
    const std::vector<std::string>& leftColumns,
    const std::vector<std::string>& rightColumns
  );
};

#endif // JOIN_TRANSFORMATION_H
