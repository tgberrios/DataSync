#ifndef ROUTER_TRANSFORMATION_H
#define ROUTER_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <vector>
#include <map>

// Router transformation: Split data by conditions into multiple outputs
class RouterTransformation : public Transformation {
public:
  RouterTransformation();
  ~RouterTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "router"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Evaluate a condition on a row
  bool evaluateCondition(const json& row, const json& condition);
  
  // Evaluate comparison operator
  bool evaluateOperator(const json& leftValue, const std::string& op, const json& rightValue);
  
  // Get value from row by column path
  json getValueByPath(const json& row, const std::string& columnPath);
  
  // Generate SQL for router (alternative approach)
  std::string generateRouterSQL(
    const std::string& sourceQuery,
    const std::vector<json>& routes
  );
};

#endif // ROUTER_TRANSFORMATION_H
