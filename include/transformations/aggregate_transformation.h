#ifndef AGGREGATE_TRANSFORMATION_H
#define AGGREGATE_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <vector>
#include <map>

// Aggregate transformation: GROUP BY with aggregation functions
class AggregateTransformation : public Transformation {
public:
  AggregateTransformation();
  ~AggregateTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "aggregate"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Perform aggregation on grouped data
  json aggregateGroup(
    const std::vector<json>& groupData,
    const std::vector<std::string>& groupByColumns,
    const json& aggregationConfig
  );
  
  // Calculate aggregation function value
  double calculateAggregation(
    const std::vector<json>& groupData,
    const std::string& column,
    const std::string& function,
    const json& aggConfig = json::object()
  );
  
  // Helper to get numeric value from JSON
  double getNumericValue(const json& value);
  
  // Generate SQL for aggregation (alternative approach)
  std::string generateAggregateSQL(
    const std::string& sourceQuery,
    const std::vector<std::string>& groupByColumns,
    const json& aggregationConfig
  );
};

#endif // AGGREGATE_TRANSFORMATION_H
