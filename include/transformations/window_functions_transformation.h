#ifndef WINDOW_FUNCTIONS_TRANSFORMATION_H
#define WINDOW_FUNCTIONS_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <vector>
#include <map>

// Window Functions transformation: ROW_NUMBER, LAG, LEAD, etc.
class WindowFunctionsTransformation : public Transformation {
public:
  enum class WindowFunction {
    ROW_NUMBER,
    LAG,
    LEAD,
    FIRST_VALUE,
    LAST_VALUE,
    RANK,
    DENSE_RANK
  };
  
  struct WindowConfig {
    WindowFunction function;
    std::string targetColumn;
    std::string sourceColumn;
    std::vector<std::string> partitionBy;
    std::vector<std::string> orderBy;
    int offset; // For LAG/LEAD
    json defaultValue; // For LAG/LEAD
  };
  
  WindowFunctionsTransformation();
  ~WindowFunctionsTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "window_functions"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Apply window function to partitioned data
  void applyWindowFunction(
    std::vector<json>& partitionData,
    const WindowConfig& windowConfig
  );
  
  // Get partition key from row
  std::string getPartitionKey(const json& row, const std::vector<std::string>& partitionBy);
  
  // Compare rows for ordering
  bool compareRows(const json& row1, const json& row2, const std::vector<std::string>& orderBy);
  
  // Parse window function from string
  WindowFunction parseWindowFunction(const std::string& funcStr);
};

#endif // WINDOW_FUNCTIONS_TRANSFORMATION_H
