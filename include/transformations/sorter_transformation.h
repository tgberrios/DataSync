#ifndef SORTER_TRANSFORMATION_H
#define SORTER_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <vector>

// Sorter transformation: Sort data by columns
class SorterTransformation : public Transformation {
public:
  enum class SortOrder {
    ASC,
    DESC
  };
  
  struct SortColumn {
    std::string column;
    SortOrder order;
  };
  
  SorterTransformation();
  ~SorterTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "sorter"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Compare two JSON rows based on sort columns
  bool compareRows(
    const json& row1,
    const json& row2,
    const std::vector<SortColumn>& sortColumns
  );
  
  // Compare two values
  int compareValues(const json& val1, const json& val2);
  
  // Parse sort order from string
  SortOrder parseSortOrder(const std::string& orderStr);
  
  // Generate SQL for sorting (alternative approach)
  std::string generateSortSQL(
    const std::string& sourceQuery,
    const std::vector<SortColumn>& sortColumns
  );
};

#endif // SORTER_TRANSFORMATION_H
