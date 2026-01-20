#ifndef UNION_TRANSFORMATION_H
#define UNION_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <vector>

// Union transformation: Combine multiple data sources
class UnionTransformation : public Transformation {
public:
  enum class UnionType {
    UNION,      // Remove duplicates
    UNION_ALL   // Keep all rows including duplicates
  };
  
  UnionTransformation();
  ~UnionTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "union"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Perform union (remove duplicates)
  std::vector<json> performUnion(
    const std::vector<json>& inputData,
    const std::vector<std::vector<json>>& additionalData
  );
  
  // Perform union all (keep duplicates)
  std::vector<json> performUnionAll(
    const std::vector<json>& inputData,
    const std::vector<std::vector<json>>& additionalData
  );
  
  // Normalize row structure (align columns)
  json normalizeRow(const json& row, const std::vector<std::string>& allColumns);
  
  // Get all unique columns from multiple data sources
  std::vector<std::string> getAllColumns(
    const std::vector<json>& inputData,
    const std::vector<std::vector<json>>& additionalData
  );
  
  // Create row signature for duplicate detection
  std::string createRowSignature(const json& row);
  
  // Parse union type from string
  UnionType parseUnionType(const std::string& unionTypeStr);
  
  // Generate SQL for union (alternative approach)
  std::string generateUnionSQL(
    const std::string& firstQuery,
    const std::vector<std::string>& additionalQueries,
    UnionType unionType
  );
};

#endif // UNION_TRANSFORMATION_H
