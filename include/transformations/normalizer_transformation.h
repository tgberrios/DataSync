#ifndef NORMALIZER_TRANSFORMATION_H
#define NORMALIZER_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <vector>

// Normalizer transformation: Denormalize columns (unpivot/flatten)
class NormalizerTransformation : public Transformation {
public:
  NormalizerTransformation();
  ~NormalizerTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "normalizer"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Denormalize a single row
  std::vector<json> denormalizeRow(
    const json& row,
    const std::vector<std::string>& columnsToDenormalize,
    const std::string& keyColumnName,
    const std::string& valueColumnName
  );
};

#endif // NORMALIZER_TRANSFORMATION_H
