#ifndef SEQUENCE_GENERATOR_TRANSFORMATION_H
#define SEQUENCE_GENERATOR_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>

// Sequence Generator transformation: Generate sequential IDs
class SequenceGeneratorTransformation : public Transformation {
public:
  SequenceGeneratorTransformation();
  ~SequenceGeneratorTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "sequence_generator"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Generate sequence for a row
  int64_t generateSequence(int rowIndex, int64_t startValue, int64_t increment);
};

#endif // SEQUENCE_GENERATOR_TRANSFORMATION_H
