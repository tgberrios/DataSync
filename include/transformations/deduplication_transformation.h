#ifndef DEDUPLICATION_TRANSFORMATION_H
#define DEDUPLICATION_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <vector>
#include <map>
#include <set>

// Deduplication transformation: Fuzzy matching and duplicate detection
class DeduplicationTransformation : public Transformation {
public:
  DeduplicationTransformation();
  ~DeduplicationTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "deduplication"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Calculate Levenshtein distance between two strings
  int levenshteinDistance(const std::string& s1, const std::string& s2);
  
  // Calculate similarity score (0.0 to 1.0)
  double calculateSimilarity(const std::string& s1, const std::string& s2);
  
  // Check if two rows are duplicates based on fuzzy matching
  bool areDuplicates(
    const json& row1,
    const json& row2,
    const std::vector<std::string>& keyColumns,
    double similarityThreshold
  );
  
  // Generate key for exact matching
  std::string generateKey(const json& row, const std::vector<std::string>& columns);
  
  // Normalize string for comparison
  std::string normalizeString(const std::string& str);
};

#endif // DEDUPLICATION_TRANSFORMATION_H
