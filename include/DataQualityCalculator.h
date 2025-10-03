#ifndef DATAQUALITYCALCULATOR_H
#define DATAQUALITYCALCULATOR_H

#include "TableMetadata.h"

class DataQualityCalculator {
public:
  DataQualityCalculator() = default;
  ~DataQualityCalculator() = default;

  // Main quality calculation method
  void calculateQualityScores(TableMetadata &metadata);

  // Individual quality score calculations
  double calculateCompletenessScore(const TableMetadata &metadata);
  double calculateAccuracyScore(const TableMetadata &metadata);
  double calculateConsistencyScore(const TableMetadata &metadata);
  double calculateValidityScore(const TableMetadata &metadata);
  double calculateTimelinessScore(const TableMetadata &metadata);
  double calculateUniquenessScore(const TableMetadata &metadata);
  double calculateIntegrityScore(const TableMetadata &metadata);
  double calculateOverallQualityScore(const TableMetadata &metadata);

private:
  // Helper methods
  double normalizeScore(double score, double maxValue = 100.0);
  double calculatePercentageScore(double value, double total, double weight = 1.0);
};

#endif // DATAQUALITYCALCULATOR_H
