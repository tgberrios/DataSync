#include "DataQualityCalculator.h"
#include "logger.h"

void DataQualityCalculator::calculateQualityScores(TableMetadata &metadata) {
  metadata.completeness_score = calculateCompletenessScore(metadata);
  metadata.accuracy_score = calculateAccuracyScore(metadata);
  metadata.consistency_score = calculateConsistencyScore(metadata);
  metadata.validity_score = calculateValidityScore(metadata);
  metadata.timeliness_score = calculateTimelinessScore(metadata);
  metadata.uniqueness_score = calculateUniquenessScore(metadata);
  metadata.integrity_score = calculateIntegrityScore(metadata);
  metadata.data_quality_score = calculateOverallQualityScore(metadata);
}

double DataQualityCalculator::calculateCompletenessScore(const TableMetadata &metadata) {
  if (metadata.total_columns == 0) {
    return 0.0;
  }
  return normalizeScore(100.0 - (metadata.null_percentage * 0.1));
}

double DataQualityCalculator::calculateAccuracyScore(const TableMetadata &metadata) {
  return normalizeScore(100.0 - (metadata.duplicate_percentage * 0.5));
}

double DataQualityCalculator::calculateConsistencyScore(const TableMetadata &metadata) {
  return normalizeScore(100.0 - (metadata.fragmentation_percentage * 0.2));
}

double DataQualityCalculator::calculateValidityScore(const TableMetadata &metadata) {
  return normalizeScore(100.0 - (metadata.null_percentage * 0.3));
}

double DataQualityCalculator::calculateTimelinessScore(const TableMetadata &metadata) {
  return normalizeScore(100.0 - (metadata.fragmentation_percentage * 0.1));
}

double DataQualityCalculator::calculateUniquenessScore(const TableMetadata &metadata) {
  return normalizeScore(100.0 - (metadata.duplicate_percentage * 0.8));
}

double DataQualityCalculator::calculateIntegrityScore(const TableMetadata &metadata) {
  return normalizeScore(100.0 - (metadata.fragmentation_percentage * 0.3));
}

double DataQualityCalculator::calculateOverallQualityScore(const TableMetadata &metadata) {
  double score = 100.0;
  
  // Weighted calculation based on different quality metrics
  score -= metadata.null_percentage * 0.5;
  score -= metadata.duplicate_percentage * 0.3;
  score -= metadata.fragmentation_percentage * 0.2;
  
  return normalizeScore(score);
}

double DataQualityCalculator::normalizeScore(double score, double maxValue) {
  return std::max(0.0, std::min(maxValue, score));
}

double DataQualityCalculator::calculatePercentageScore(double value, double total, double weight) {
  if (total == 0) {
    return 100.0;
  }
  double percentage = (value / total) * 100.0;
  return normalizeScore(100.0 - (percentage * weight));
}
