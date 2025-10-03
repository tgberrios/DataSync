#include "DataClassifier.h"
#include "logger.h"
#include <algorithm>
#include <vector>

void DataClassifier::classifyTable(TableMetadata &metadata) {
  metadata.data_category = determineDataCategory(metadata.table_name, metadata.schema_name);
  metadata.business_domain = determineBusinessDomain(metadata.table_name, metadata.schema_name);
  metadata.sensitivity_level = determineSensitivityLevel(metadata.table_name, metadata.schema_name);
  metadata.data_classification = determineDataClassification(metadata.table_name, metadata.schema_name);
  metadata.retention_policy = determineRetentionPolicy(metadata.data_category, metadata.sensitivity_level);
  metadata.backup_frequency = determineBackupFrequency(metadata.data_category, metadata.access_frequency);
  metadata.compliance_requirements = determineComplianceRequirements(metadata.sensitivity_level, metadata.business_domain);
}

std::string DataClassifier::determineDataCategory(const std::string &table_name, const std::string &schema_name) {
  std::string name = toLowerCase(table_name);
  std::string schema = toLowerCase(schema_name);

  // Schema-based classification
  if (containsAnyPattern(schema, {"analytics", "reports", "metrics", "logs"})) {
    return "ANALYTICAL";
  }
  if (containsAnyPattern(schema, {"master", "reference", "lookup"})) {
    return "REFERENCE";
  }
  if (containsAnyPattern(schema, {"transaction", "operational", "business"})) {
    return "TRANSACTIONAL";
  }
  if (containsAnyPattern(schema, {"sport", "betting", "bookmaker"})) {
    return "SPORTS";
  }

  // Table name-based classification
  std::vector<std::string> analyticalPatterns = {"log", "audit", "history", "archive", "metrics", "analytics", "stats"};
  if (containsAnyPattern(name, analyticalPatterns)) {
    return "ANALYTICAL";
  }

  std::vector<std::string> referencePatterns = {"ref", "lookup", "config", "master", "dictionary", "catalog"};
  if (containsAnyPattern(name, referencePatterns)) {
    return "REFERENCE";
  }

  std::vector<std::string> masterDataPatterns = {"customer", "product", "supplier", "location", "employee", "user"};
  if (containsAnyPattern(name, masterDataPatterns)) {
    return "MASTER_DATA";
  }

  std::vector<std::string> operationalPatterns = {"session", "workflow", "process", "state", "status", "queue"};
  if (containsAnyPattern(name, operationalPatterns)) {
    return "OPERATIONAL";
  }

  std::vector<std::string> temporalPatterns = {"schedule", "event", "appointment", "deadline", "calendar"};
  if (containsAnyPattern(name, temporalPatterns)) {
    return "TEMPORAL";
  }

  std::vector<std::string> geospatialPatterns = {"coordinate", "address", "region", "zone", "location", "geo"};
  if (containsAnyPattern(name, geospatialPatterns)) {
    return "GEOSPATIAL";
  }

  std::vector<std::string> financialPatterns = {"account", "budget", "forecast", "report", "invoice", "payment"};
  if (containsAnyPattern(name, financialPatterns)) {
    return "FINANCIAL";
  }

  std::vector<std::string> compliancePatterns = {"gdpr", "sox", "pci", "regulation", "compliance"};
  if (containsAnyPattern(name, compliancePatterns)) {
    return "COMPLIANCE";
  }

  std::vector<std::string> technicalPatterns = {"system", "infrastructure", "monitoring", "alert", "error"};
  if (containsAnyPattern(name, technicalPatterns)) {
    return "TECHNICAL";
  }

  std::vector<std::string> sportsPatterns = {"sport", "bet", "betting", "odds", "match", "game", "team", "player", 
                                            "league", "tournament", "championship", "season", "fixture", "result", 
                                            "score", "statistic", "performance", "ranking", "standings", "bookmaker", 
                                            "bookie", "stake", "wager", "payout", "winner", "loser", "draw", 
                                            "handicap", "spread", "over_under", "live_bet", "in_play", "pre_match", 
                                            "outcome", "event", "competition", "sportbook", "sportsbook"};
  if (containsAnyPattern(name, sportsPatterns)) {
    return "SPORTS";
  }

  return "TRANSACTIONAL";
}

std::string DataClassifier::determineBusinessDomain(const std::string &table_name, const std::string &schema_name) {
  std::string name = toLowerCase(table_name);

  std::vector<std::string> customerPatterns = {"user", "customer", "client", "profile", "preference"};
  if (containsAnyPattern(name, customerPatterns)) {
    return "CUSTOMER";
  }

  std::vector<std::string> salesPatterns = {"order", "sale", "transaction", "deal", "opportunity", "quote"};
  if (containsAnyPattern(name, salesPatterns)) {
    return "SALES";
  }

  std::vector<std::string> marketingPatterns = {"campaign", "lead", "segment", "promotion", "marketing"};
  if (containsAnyPattern(name, marketingPatterns)) {
    return "MARKETING";
  }

  std::vector<std::string> hrPatterns = {"employee", "hr", "payroll", "benefit", "performance", "training"};
  if (containsAnyPattern(name, hrPatterns)) {
    return "HR";
  }

  std::vector<std::string> financePatterns = {"finance", "account", "payment", "budget", "invoice"};
  if (containsAnyPattern(name, financePatterns)) {
    return "FINANCE";
  }

  std::vector<std::string> inventoryPatterns = {"product", "inventory", "stock", "supplier", "warehouse"};
  if (containsAnyPattern(name, inventoryPatterns)) {
    return "INVENTORY";
  }

  std::vector<std::string> operationsPatterns = {"process", "workflow", "task", "schedule", "operation"};
  if (containsAnyPattern(name, operationsPatterns)) {
    return "OPERATIONS";
  }

  std::vector<std::string> supportPatterns = {"ticket", "case", "knowledge", "faq", "support"};
  if (containsAnyPattern(name, supportPatterns)) {
    return "SUPPORT";
  }

  std::vector<std::string> securityPatterns = {"access", "permission", "audit", "compliance", "security"};
  if (containsAnyPattern(name, securityPatterns)) {
    return "SECURITY";
  }

  std::vector<std::string> analyticsPatterns = {"report", "dashboard", "metric", "kpi", "analytics"};
  if (containsAnyPattern(name, analyticsPatterns)) {
    return "ANALYTICS";
  }

  std::vector<std::string> communicationPatterns = {"message", "notification", "alert", "communication"};
  if (containsAnyPattern(name, communicationPatterns)) {
    return "COMMUNICATION";
  }

  std::vector<std::string> legalPatterns = {"contract", "agreement", "policy", "terms", "legal"};
  if (containsAnyPattern(name, legalPatterns)) {
    return "LEGAL";
  }

  std::vector<std::string> researchPatterns = {"study", "experiment", "survey", "research"};
  if (containsAnyPattern(name, researchPatterns)) {
    return "RESEARCH";
  }

  std::vector<std::string> manufacturingPatterns = {"production", "quality", "material", "manufacturing"};
  if (containsAnyPattern(name, manufacturingPatterns)) {
    return "MANUFACTURING";
  }

  std::vector<std::string> logisticsPatterns = {"shipping", "tracking", "delivery", "route", "logistics"};
  if (containsAnyPattern(name, logisticsPatterns)) {
    return "LOGISTICS";
  }

  std::vector<std::string> healthcarePatterns = {"patient", "record", "treatment", "drug", "healthcare"};
  if (containsAnyPattern(name, healthcarePatterns)) {
    return "HEALTHCARE";
  }

  std::vector<std::string> educationPatterns = {"student", "course", "grade", "curriculum", "education"};
  if (containsAnyPattern(name, educationPatterns)) {
    return "EDUCATION";
  }

  std::vector<std::string> realEstatePatterns = {"property", "lease", "tenant", "maintenance", "real_estate"};
  if (containsAnyPattern(name, realEstatePatterns)) {
    return "REAL_ESTATE";
  }

  std::vector<std::string> insurancePatterns = {"policy", "claim", "coverage", "risk", "insurance"};
  if (containsAnyPattern(name, insurancePatterns)) {
    return "INSURANCE";
  }

  // Sports patterns (same as data category)
  std::vector<std::string> sportsPatterns = {"sport", "bet", "betting", "odds", "match", "game", "team", "player", 
                                            "league", "tournament", "championship", "season", "fixture", "result", 
                                            "score", "statistic", "performance", "ranking", "standings", "bookmaker", 
                                            "bookie", "stake", "wager", "payout", "winner", "loser", "draw", 
                                            "handicap", "spread", "over_under", "live_bet", "in_play", "pre_match", 
                                            "outcome", "event", "competition", "sportbook", "sportsbook"};
  if (containsAnyPattern(name, sportsPatterns)) {
    return "SPORTS";
  }

  return "GENERAL";
}

std::string DataClassifier::determineSensitivityLevel(const std::string &table_name, const std::string &schema_name) {
  std::string name = toLowerCase(table_name);

  std::vector<std::string> criticalPatterns = {"password", "ssn", "credit", "bank", "medical", "biometric", "secret", "private_key"};
  if (containsAnyPattern(name, criticalPatterns)) {
    return "CRITICAL";
  }

  std::vector<std::string> highPatterns = {"email", "phone", "address", "personal", "financial", "salary", "social", "identity"};
  if (containsAnyPattern(name, highPatterns)) {
    return "HIGH";
  }

  std::vector<std::string> mediumPatterns = {"name", "age", "preference", "business", "internal", "confidential"};
  if (containsAnyPattern(name, mediumPatterns)) {
    return "MEDIUM";
  }

  std::vector<std::string> publicPatterns = {"marketing", "public", "announcement", "general", "catalog", "reference"};
  if (containsAnyPattern(name, publicPatterns)) {
    return "PUBLIC";
  }

  std::vector<std::string> sportsPatterns = {"bet", "betting", "wager", "stake", "payout", "bookmaker", "bookie", "sportbook", "sportsbook"};
  if (containsAnyPattern(name, sportsPatterns)) {
    return "HIGH"; // Betting data is highly sensitive
  }

  return "LOW";
}

std::string DataClassifier::determineDataClassification(const std::string &table_name, const std::string &schema_name) {
  std::string name = toLowerCase(table_name);

  std::vector<std::string> confidentialPatterns = {"confidential", "secret", "private"};
  if (containsAnyPattern(name, confidentialPatterns)) {
    return "CONFIDENTIAL";
  }

  std::vector<std::string> internalPatterns = {"internal", "restricted"};
  if (containsAnyPattern(name, internalPatterns)) {
    return "INTERNAL";
  }

  std::vector<std::string> publicPatterns = {"public", "open"};
  if (containsAnyPattern(name, publicPatterns)) {
    return "PUBLIC";
  }

  std::vector<std::string> sportsPatterns = {"bet", "betting", "wager", "stake", "payout", "bookmaker", "bookie", "sportbook", "sportsbook"};
  if (containsAnyPattern(name, sportsPatterns)) {
    return "CONFIDENTIAL"; // Betting data is confidential
  }

  return "INTERNAL";
}

std::string DataClassifier::determineRetentionPolicy(const std::string &data_category, const std::string &sensitivity_level) {
  if (sensitivity_level == "CRITICAL") {
    return "7_YEARS";
  }
  if (sensitivity_level == "HIGH") {
    return "5_YEARS";
  }
  if (data_category == "ANALYTICAL") {
    return "3_YEARS";
  }
  if (data_category == "TRANSACTIONAL") {
    return "2_YEARS";
  }
  if (data_category == "SPORTS") {
    return "3_YEARS"; // Sports data requires longer retention for compliance
  }
  return "1_YEAR";
}

std::string DataClassifier::determineBackupFrequency(const std::string &data_category, const std::string &access_frequency) {
  if (access_frequency == "REAL_TIME" || access_frequency == "HIGH") {
    return "HOURLY";
  }
  if (data_category == "TRANSACTIONAL" || data_category == "MASTER_DATA") {
    return "DAILY";
  }
  if (data_category == "ANALYTICAL") {
    return "WEEKLY";
  }
  if (data_category == "SPORTS") {
    return "DAILY"; // Sports data requires frequent backups due to high value
  }
  return "MONTHLY";
}

std::string DataClassifier::determineComplianceRequirements(const std::string &sensitivity_level, const std::string &business_domain) {
  if (sensitivity_level == "CRITICAL" || sensitivity_level == "HIGH") {
    if (business_domain == "HEALTHCARE") {
      return "HIPAA";
    }
    if (business_domain == "FINANCE") {
      return "SOX,PCI";
    }
    if (business_domain == "SPORTS") {
      return "GDPR,PCI,AML"; // Sports betting requires GDPR, PCI, and AML compliance
    }
    return "GDPR";
  }
  if (business_domain == "HEALTHCARE") {
    return "HIPAA";
  }
  if (business_domain == "FINANCE") {
    return "SOX";
  }
  if (business_domain == "SPORTS") {
    return "GDPR,AML"; // Sports betting requires GDPR and AML compliance
  }
  return "GDPR";
}

bool DataClassifier::containsAnyPattern(const std::string &text, const std::vector<std::string> &patterns) {
  for (const auto &pattern : patterns) {
    if (text.find(pattern) != std::string::npos) {
      return true;
    }
  }
  return false;
}

std::string DataClassifier::toLowerCase(const std::string &text) {
  std::string result = text;
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  return result;
}
