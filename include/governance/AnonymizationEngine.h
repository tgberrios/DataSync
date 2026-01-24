#ifndef ANONYMIZATION_ENGINE_H
#define ANONYMIZATION_ENGINE_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>
#include <memory>

using json = nlohmann::json;

// AnonymizationEngine: Motor de anonymization con k-anonymity, l-diversity, t-closeness, differential privacy
class AnonymizationEngine {
public:
  enum class AnonymizationType {
    K_ANONYMITY,
    L_DIVERSITY,
    T_CLOSENESS,
    DIFFERENTIAL_PRIVACY
  };

  struct AnonymizationConfig {
    AnonymizationType type;
    int kValue;
    int lValue;
    double tValue;
    double epsilon;  // Para differential privacy
    std::vector<std::string> quasiIdentifiers;
    std::vector<std::string> sensitiveAttributes;
    std::map<std::string, int> generalizationLevels;
    double suppressionThreshold;
  };

  struct AnonymizationResult {
    json anonymizedDataset;
    int originalRecordCount;
    int anonymizedRecordCount;
    int suppressedRecords;
    double informationLoss;
    bool kAnonymityAchieved;
    bool lDiversityAchieved;
    bool tClosenessAchieved;
    std::map<std::string, double> metrics;
  };

  struct AnonymizationProfile {
    int profileId;
    std::string profileName;
    std::string schemaName;
    std::string tableName;
    AnonymizationType anonymizationType;
    int kValue;
    int lValue;
    double tValue;
    double epsilon;
    std::vector<std::string> quasiIdentifiers;
    std::vector<std::string> sensitiveAttributes;
    json generalizationLevels;
    double suppressionThreshold;
    bool active;
  };

  explicit AnonymizationEngine(const std::string& connectionString);
  ~AnonymizationEngine() = default;

  // Anonymizar dataset usando un perfil
  AnonymizationResult anonymizeDataset(
      const json& dataset,
      const std::string& profileName
  );

  // Anonymizar dataset con configuración directa
  AnonymizationResult anonymizeDataset(
      const json& dataset,
      const AnonymizationConfig& config
  );

  // Aplicar k-anonymity
  AnonymizationResult applyKAnonymity(
      const json& dataset,
      int k,
      const std::vector<std::string>& quasiIdentifiers
  );

  // Aplicar l-diversity
  AnonymizationResult applyLDiversity(
      const json& dataset,
      int k,
      int l,
      const std::vector<std::string>& quasiIdentifiers,
      const std::string& sensitiveAttribute
  );

  // Aplicar t-closeness
  AnonymizationResult applyTCloseness(
      const json& dataset,
      int k,
      double t,
      const std::vector<std::string>& quasiIdentifiers,
      const std::string& sensitiveAttribute
  );

  // Aplicar differential privacy
  AnonymizationResult applyDifferentialPrivacy(
      const json& dataset,
      double epsilon,
      const std::vector<std::string>& sensitiveAttributes
  );

  // Validar k-anonymity
  bool validateKAnonymity(
      const json& dataset,
      int k,
      const std::vector<std::string>& quasiIdentifiers
  );

  // Validar l-diversity
  bool validateLDiversity(
      const json& dataset,
      int k,
      int l,
      const std::vector<std::string>& quasiIdentifiers,
      const std::string& sensitiveAttribute
  );

  // Obtener perfil de anonymization
  std::unique_ptr<AnonymizationProfile> getProfile(const std::string& profileName);

  // Listar perfiles
  std::vector<AnonymizationProfile> listProfiles(
      const std::string& schemaName = "",
      const std::string& tableName = ""
  );

private:
  std::string connectionString_;

  // Helper methods
  std::vector<json> groupByQuasiIdentifiers(
      const json& dataset,
      const std::vector<std::string>& quasiIdentifiers
  );

  json generalizeValue(const std::string& value, const std::string& attribute, int level);
  json suppressRecord(const json& record);
  
  double calculateInformationLoss(
      const json& original,
      const json& anonymized
  );

  std::map<std::string, int> calculateEquivalenceClasses(
      const json& dataset,
      const std::vector<std::string>& quasiIdentifiers
  );

  std::map<std::string, std::vector<std::string>> calculateSensitiveValueDistribution(
      const json& dataset,
      const std::string& sensitiveAttribute
  );

  double calculateTCloseness(
      const std::vector<std::string>& equivalenceClassValues,
      const std::map<std::string, double>& globalDistribution
  );

  std::unique_ptr<AnonymizationProfile> loadProfileFromDatabase(const std::string& profileName);
};

#endif // ANONYMIZATION_ENGINE_H
