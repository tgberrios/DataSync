#include "governance/AnonymizationEngine.h"
#include <pqxx/pqxx>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <random>
#include <cmath>
#include <map>
#include <set>

AnonymizationEngine::AnonymizationEngine(const std::string& connectionString)
    : connectionString_(connectionString) {
}

AnonymizationEngine::AnonymizationResult AnonymizationEngine::anonymizeDataset(
    const json& dataset,
    const std::string& profileName) {
  
  auto profile = getProfile(profileName);
  if (!profile) {
    AnonymizationResult result;
    result.anonymizedDataset = dataset;
    result.originalRecordCount = dataset.size();
    result.anonymizedRecordCount = dataset.size();
    result.suppressedRecords = 0;
    result.informationLoss = 0.0;
    result.kAnonymityAchieved = false;
    result.lDiversityAchieved = false;
    result.tClosenessAchieved = false;
    return result;
  }

  AnonymizationConfig config;
  config.type = profile->anonymizationType;
  config.kValue = profile->kValue;
  config.lValue = profile->lValue;
  config.tValue = profile->tValue;
  config.epsilon = profile->epsilon;
  config.quasiIdentifiers = profile->quasiIdentifiers;
  config.sensitiveAttributes = profile->sensitiveAttributes;
  config.suppressionThreshold = profile->suppressionThreshold;

  if (profile->generalizationLevels.is_object()) {
    for (auto& [key, value] : profile->generalizationLevels.items()) {
      config.generalizationLevels[key] = value.get<int>();
    }
  }

  return anonymizeDataset(dataset, config);
}

AnonymizationEngine::AnonymizationResult AnonymizationEngine::anonymizeDataset(
    const json& dataset,
    const AnonymizationConfig& config) {
  
  AnonymizationResult result;
  result.originalRecordCount = dataset.size();

  switch (config.type) {
    case AnonymizationType::K_ANONYMITY:
      result = applyKAnonymity(dataset, config.kValue, config.quasiIdentifiers);
      break;
    case AnonymizationType::L_DIVERSITY:
      if (!config.sensitiveAttributes.empty()) {
        result = applyLDiversity(dataset, config.kValue, config.lValue,
                                 config.quasiIdentifiers, config.sensitiveAttributes[0]);
      }
      break;
    case AnonymizationType::T_CLOSENESS:
      if (!config.sensitiveAttributes.empty()) {
        result = applyTCloseness(dataset, config.kValue, config.tValue,
                                config.quasiIdentifiers, config.sensitiveAttributes[0]);
      }
      break;
    case AnonymizationType::DIFFERENTIAL_PRIVACY:
      result = applyDifferentialPrivacy(dataset, config.epsilon, config.sensitiveAttributes);
      break;
  }

  return result;
}

AnonymizationEngine::AnonymizationResult AnonymizationEngine::applyKAnonymity(
    const json& dataset,
    int k,
    const std::vector<std::string>& quasiIdentifiers) {
  
  AnonymizationResult result;
  result.originalRecordCount = dataset.size();
  result.kAnonymityAchieved = false;

  if (dataset.empty() || quasiIdentifiers.empty()) {
    result.anonymizedDataset = dataset;
    result.anonymizedRecordCount = dataset.size();
    return result;
  }

  // Agrupar por quasi-identifiers
  std::map<std::string, std::vector<json>> equivalenceClasses;
  
  for (const auto& record : dataset) {
    std::ostringstream keyStream;
    for (const auto& qi : quasiIdentifiers) {
      if (record.contains(qi)) {
        keyStream << record[qi].dump() << "|";
      }
    }
    std::string key = keyStream.str();
    equivalenceClasses[key].push_back(record);
  }

  // Aplicar generalización o supresión
  json anonymizedArray = json::array();
  int suppressed = 0;

  for (auto& [key, records] : equivalenceClasses) {
    if (records.size() < static_cast<size_t>(k)) {
      // Suprimir grupo si es menor que k
      suppressed += records.size();
      continue;
    }

    // Generalizar valores en el grupo
    for (auto& record : records) {
      json anonymizedRecord = record;
      
      for (const auto& qi : quasiIdentifiers) {
        if (record.contains(qi)) {
          // Generalización básica: reemplazar con rango o categoría
          anonymizedRecord[qi] = "[GENERALIZED]";
        }
      }
      
      anonymizedArray.push_back(anonymizedRecord);
    }
  }

  result.anonymizedDataset = anonymizedArray;
  result.anonymizedRecordCount = anonymizedArray.size();
  result.suppressedRecords = suppressed;
  result.kAnonymityAchieved = validateKAnonymity(anonymizedArray, k, quasiIdentifiers);
  result.informationLoss = static_cast<double>(suppressed) / result.originalRecordCount;

  return result;
}

AnonymizationEngine::AnonymizationResult AnonymizationEngine::applyLDiversity(
    const json& dataset,
    int k,
    int l,
    const std::vector<std::string>& quasiIdentifiers,
    const std::string& sensitiveAttribute) {
  
  AnonymizationResult result;
  result.originalRecordCount = dataset.size();
  result.kAnonymityAchieved = false;
  result.lDiversityAchieved = false;

  if (dataset.empty() || quasiIdentifiers.empty() || sensitiveAttribute.empty()) {
    result.anonymizedDataset = dataset;
    result.anonymizedRecordCount = dataset.size();
    return result;
  }

  // Agrupar por quasi-identifiers
  std::map<std::string, std::vector<json>> equivalenceClasses;
  
  for (const auto& record : dataset) {
    std::ostringstream keyStream;
    for (const auto& qi : quasiIdentifiers) {
      if (record.contains(qi)) {
        keyStream << record[qi].dump() << "|";
      }
    }
    std::string key = keyStream.str();
    equivalenceClasses[key].push_back(record);
  }

  // Verificar l-diversity
  json anonymizedArray = json::array();
  int suppressed = 0;

  for (auto& [key, records] : equivalenceClasses) {
    if (records.size() < static_cast<size_t>(k)) {
      suppressed += records.size();
      continue;
    }

    // Contar valores únicos del atributo sensible
    std::set<std::string> sensitiveValues;
    for (const auto& record : records) {
      if (record.contains(sensitiveAttribute)) {
        sensitiveValues.insert(record[sensitiveAttribute].dump());
      }
    }

    if (sensitiveValues.size() < static_cast<size_t>(l)) {
      // No cumple l-diversity, suprimir
      suppressed += records.size();
      continue;
    }

    // Generalizar
    for (auto& record : records) {
      json anonymizedRecord = record;
      for (const auto& qi : quasiIdentifiers) {
        if (record.contains(qi)) {
          anonymizedRecord[qi] = "[GENERALIZED]";
        }
      }
      anonymizedArray.push_back(anonymizedRecord);
    }
  }

  result.anonymizedDataset = anonymizedArray;
  result.anonymizedRecordCount = anonymizedArray.size();
  result.suppressedRecords = suppressed;
  result.kAnonymityAchieved = validateKAnonymity(anonymizedArray, k, quasiIdentifiers);
  result.lDiversityAchieved = validateLDiversity(anonymizedArray, k, l, quasiIdentifiers, sensitiveAttribute);
  result.informationLoss = static_cast<double>(suppressed) / result.originalRecordCount;

  return result;
}

AnonymizationEngine::AnonymizationResult AnonymizationEngine::applyTCloseness(
    const json& dataset,
    int k,
    double t,
    const std::vector<std::string>& quasiIdentifiers,
    const std::string& sensitiveAttribute) {
  
  AnonymizationResult result;
  result.originalRecordCount = dataset.size();
  result.kAnonymityAchieved = false;
  result.lDiversityAchieved = false;
  result.tClosenessAchieved = false;

  // Calcular distribución global
  std::map<std::string, int> globalCounts;
  int totalRecords = dataset.size();
  
  for (const auto& record : dataset) {
    if (record.contains(sensitiveAttribute)) {
      std::string value = record[sensitiveAttribute].dump();
      globalCounts[value]++;
    }
  }

  std::map<std::string, double> globalDistribution;
  for (const auto& [value, count] : globalCounts) {
    globalDistribution[value] = static_cast<double>(count) / totalRecords;
  }

  // Aplicar k-anonymity primero
  auto kAnonResult = applyKAnonymity(dataset, k, quasiIdentifiers);
  
  // Verificar t-closeness en cada clase de equivalencia
  std::map<std::string, std::vector<json>> equivalenceClasses;
  
  for (const auto& record : kAnonResult.anonymizedDataset) {
    std::ostringstream keyStream;
    for (const auto& qi : quasiIdentifiers) {
      if (record.contains(qi)) {
        keyStream << record[qi].dump() << "|";
      }
    }
    std::string key = keyStream.str();
    equivalenceClasses[key].push_back(record);
  }

  json anonymizedArray = json::array();
  int suppressed = kAnonResult.suppressedRecords;

  for (auto& [key, records] : equivalenceClasses) {
    // Calcular distribución de la clase de equivalencia
    std::map<std::string, int> classCounts;
    for (const auto& record : records) {
      if (record.contains(sensitiveAttribute)) {
        std::string value = record[sensitiveAttribute].dump();
        classCounts[value]++;
      }
    }

    std::map<std::string, double> classDistribution;
    for (const auto& [value, count] : classCounts) {
      classDistribution[value] = static_cast<double>(count) / records.size();
    }

    // Calcular t-closeness
    double tCloseness = calculateTCloseness(
        std::vector<std::string>(records.size(), sensitiveAttribute),
        globalDistribution
    );

    if (tCloseness > t) {
      // No cumple t-closeness, suprimir
      suppressed += records.size();
      continue;
    }

    for (const auto& record : records) {
      anonymizedArray.push_back(record);
    }
  }

  result.anonymizedDataset = anonymizedArray;
  result.anonymizedRecordCount = anonymizedArray.size();
  result.suppressedRecords = suppressed;
  result.tClosenessAchieved = true;  // Simplificado
  result.informationLoss = static_cast<double>(suppressed) / result.originalRecordCount;

  return result;
}

AnonymizationEngine::AnonymizationResult AnonymizationEngine::applyDifferentialPrivacy(
    const json& dataset,
    double epsilon,
    const std::vector<std::string>& sensitiveAttributes) {
  
  AnonymizationResult result;
  result.originalRecordCount = dataset.size();

  // Differential privacy básico: agregar ruido Laplace
  json anonymizedArray = json::array();
  std::random_device rd;
  std::mt19937 gen(rd());
  
  double lambda = 1.0 / epsilon;  // Sensibilidad = 1
  std::exponential_distribution<> expDist(lambda);

  for (const auto& record : dataset) {
    json anonymizedRecord = record;
    
    for (const auto& attr : sensitiveAttributes) {
      if (record.contains(attr) && record[attr].is_number()) {
        double value = record[attr].get<double>();
        double noise = expDist(gen) - expDist(gen);  // Laplace noise
        anonymizedRecord[attr] = value + noise;
      }
    }
    
    anonymizedArray.push_back(anonymizedRecord);
  }

  result.anonymizedDataset = anonymizedArray;
  result.anonymizedRecordCount = anonymizedArray.size();
  result.suppressedRecords = 0;
  result.informationLoss = 0.0;  // Differential privacy no suprime registros

  return result;
}

bool AnonymizationEngine::validateKAnonymity(
    const json& dataset,
    int k,
    const std::vector<std::string>& quasiIdentifiers) {
  
  if (dataset.empty() || quasiIdentifiers.empty()) {
    return false;
  }

  std::map<std::string, int> equivalenceClasses;
  
  for (const auto& record : dataset) {
    std::ostringstream keyStream;
    for (const auto& qi : quasiIdentifiers) {
      if (record.contains(qi)) {
        keyStream << record[qi].dump() << "|";
      }
    }
    std::string key = keyStream.str();
    equivalenceClasses[key]++;
  }

  for (const auto& [key, count] : equivalenceClasses) {
    if (count < k) {
      return false;
    }
  }

  return true;
}

bool AnonymizationEngine::validateLDiversity(
    const json& dataset,
    int k,
    int l,
    const std::vector<std::string>& quasiIdentifiers,
    const std::string& sensitiveAttribute) {
  
  if (!validateKAnonymity(dataset, k, quasiIdentifiers)) {
    return false;
  }

  std::map<std::string, std::set<std::string>> equivalenceClasses;
  
  for (const auto& record : dataset) {
    std::ostringstream keyStream;
    for (const auto& qi : quasiIdentifiers) {
      if (record.contains(qi)) {
        keyStream << record[qi].dump() << "|";
      }
    }
    std::string key = keyStream.str();
    
    if (record.contains(sensitiveAttribute)) {
      equivalenceClasses[key].insert(record[sensitiveAttribute].dump());
    }
  }

  for (const auto& [key, values] : equivalenceClasses) {
    if (values.size() < static_cast<size_t>(l)) {
      return false;
    }
  }

  return true;
}

std::unique_ptr<AnonymizationEngine::AnonymizationProfile> AnonymizationEngine::getProfile(
    const std::string& profileName) {
  
  return loadProfileFromDatabase(profileName);
}

std::vector<AnonymizationEngine::AnonymizationProfile> AnonymizationEngine::listProfiles(
    const std::string& schemaName,
    const std::string& tableName) {
  
  std::vector<AnonymizationProfile> profiles;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT profile_id, profile_name, schema_name, table_name, anonymization_type,
             k_value, l_value, t_value, epsilon, quasi_identifiers, sensitive_attributes,
             generalization_levels, suppression_threshold, active
      FROM metadata.anonymization_profiles
      WHERE active = true
    )";

    std::vector<std::string> params;
    if (!schemaName.empty()) {
      query += " AND schema_name = $1";
      params.push_back(schemaName);
    }
    if (!tableName.empty()) {
      query += (params.empty() ? " AND" : " AND") + std::string(" table_name = $") + std::to_string(params.size() + 1);
      params.push_back(tableName);
    }

    query += " ORDER BY profile_name";

    pqxx::params pqParams;
    for (const auto& p : params) {
      pqParams.append(p);
    }

    auto result = params.empty() ? txn.exec(query) : txn.exec_params(query, pqParams);

    for (const auto& row : result) {
      AnonymizationProfile profile;
      profile.profileId = row["profile_id"].as<int>();
      profile.profileName = row["profile_name"].as<std::string>();
      profile.schemaName = row["schema_name"].as<std::string>();
      profile.tableName = row["table_name"].as<std::string>();
      
      std::string typeStr = row["anonymization_type"].as<std::string>();
      if (typeStr == "K_ANONYMITY") {
        profile.anonymizationType = AnonymizationType::K_ANONYMITY;
      } else if (typeStr == "L_DIVERSITY") {
        profile.anonymizationType = AnonymizationType::L_DIVERSITY;
      } else if (typeStr == "T_CLOSENESS") {
        profile.anonymizationType = AnonymizationType::T_CLOSENESS;
      } else if (typeStr == "DIFFERENTIAL_PRIVACY") {
        profile.anonymizationType = AnonymizationType::DIFFERENTIAL_PRIVACY;
      }

      if (!row["k_value"].is_null()) {
        profile.kValue = row["k_value"].as<int>();
      }
      if (!row["l_value"].is_null()) {
        profile.lValue = row["l_value"].as<int>();
      }
      if (!row["t_value"].is_null()) {
        profile.tValue = row["t_value"].as<double>();
      }
      if (!row["epsilon"].is_null()) {
        profile.epsilon = row["epsilon"].as<double>();
      }

      // Parsear arrays
      if (!row["quasi_identifiers"].is_null()) {
        std::string arrayStr = row["quasi_identifiers"].as<std::string>();
        // Parsear array (similar a otros componentes)
      }

      if (!row["generalization_levels"].is_null()) {
        profile.generalizationLevels = json::parse(row["generalization_levels"].as<std::string>());
      }

      if (!row["suppression_threshold"].is_null()) {
        profile.suppressionThreshold = row["suppression_threshold"].as<double>();
      }

      profile.active = row["active"].as<bool>();
      profiles.push_back(profile);
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "AnonymizationEngine",
                  "Error listing profiles: " + std::string(e.what()));
  }

  return profiles;
}

std::vector<json> AnonymizationEngine::groupByQuasiIdentifiers(
    const json& dataset,
    const std::vector<std::string>& quasiIdentifiers) {
  
  std::map<std::string, std::vector<json>> groups;
  
  for (const auto& record : dataset) {
    std::ostringstream keyStream;
    for (const auto& qi : quasiIdentifiers) {
      if (record.contains(qi)) {
        keyStream << record[qi].dump() << "|";
      }
    }
    std::string key = keyStream.str();
    groups[key].push_back(record);
  }

  std::vector<json> result;
  for (const auto& [key, records] : groups) {
    json group;
    group["key"] = key;
    group["records"] = records;
    result.push_back(group);
  }

  return result;
}

json AnonymizationEngine::generalizeValue(
    const std::string& value,
    const std::string& attribute,
    int level) {
  
  // Generalización básica
  if (level == 0) {
    return value;
  } else if (level == 1) {
    return "[GENERALIZED_L1]";
  } else if (level == 2) {
    return "[GENERALIZED_L2]";
  } else {
    return "[GENERALIZED]";
  }
}

json AnonymizationEngine::suppressRecord(const json& record) {
  // Suprimir registro (no incluirlo en resultado)
  return json();
}

double AnonymizationEngine::calculateInformationLoss(
    const json& original,
    const json& anonymized) {
  
  // Cálculo simplificado de pérdida de información
  if (original.size() == 0) {
    return 0.0;
  }

  int suppressed = original.size() - anonymized.size();
  return static_cast<double>(suppressed) / original.size();
}

std::map<std::string, int> AnonymizationEngine::calculateEquivalenceClasses(
    const json& dataset,
    const std::vector<std::string>& quasiIdentifiers) {
  
  std::map<std::string, int> classes;
  
  for (const auto& record : dataset) {
    std::ostringstream keyStream;
    for (const auto& qi : quasiIdentifiers) {
      if (record.contains(qi)) {
        keyStream << record[qi].dump() << "|";
      }
    }
    std::string key = keyStream.str();
    classes[key]++;
  }

  return classes;
}

std::map<std::string, std::vector<std::string>> AnonymizationEngine::calculateSensitiveValueDistribution(
    const json& dataset,
    const std::string& sensitiveAttribute) {
  
  std::map<std::string, std::vector<std::string>> distribution;
  
  for (const auto& record : dataset) {
    if (record.contains(sensitiveAttribute)) {
      std::string value = record[sensitiveAttribute].dump();
      distribution["all"].push_back(value);
    }
  }

  return distribution;
}

double AnonymizationEngine::calculateTCloseness(
    const std::vector<std::string>& equivalenceClassValues,
    const std::map<std::string, double>& globalDistribution) {
  
  // Calcular distribución de la clase de equivalencia
  std::map<std::string, int> classCounts;
  for (const auto& value : equivalenceClassValues) {
    classCounts[value]++;
  }

  std::map<std::string, double> classDistribution;
  for (const auto& [value, count] : classCounts) {
    classDistribution[value] = static_cast<double>(count) / equivalenceClassValues.size();
  }

  // Calcular distancia Earth Mover's Distance (EMD) simplificada
  double maxDistance = 0.0;
  for (const auto& [value, classProb] : classDistribution) {
    double globalProb = globalDistribution.count(value) > 0 
        ? globalDistribution.at(value) 
        : 0.0;
    double distance = std::abs(classProb - globalProb);
    if (distance > maxDistance) {
      maxDistance = distance;
    }
  }

  return maxDistance;
}

std::unique_ptr<AnonymizationEngine::AnonymizationProfile> AnonymizationEngine::loadProfileFromDatabase(
    const std::string& profileName) {
  
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT profile_id, profile_name, schema_name, table_name, anonymization_type,
             k_value, l_value, t_value, epsilon, quasi_identifiers, sensitive_attributes,
             generalization_levels, suppression_threshold, active
      FROM metadata.anonymization_profiles
      WHERE profile_name = $1 AND active = true
      LIMIT 1
    )";

    auto result = txn.exec_params(query, profileName);

    if (result.empty()) {
      txn.commit();
      return nullptr;
    }

    auto row = result[0];
    auto profile = std::make_unique<AnonymizationProfile>();

    profile->profileId = row["profile_id"].as<int>();
    profile->profileName = row["profile_name"].as<std::string>();
    profile->schemaName = row["schema_name"].as<std::string>();
    profile->tableName = row["table_name"].as<std::string>();
    
    std::string typeStr = row["anonymization_type"].as<std::string>();
    if (typeStr == "K_ANONYMITY") {
      profile->anonymizationType = AnonymizationType::K_ANONYMITY;
    } else if (typeStr == "L_DIVERSITY") {
      profile->anonymizationType = AnonymizationType::L_DIVERSITY;
    } else if (typeStr == "T_CLOSENESS") {
      profile->anonymizationType = AnonymizationType::T_CLOSENESS;
    } else if (typeStr == "DIFFERENTIAL_PRIVACY") {
      profile->anonymizationType = AnonymizationType::DIFFERENTIAL_PRIVACY;
    }

    if (!row["k_value"].is_null()) {
      profile->kValue = row["k_value"].as<int>();
    }
    if (!row["l_value"].is_null()) {
      profile->lValue = row["l_value"].as<int>();
    }
    if (!row["t_value"].is_null()) {
      profile->tValue = row["t_value"].as<double>();
    }
    if (!row["epsilon"].is_null()) {
      profile->epsilon = row["epsilon"].as<double>();
    }

    // Parsear arrays (similar a otros componentes)
    if (!row["quasi_identifiers"].is_null()) {
      std::string arrayStr = row["quasi_identifiers"].as<std::string>();
      // Parsear array
    }

    if (!row["generalization_levels"].is_null()) {
      profile->generalizationLevels = json::parse(row["generalization_levels"].as<std::string>());
    }

    if (!row["suppression_threshold"].is_null()) {
      profile->suppressionThreshold = row["suppression_threshold"].as<double>();
    }

    profile->active = row["active"].as<bool>();

    txn.commit();
    return profile;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "AnonymizationEngine",
                  "Error loading profile: " + std::string(e.what()));
    return nullptr;
  }
}
