#include "governance/DynamicMaskingEngine.h"
#include <pqxx/pqxx>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <cstring>
#include <cctype>
#include <openssl/sha.h>
#include <openssl/md5.h>

DynamicMaskingEngine::DynamicMaskingEngine(const std::string& connectionString)
    : connectionString_(connectionString) {
}

std::string DynamicMaskingEngine::applyMasking(
    const std::string& value,
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    const std::string& username,
    const std::vector<std::string>& userRoles) {
  
  if (value.empty()) {
    return value;
  }

  // Obtener política de masking
  auto policy = getMaskingPolicy(schemaName, tableName, columnName);
  if (!policy) {
    return value;  // No hay política, retornar valor original
  }

  // Verificar si el usuario tiene acceso sin masking
  if (!shouldMaskForUser(*policy, username, userRoles)) {
    return value;  // Usuario tiene acceso sin masking
  }

  // Aplicar masking según el tipo
  if (policy->maskingType == "FULL") {
    return applyFullMasking(value);
  } else if (policy->maskingType == "PARTIAL") {
    return applyPartialMasking(value, policy->maskChar, policy->visibleChars);
  } else if (policy->maskingType == "EMAIL") {
    return applyEmailMasking(value);
  } else if (policy->maskingType == "PHONE") {
    return applyPhoneMasking(value);
  } else if (policy->maskingType == "HASH") {
    return applyHashMasking(value, policy->hashAlgorithm);
  } else if (policy->maskingType == "TOKENIZE") {
    return applyTokenizeMasking(value);
  }

  return value;
}

json DynamicMaskingEngine::applyMaskingToRow(
    const json& row,
    const std::string& schemaName,
    const std::string& tableName,
    const std::vector<std::string>& columns,
    const std::string& username,
    const std::vector<std::string>& userRoles) {
  
  json maskedRow = row;

  for (const auto& column : columns) {
    if (row.contains(column) && row[column].is_string()) {
      std::string originalValue = row[column].get<std::string>();
      std::string maskedValue = applyMasking(
          originalValue, schemaName, tableName, column, username, userRoles);
      maskedRow[column] = maskedValue;
    }
  }

  return maskedRow;
}

std::vector<json> DynamicMaskingEngine::applyMaskingToRows(
    const std::vector<json>& rows,
    const std::string& schemaName,
    const std::string& tableName,
    const std::vector<std::string>& columns,
    const std::string& username,
    const std::vector<std::string>& userRoles) {
  
  std::vector<json> maskedRows;
  maskedRows.reserve(rows.size());

  for (const auto& row : rows) {
    maskedRows.push_back(applyMaskingToRow(row, schemaName, tableName, columns, username, userRoles));
  }

  return maskedRows;
}

std::unique_ptr<DynamicMaskingEngine::MaskingPolicy> DynamicMaskingEngine::getMaskingPolicy(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  std::string cacheKey = generateCacheKey(schemaName, tableName, columnName);

  // Verificar cache
  {
    std::lock_guard<std::mutex> lock(cacheMutex_);
    auto it = policyCache_.find(cacheKey);
    if (it != policyCache_.end()) {
      return std::make_unique<MaskingPolicy>(*it->second);
    }
  }

  // Cargar desde base de datos
  auto policy = loadPolicyFromDatabase(schemaName, tableName, columnName);
  if (policy) {
    std::lock_guard<std::mutex> lock(cacheMutex_);
    policyCache_[cacheKey] = std::make_unique<MaskingPolicy>(*policy);
  }

  return policy;
}

bool DynamicMaskingEngine::shouldMaskForUser(
    const MaskingPolicy& policy,
    const std::string& username,
    const std::vector<std::string>& userRoles) {
  
  // Si la política no está activa, no aplicar masking
  if (!policy.active) {
    return false;
  }

  // Si no hay whitelist, aplicar masking a todos
  if (policy.roleWhitelist.empty()) {
    return true;
  }

  // Verificar si el usuario tiene algún rol en la whitelist
  for (const auto& role : userRoles) {
    if (std::find(policy.roleWhitelist.begin(), policy.roleWhitelist.end(), role) 
        != policy.roleWhitelist.end()) {
      return false;  // Usuario tiene rol permitido, no aplicar masking
    }
  }

  return true;  // Usuario no tiene rol permitido, aplicar masking
}

void DynamicMaskingEngine::invalidateCache() {
  std::lock_guard<std::mutex> lock(cacheMutex_);
  policyCache_.clear();
}

std::unique_ptr<DynamicMaskingEngine::MaskingPolicy> DynamicMaskingEngine::loadPolicyFromDatabase(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT policy_id, policy_name, schema_name, table_name, column_name,
             masking_type, masking_function, masking_params, role_whitelist, active
      FROM metadata.masking_policies
      WHERE schema_name = $1 AND table_name = $2 AND column_name = $3 AND active = true
      LIMIT 1
    )";

    auto result = txn.exec_params(query, schemaName, tableName, columnName);

    if (result.empty()) {
      txn.commit();
      return nullptr;
    }

    auto row = result[0];
    auto policy = std::make_unique<MaskingPolicy>();

    policy->policyId = row["policy_id"].as<int>();
    policy->policyName = row["policy_name"].as<std::string>();
    policy->schemaName = row["schema_name"].as<std::string>();
    policy->tableName = row["table_name"].as<std::string>();
    policy->columnName = row["column_name"].as<std::string>();
    policy->maskingType = row["masking_type"].as<std::string>();

    if (!row["masking_function"].is_null()) {
      policy->maskingAlgorithm = row["masking_function"].as<std::string>();
    } else {
      policy->maskingAlgorithm = "random";
    }

    // Parsear masking_params
    if (!row["masking_params"].is_null()) {
      policy->maskingParams = json::parse(row["masking_params"].as<std::string>());
      
      if (policy->maskingParams.contains("preserve_format")) {
        policy->preserveFormat = policy->maskingParams["preserve_format"].get<bool>();
      }
      if (policy->maskingParams.contains("mask_char")) {
        std::string maskCharStr = policy->maskingParams["mask_char"].get<std::string>();
        policy->maskChar = maskCharStr.empty() ? '*' : maskCharStr[0];
      } else {
        policy->maskChar = '*';
      }
      if (policy->maskingParams.contains("visible_chars")) {
        policy->visibleChars = policy->maskingParams["visible_chars"].get<int>();
      } else {
        policy->visibleChars = 0;
      }
      if (policy->maskingParams.contains("hash_algorithm")) {
        policy->hashAlgorithm = policy->maskingParams["hash_algorithm"].get<std::string>();
      } else {
        policy->hashAlgorithm = "SHA256";
      }
    } else {
      policy->preserveFormat = false;
      policy->maskChar = '*';
      policy->visibleChars = 0;
      policy->hashAlgorithm = "SHA256";
    }

    // Parsear role_whitelist
    if (!row["role_whitelist"].is_null()) {
      std::string arrayStr = row["role_whitelist"].as<std::string>();
      if (!arrayStr.empty() && arrayStr != "{}") {
        std::string content = arrayStr;
        if (content.front() == '{' && content.back() == '}') {
          content = content.substr(1, content.length() - 2);
        }
        std::istringstream iss(content);
        std::string item;
        while (std::getline(iss, item, ',')) {
          if (!item.empty()) {
            if (item.front() == '"' && item.back() == '"') {
              item = item.substr(1, item.length() - 2);
            }
            policy->roleWhitelist.push_back(item);
          }
        }
      }
    }

    policy->active = row["active"].as<bool>();

    txn.commit();
    return policy;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "DynamicMaskingEngine",
                  "Error loading masking policy: " + std::string(e.what()));
    return nullptr;
  }
}

std::string DynamicMaskingEngine::applyFullMasking(const std::string& value) {
  return std::string(value.length(), '*');
}

std::string DynamicMaskingEngine::applyPartialMasking(
    const std::string& value,
    char maskChar,
    int visibleChars) {
  
  if (value.length() <= static_cast<size_t>(visibleChars)) {
    return value;  // Valor muy corto, no aplicar masking
  }

  std::string masked = value;
  int charsToMask = value.length() - visibleChars;
  
  // Mantener primeros caracteres visibles
  for (size_t i = visibleChars; i < value.length(); ++i) {
    masked[i] = maskChar;
  }

  return masked;
}

std::string DynamicMaskingEngine::applyEmailMasking(const std::string& value) {
  size_t atPos = value.find('@');
  if (atPos == std::string::npos) {
    return applyFullMasking(value);  // No es un email válido
  }

  std::string localPart = value.substr(0, atPos);
  std::string domain = value.substr(atPos + 1);

  // Maskear local part, mantener dominio
  std::string maskedLocal = localPart.length() > 2 
      ? localPart.substr(0, 1) + std::string(localPart.length() - 2, '*') + localPart.substr(localPart.length() - 1)
      : std::string(localPart.length(), '*');

  return maskedLocal + "@" + domain;
}

std::string DynamicMaskingEngine::applyPhoneMasking(const std::string& value) {
  // Mantener últimos 4 dígitos
  if (value.length() <= 4) {
    return std::string(value.length(), '*');
  }

  std::string masked = std::string(value.length() - 4, '*') + value.substr(value.length() - 4);
  return masked;
}

std::string DynamicMaskingEngine::applyHashMasking(
    const std::string& value,
    const std::string& algorithm) {
  
  unsigned char hash[SHA256_DIGEST_LENGTH];
  
  if (algorithm == "SHA256") {
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, value.c_str(), value.length());
    SHA256_Final(hash, &sha256);
    
    std::ostringstream oss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
      oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    return oss.str();
  } else if (algorithm == "SHA512") {
    unsigned char hash512[SHA512_DIGEST_LENGTH];
    SHA512_CTX sha512;
    SHA512_Init(&sha512);
    SHA512_Update(&sha512, value.c_str(), value.length());
    SHA512_Final(hash512, &sha512);
    
    std::ostringstream oss;
    for (int i = 0; i < SHA512_DIGEST_LENGTH; ++i) {
      oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash512[i]);
    }
    return oss.str();
  } else if (algorithm == "MD5") {
    unsigned char hashMD5[MD5_DIGEST_LENGTH];
    MD5_CTX md5;
    MD5_Init(&md5);
    MD5_Update(&md5, value.c_str(), value.length());
    MD5_Final(hashMD5, &md5);
    
    std::ostringstream oss;
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
      oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hashMD5[i]);
    }
    return oss.str();
  }

  return applyHashMasking(value, "SHA256");  // Default
}

std::string DynamicMaskingEngine::applyTokenizeMasking(const std::string& value) {
  // Tokenization básico - en producción debería usar TokenizationManager
  // Por ahora, retornar hash truncado
  std::string hash = applyHashMasking(value, "SHA256");
  return hash.substr(0, 16);  // Primeros 16 caracteres del hash
}

std::string DynamicMaskingEngine::applyFormatPreservingMasking(
    const std::string& value,
    const std::string& originalType) {
  
  // Format-preserving masking básico
  // Mantener estructura pero cambiar valores
  std::string masked = value;
  for (char& c : masked) {
    if (std::isdigit(c)) {
      c = '0' + (c - '0' + 1) % 10;  // Rotar dígitos
    } else if (std::isalpha(c)) {
      c = std::isupper(c) 
          ? 'A' + (c - 'A' + 1) % 26 
          : 'a' + (c - 'a' + 1) % 26;  // Rotar letras
    }
  }
  return masked;
}

std::string DynamicMaskingEngine::generateCacheKey(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  return schemaName + "." + tableName + "." + columnName;
}
