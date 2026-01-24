#include "governance/TokenizationManager.h"
#include <pqxx/pqxx>
#include <sstream>
#include <iomanip>
#include <random>
#include <cstring>
#include <cctype>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/aes.h>
#include <openssl/rand.h>

TokenizationManager::TokenizationManager(const std::string& connectionString)
    : connectionString_(connectionString) {
}

std::string TokenizationManager::tokenize(
    const std::string& value,
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    bool reversible,
    TokenType tokenType) {
  
  if (value.empty()) {
    return value;
  }

  // Verificar si ya existe un token para este valor
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT token_value
      FROM metadata.tokenization_tokens
      WHERE schema_name = $1 AND table_name = $2 AND column_name = $3
        AND original_value = $4
      LIMIT 1
    )";

    auto result = txn.exec_params(query, schemaName, tableName, columnName, value);
    if (!result.empty()) {
      std::string existingToken = result[0]["token_value"].as<std::string>();
      txn.commit();
      return existingToken;
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error checking existing token: " + std::string(e.what()));
  }

  // Generar nuevo token
  TokenType actualType = reversible ? (tokenType == TokenType::FPE ? TokenType::FPE : TokenType::REVERSIBLE) : TokenType::IRREVERSIBLE;
  std::string token = generateToken(value, actualType);

  // Guardar registro
  TokenRecord record;
  record.schemaName = schemaName;
  record.tableName = tableName;
  record.columnName = columnName;
  record.originalValue = value;
  record.tokenValue = token;
  record.tokenType = actualType;
  record.encryptionKeyId = getActiveKeyId(schemaName, tableName, columnName);
  record.createdAt = std::chrono::system_clock::now();
  record.expiresAt = std::chrono::system_clock::time_point::max();
  record.lastAccessedAt = std::chrono::system_clock::now();
  record.accessCount = 0;

  saveTokenRecord(record);

  return token;
}

std::string TokenizationManager::detokenize(
    const std::string& token,
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    const std::string& username,
    const std::string& reason) {
  
  auto record = getTokenRecord(token, schemaName, tableName, columnName);
  if (!record) {
    Logger::warning(LogCategory::GOVERNANCE, "TokenizationManager",
                    "Token not found for detokenization");
    return "";
  }

  if (record->tokenType == TokenType::IRREVERSIBLE) {
    Logger::warning(LogCategory::GOVERNANCE, "TokenizationManager",
                    "Attempt to detokenize irreversible token");
    return "";
  }

  // Actualizar acceso
  updateTokenAccess(token, schemaName, tableName, columnName);

  // Obtener valor original
  std::string originalValue = record->originalValue;
  if (record->tokenType == TokenType::REVERSIBLE) {
    originalValue = decryptToken(token, record->encryptionKeyId);
  }

  // Registrar auditoría
  DetokenizationAudit audit;
  audit.tokenValue = token;
  audit.username = username;
  audit.reason = reason.empty() ? "Data access" : reason;
  audit.originalValue = originalValue;
  audit.accessedAt = std::chrono::system_clock::now();
  logDetokenization(audit);

  return originalValue;
}

void TokenizationManager::rotateTokens(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Obtener todos los tokens para esta columna
    std::string query = R"(
      SELECT token_id, original_value, token_value, token_type, encryption_key_id
      FROM metadata.tokenization_tokens
      WHERE schema_name = $1 AND table_name = $2 AND column_name = $3
    )";

    auto result = txn.exec_params(query, schemaName, tableName, columnName);

    // Generar nueva key
    std::string newKeyId = generateEncryptionKey();

    // Regenerar tokens con nueva key
    for (const auto& row : result) {
      std::string originalValue = row["original_value"].as<std::string>();
      TokenType tokenType = static_cast<TokenType>(row["token_type"].as<int>());
      
      std::string newToken = generateToken(originalValue, tokenType);

      // Actualizar token
      std::string updateQuery = R"(
        UPDATE metadata.tokenization_tokens
        SET token_value = $1, encryption_key_id = $2, updated_at = NOW()
        WHERE token_id = $3
      )";

      txn.exec_params(updateQuery, newToken, newKeyId, row["token_id"].as<int>());
    }

    txn.commit();
    Logger::info(LogCategory::GOVERNANCE, "TokenizationManager",
                 "Rotated tokens for " + schemaName + "." + tableName + "." + columnName);
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error rotating tokens: " + std::string(e.what()));
  }
}

std::unique_ptr<TokenizationManager::TokenRecord> TokenizationManager::getTokenRecord(
    const std::string& token,
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT token_id, schema_name, table_name, column_name, original_value,
             token_value, token_type, encryption_key_id, created_at, expires_at,
             last_accessed_at, access_count
      FROM metadata.tokenization_tokens
      WHERE token_value = $1 AND schema_name = $2 AND table_name = $3 AND column_name = $4
      LIMIT 1
    )";

    auto result = txn.exec_params(query, token, schemaName, tableName, columnName);

    if (result.empty()) {
      txn.commit();
      return nullptr;
    }

    auto row = result[0];
    auto record = std::make_unique<TokenRecord>();

    record->tokenId = row["token_id"].as<int>();
    record->schemaName = row["schema_name"].as<std::string>();
    record->tableName = row["table_name"].as<std::string>();
    record->columnName = row["column_name"].as<std::string>();
    record->originalValue = row["original_value"].as<std::string>();
    record->tokenValue = row["token_value"].as<std::string>();
    record->tokenType = static_cast<TokenType>(row["token_type"].as<int>());
    
    if (!row["encryption_key_id"].is_null()) {
      record->encryptionKeyId = row["encryption_key_id"].as<std::string>();
    }

    auto createdTimeT = row["created_at"].as<std::time_t>();
    record->createdAt = std::chrono::system_clock::from_time_t(createdTimeT);

    if (!row["expires_at"].is_null()) {
      auto expiresTimeT = row["expires_at"].as<std::time_t>();
      record->expiresAt = std::chrono::system_clock::from_time_t(expiresTimeT);
    } else {
      record->expiresAt = std::chrono::system_clock::time_point::max();
    }

    if (!row["last_accessed_at"].is_null()) {
      auto lastAccessTimeT = row["last_accessed_at"].as<std::time_t>();
      record->lastAccessedAt = std::chrono::system_clock::from_time_t(lastAccessTimeT);
    } else {
      record->lastAccessedAt = record->createdAt;
    }

    record->accessCount = row["access_count"].as<int>();

    txn.commit();
    return record;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error getting token record: " + std::string(e.what()));
    return nullptr;
  }
}

std::vector<TokenizationManager::TokenRecord> TokenizationManager::listTokens(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    int limit) {
  
  std::vector<TokenRecord> tokens;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT token_id, schema_name, table_name, column_name, original_value,
             token_value, token_type, encryption_key_id, created_at, expires_at,
             last_accessed_at, access_count
      FROM metadata.tokenization_tokens
      WHERE 1=1
    )";

    std::vector<std::string> params;
    int paramCount = 1;

    if (!schemaName.empty()) {
      query += " AND schema_name = $" + std::to_string(paramCount++);
      params.push_back(schemaName);
    }
    if (!tableName.empty()) {
      query += " AND table_name = $" + std::to_string(paramCount++);
      params.push_back(tableName);
    }
    if (!columnName.empty()) {
      query += " AND column_name = $" + std::to_string(paramCount++);
      params.push_back(columnName);
    }

    query += " ORDER BY created_at DESC LIMIT $" + std::to_string(paramCount++);
    params.push_back(std::to_string(limit));

    pqxx::params pqParams;
    for (const auto& p : params) {
      pqParams.append(p);
    }

    auto result = txn.exec_params(query, pqParams);

    for (const auto& row : result) {
      TokenRecord record;
      record.tokenId = row["token_id"].as<int>();
      record.schemaName = row["schema_name"].as<std::string>();
      record.tableName = row["table_name"].as<std::string>();
      record.columnName = row["column_name"].as<std::string>();
      record.originalValue = row["original_value"].as<std::string>();
      record.tokenValue = row["token_value"].as<std::string>();
      record.tokenType = static_cast<TokenType>(row["token_type"].as<int>());
      
      if (!row["encryption_key_id"].is_null()) {
        record.encryptionKeyId = row["encryption_key_id"].as<std::string>();
      }

      auto createdTimeT = row["created_at"].as<std::time_t>();
      record.createdAt = std::chrono::system_clock::from_time_t(createdTimeT);

      if (!row["expires_at"].is_null()) {
        auto expiresTimeT = row["expires_at"].as<std::time_t>();
        record.expiresAt = std::chrono::system_clock::from_time_t(expiresTimeT);
      } else {
        record.expiresAt = std::chrono::system_clock::time_point::max();
      }

      if (!row["last_accessed_at"].is_null()) {
        auto lastAccessTimeT = row["last_accessed_at"].as<std::time_t>();
        record.lastAccessedAt = std::chrono::system_clock::from_time_t(lastAccessTimeT);
      } else {
        record.lastAccessedAt = record.createdAt;
      }

      record.accessCount = row["access_count"].as<int>();
      tokens.push_back(record);
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error listing tokens: " + std::string(e.what()));
  }

  return tokens;
}

std::vector<TokenizationManager::DetokenizationAudit> TokenizationManager::getDetokenizationHistory(
    const std::string& token,
    const std::string& username,
    int limit) {
  
  std::vector<DetokenizationAudit> history;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT token_value, username, reason, original_value, accessed_at
      FROM metadata.tokenization_audit
      WHERE 1=1
    )";

    std::vector<std::string> params;
    int paramCount = 1;

    if (!token.empty()) {
      query += " AND token_value = $" + std::to_string(paramCount++);
      params.push_back(token);
    }
    if (!username.empty()) {
      query += " AND username = $" + std::to_string(paramCount++);
      params.push_back(username);
    }

    query += " ORDER BY accessed_at DESC LIMIT $" + std::to_string(paramCount++);
    params.push_back(std::to_string(limit));

    pqxx::params pqParams;
    for (const auto& p : params) {
      pqParams.append(p);
    }

    auto result = txn.exec_params(query, pqParams);

    for (const auto& row : result) {
      DetokenizationAudit audit;
      audit.tokenValue = row["token_value"].as<std::string>();
      audit.username = row["username"].as<std::string>();
      audit.reason = row["reason"].as<std::string>();
      audit.originalValue = row["original_value"].as<std::string>();
      
      auto accessedTimeT = row["accessed_at"].as<std::time_t>();
      audit.accessedAt = std::chrono::system_clock::from_time_t(accessedTimeT);

      history.push_back(audit);
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error getting detokenization history: " + std::string(e.what()));
  }

  return history;
}

std::string TokenizationManager::generateEncryptionKey(const std::string& algorithm) {
  std::string keyId = "key_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
  
  // Generar key material (32 bytes para AES256)
  unsigned char keyMaterial[32];
  RAND_bytes(keyMaterial, 32);

  // Encriptar key material antes de guardar
  std::string keyMaterialStr(reinterpret_cast<const char*>(keyMaterial), 32);
  std::string encryptedKey = encryptKeyMaterial(keyMaterialStr);

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      INSERT INTO metadata.tokenization_keys (key_id, key_material, algorithm, created_at, active)
      VALUES ($1, $2, $3, NOW(), true)
    )";

    txn.exec_params(query, keyId, encryptedKey, algorithm);
    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "TokenizationManager",
                 "Generated new encryption key: " + keyId);
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error generating encryption key: " + std::string(e.what()));
  }

  return keyId;
}

void TokenizationManager::rotateEncryptionKeys(const std::string& keyId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    if (keyId.empty()) {
      // Rotar todas las keys activas
      std::string query = R"(
        UPDATE metadata.tokenization_keys
        SET active = false, rotated_at = NOW()
        WHERE active = true
      )";
      txn.exec(query);
    } else {
      // Rotar key específica
      std::string query = R"(
        UPDATE metadata.tokenization_keys
        SET active = false, rotated_at = NOW()
        WHERE key_id = $1
      )";
      txn.exec_params(query, keyId);
    }

    txn.commit();
    Logger::info(LogCategory::GOVERNANCE, "TokenizationManager",
                 "Rotated encryption keys");
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error rotating encryption keys: " + std::string(e.what()));
  }
}

std::vector<json> TokenizationManager::listEncryptionKeys() {
  std::vector<json> keys;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT key_id, algorithm, created_at, rotated_at, active
      FROM metadata.tokenization_keys
      ORDER BY created_at DESC
    )";

    auto result = txn.exec(query);

    for (const auto& row : result) {
      json key;
      key["key_id"] = row["key_id"].as<std::string>();
      key["algorithm"] = row["algorithm"].as<std::string>();
      key["created_at"] = row["created_at"].as<std::string>();
      if (!row["rotated_at"].is_null()) {
        key["rotated_at"] = row["rotated_at"].as<std::string>();
      }
      key["active"] = row["active"].as<bool>();
      keys.push_back(key);
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error listing encryption keys: " + std::string(e.what()));
  }

  return keys;
}

std::string TokenizationManager::generateToken(const std::string& value, TokenType type) {
  if (type == TokenType::IRREVERSIBLE) {
    return generateIrreversibleToken(value);
  } else if (type == TokenType::FPE) {
    std::string keyId = getActiveKeyId("", "", "");
    return generateFPEToken(value, keyId);
  } else {
    std::string keyId = getActiveKeyId("", "", "");
    return generateReversibleToken(value, keyId);
  }
}

std::string TokenizationManager::generateReversibleToken(
    const std::string& value,
    const std::string& keyId) {
  
  // Usar AES para encriptación reversible
  // Por simplicidad, usar hash + keyId como token
  // En producción, usar AES-GCM o similar
  
  std::ostringstream oss;
  oss << "tok_" << keyId.substr(0, 8) << "_";
  
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  SHA256_Update(&sha256, value.c_str(), value.length());
  SHA256_Update(&sha256, keyId.c_str(), keyId.length());
  SHA256_Final(hash, &sha256);
  
  for (int i = 0; i < 16; ++i) {  // Primeros 16 bytes
    oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
  }
  
  return oss.str();
}

std::string TokenizationManager::generateIrreversibleToken(const std::string& value) {
  // Hash SHA256 del valor (irreversible)
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  SHA256_Update(&sha256, value.c_str(), value.length());
  SHA256_Final(hash, &sha256);
  
  std::ostringstream oss;
  oss << "tok_irr_";
  for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
    oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
  }
  
  return oss.str();
}

std::string TokenizationManager::generateFPEToken(
    const std::string& value,
    const std::string& keyId) {
  
  // Format-preserving encryption básico
  // Mantener longitud y formato del valor original
  std::string token = value;
  
  // Rotar caracteres manteniendo formato
  for (char& c : token) {
    if (std::isdigit(c)) {
      c = '0' + (c - '0' + 5) % 10;
    } else if (std::isalpha(c)) {
      c = std::isupper(c) 
          ? 'A' + (c - 'A' + 13) % 26 
          : 'a' + (c - 'a' + 13) % 26;
    }
  }
  
  return "tok_fpe_" + token;
}

std::string TokenizationManager::decryptToken(
    const std::string& token,
    const std::string& keyId) {
  
  // Para tokens reversibles, necesitamos el valor original guardado
  // En este caso, lo obtenemos de la base de datos
  // En producción, usar AES-GCM para decrypt real
  
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT original_value
      FROM metadata.tokenization_tokens
      WHERE token_value = $1 AND encryption_key_id = $2
      LIMIT 1
    )";

    auto result = txn.exec_params(query, token, keyId);
    if (!result.empty()) {
      std::string originalValue = result[0]["original_value"].as<std::string>();
      txn.commit();
      return originalValue;
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error decrypting token: " + std::string(e.what()));
  }

  return "";
}

std::string TokenizationManager::getActiveKeyId(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT key_id
      FROM metadata.tokenization_keys
      WHERE active = true
      ORDER BY created_at DESC
      LIMIT 1
    )";

    auto result = txn.exec(query);
    if (!result.empty()) {
      std::string keyId = result[0]["key_id"].as<std::string>();
      txn.commit();
      return keyId;
    }

    // Si no hay key activa, generar una nueva
    txn.commit();
    return generateEncryptionKey();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error getting active key: " + std::string(e.what()));
    return generateEncryptionKey();
  }
}

void TokenizationManager::saveTokenRecord(const TokenRecord& record) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      INSERT INTO metadata.tokenization_tokens
        (schema_name, table_name, column_name, original_value, token_value,
         token_type, encryption_key_id, created_at, expires_at, last_accessed_at, access_count)
      VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8, NOW(), $9)
      ON CONFLICT (schema_name, table_name, column_name, original_value)
      DO UPDATE SET
        token_value = EXCLUDED.token_value,
        last_accessed_at = NOW()
    )";

    std::time_t expiresTimeT = std::chrono::system_clock::to_time_t(record.expiresAt);
    std::string expiresAtStr = (record.expiresAt == std::chrono::system_clock::time_point::max())
        ? "NULL"
        : "to_timestamp(" + std::to_string(expiresTimeT) + ")";

    txn.exec_params(query,
                     record.schemaName,
                     record.tableName,
                     record.columnName,
                     record.originalValue,
                     record.tokenValue,
                     static_cast<int>(record.tokenType),
                     record.encryptionKeyId,
                     expiresAtStr == "NULL" ? nullptr : expiresAtStr,
                     record.accessCount);

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error saving token record: " + std::string(e.what()));
  }
}

void TokenizationManager::updateTokenAccess(
    const std::string& token,
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName) {
  
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.tokenization_tokens
      SET last_accessed_at = NOW(), access_count = access_count + 1
      WHERE token_value = $1 AND schema_name = $2 AND table_name = $3 AND column_name = $4
    )";

    txn.exec_params(query, token, schemaName, tableName, columnName);
    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error updating token access: " + std::string(e.what()));
  }
}

void TokenizationManager::logDetokenization(const DetokenizationAudit& audit) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      INSERT INTO metadata.tokenization_audit
        (token_value, username, reason, original_value, accessed_at)
      VALUES ($1, $2, $3, $4, NOW())
    )";

    txn.exec_params(query,
                    audit.tokenValue,
                    audit.username,
                    audit.reason,
                    audit.originalValue);

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "TokenizationManager",
                  "Error logging detokenization: " + std::string(e.what()));
  }
}

std::string TokenizationManager::encryptKeyMaterial(const std::string& keyMaterial) {
  // Encriptación básica de key material
  // En producción, usar un master key o HSM
  // Por ahora, retornar base64 del material
  return keyMaterial;  // Simplificado - en producción debe estar encriptado
}

std::string TokenizationManager::decryptKeyMaterial(const std::string& encryptedKey) {
  // Decrypt básico
  return encryptedKey;  // Simplificado
}
