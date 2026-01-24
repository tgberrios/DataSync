#ifndef TOKENIZATION_MANAGER_H
#define TOKENIZATION_MANAGER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <memory>
#include <chrono>

using json = nlohmann::json;

// TokenizationManager: Gestión avanzada de tokenization reversible e irreversible
class TokenizationManager {
public:
  enum class TokenType {
    REVERSIBLE,
    IRREVERSIBLE,
    FPE  // Format-Preserving Encryption
  };

  struct TokenRecord {
    int tokenId;
    std::string schemaName;
    std::string tableName;
    std::string columnName;
    std::string originalValue;
    std::string tokenValue;
    TokenType tokenType;
    std::string encryptionKeyId;
    std::chrono::system_clock::time_point createdAt;
    std::chrono::system_clock::time_point expiresAt;
    std::chrono::system_clock::time_point lastAccessedAt;
    int accessCount;
  };

  struct DetokenizationAudit {
    std::string tokenValue;
    std::string username;
    std::string reason;
    std::string originalValue;
    std::chrono::system_clock::time_point accessedAt;
  };

  explicit TokenizationManager(const std::string& connectionString);
  ~TokenizationManager() = default;

  // Tokenizar un valor
  std::string tokenize(
      const std::string& value,
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName,
      bool reversible = true,
      TokenType tokenType = TokenType::REVERSIBLE
  );

  // Detokenizar un token (con auditoría)
  std::string detokenize(
      const std::string& token,
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName,
      const std::string& username,
      const std::string& reason = ""
  );

  // Rotar tokens para una columna
  void rotateTokens(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName
  );

  // Obtener registro de token
  std::unique_ptr<TokenRecord> getTokenRecord(
      const std::string& token,
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName
  );

  // Listar tokens
  std::vector<TokenRecord> listTokens(
      const std::string& schemaName = "",
      const std::string& tableName = "",
      const std::string& columnName = "",
      int limit = 100
  );

  // Obtener historial de detokenization
  std::vector<DetokenizationAudit> getDetokenizationHistory(
      const std::string& token = "",
      const std::string& username = "",
      int limit = 100
  );

  // Generar nueva key de encriptación
  std::string generateEncryptionKey(const std::string& algorithm = "AES256");

  // Rotar keys de encriptación
  void rotateEncryptionKeys(const std::string& keyId = "");

  // Listar keys
  std::vector<json> listEncryptionKeys();

private:
  std::string connectionString_;

  // Helper methods
  std::string generateToken(const std::string& value, TokenType type);
  std::string generateReversibleToken(const std::string& value, const std::string& keyId);
  std::string generateIrreversibleToken(const std::string& value);
  std::string generateFPEToken(const std::string& value, const std::string& keyId);

  std::string decryptToken(const std::string& token, const std::string& keyId);
  std::string getActiveKeyId(const std::string& schemaName, const std::string& tableName, const std::string& columnName);

  void saveTokenRecord(const TokenRecord& record);
  void updateTokenAccess(const std::string& token, const std::string& schemaName, const std::string& tableName, const std::string& columnName);
  void logDetokenization(const DetokenizationAudit& audit);

  std::string encryptKeyMaterial(const std::string& keyMaterial);
  std::string decryptKeyMaterial(const std::string& encryptedKey);
};

#endif // TOKENIZATION_MANAGER_H
