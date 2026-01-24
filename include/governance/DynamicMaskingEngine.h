#ifndef DYNAMIC_MASKING_ENGINE_H
#define DYNAMIC_MASKING_ENGINE_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <mutex>

using json = nlohmann::json;

// DynamicMaskingEngine: Aplica masking dinámico en tiempo real según políticas
class DynamicMaskingEngine {
public:
  struct MaskingPolicy {
    int policyId;
    std::string policyName;
    std::string schemaName;
    std::string tableName;
    std::string columnName;
    std::string maskingType;  // FULL, PARTIAL, EMAIL, PHONE, HASH, TOKENIZE
    std::string maskingAlgorithm;  // deterministic, random, format-preserving
    bool preserveFormat;
    char maskChar;
    int visibleChars;
    std::string hashAlgorithm;  // SHA256, SHA512, MD5
    std::vector<std::string> roleWhitelist;
    bool active;
    json maskingParams;
  };

  explicit DynamicMaskingEngine(const std::string& connectionString);
  ~DynamicMaskingEngine() = default;

  // Aplicar masking a un valor individual
  std::string applyMasking(
      const std::string& value,
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName,
      const std::string& username,
      const std::vector<std::string>& userRoles
  );

  // Aplicar masking a una fila completa
  json applyMaskingToRow(
      const json& row,
      const std::string& schemaName,
      const std::string& tableName,
      const std::vector<std::string>& columns,
      const std::string& username,
      const std::vector<std::string>& userRoles
  );

  // Aplicar masking a múltiples filas
  std::vector<json> applyMaskingToRows(
      const std::vector<json>& rows,
      const std::string& schemaName,
      const std::string& tableName,
      const std::vector<std::string>& columns,
      const std::string& username,
      const std::vector<std::string>& userRoles
  );

  // Obtener política de masking para una columna
  std::unique_ptr<MaskingPolicy> getMaskingPolicy(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName
  );

  // Verificar si un usuario/rol tiene acceso sin masking
  bool shouldMaskForUser(
      const MaskingPolicy& policy,
      const std::string& username,
      const std::vector<std::string>& userRoles
  );

  // Invalidar cache de políticas
  void invalidateCache();

private:
  std::string connectionString_;
  std::map<std::string, std::unique_ptr<MaskingPolicy>> policyCache_;
  std::mutex cacheMutex_;

  // Helper methods
  std::unique_ptr<MaskingPolicy> loadPolicyFromDatabase(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName
  );

  std::string applyFullMasking(const std::string& value);
  std::string applyPartialMasking(const std::string& value, char maskChar, int visibleChars);
  std::string applyEmailMasking(const std::string& value);
  std::string applyPhoneMasking(const std::string& value);
  std::string applyHashMasking(const std::string& value, const std::string& algorithm);
  std::string applyTokenizeMasking(const std::string& value);
  std::string applyFormatPreservingMasking(const std::string& value, const std::string& originalType);

  std::string generateCacheKey(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName
  );
};

#endif // DYNAMIC_MASKING_ENGINE_H
