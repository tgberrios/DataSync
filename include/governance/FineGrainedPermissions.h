#ifndef FINE_GRAINED_PERMISSIONS_H
#define FINE_GRAINED_PERMISSIONS_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>
#include <memory>

using json = nlohmann::json;

// FineGrainedPermissions: Sistema de permisos granulares (columna, fila, RBAC, ABAC)
class FineGrainedPermissions {
public:
  enum class PolicyType {
    COLUMN,
    ROW,
    TABLE
  };

  enum class Operation {
    SELECT,
    INSERT,
    UPDATE,
    DELETE
  };

  struct PermissionPolicy {
    int policyId;
    std::string policyName;
    PolicyType policyType;
    std::string schemaName;
    std::string tableName;
    std::string columnName;
    std::string roleName;
    std::string username;
    Operation operation;
    std::string conditionExpression;  // SQL condition para ROW-level
    json attributeConditions;  // Para ABAC
    int priority;
    bool active;
  };

  struct UserAttribute {
    std::string userId;
    std::string attributeName;
    std::string attributeValue;
  };

  explicit FineGrainedPermissions(const std::string& connectionString);
  ~FineGrainedPermissions() = default;

  // Verificar permiso de columna
  bool checkColumnPermission(
      const std::string& username,
      const std::vector<std::string>& roles,
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName,
      const std::string& operation
  );

  // Generar filtro SQL para filas
  std::string generateRowFilter(
      const std::string& username,
      const std::vector<std::string>& roles,
      const std::string& schemaName,
      const std::string& tableName
  );

  // Obtener columnas accesibles para un usuario
  std::vector<std::string> getAccessibleColumns(
      const std::string& username,
      const std::vector<std::string>& roles,
      const std::string& schemaName,
      const std::string& tableName
  );

  // Obtener políticas aplicables
  std::vector<PermissionPolicy> getApplicablePolicies(
      const std::string& username,
      const std::vector<std::string>& roles,
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& operation
  );

  // Verificar condición ABAC
  bool evaluateABACCondition(
      const json& attributeConditions,
      const std::string& username,
      const std::vector<std::string>& roles
  );

  // Obtener atributos de usuario
  std::vector<UserAttribute> getUserAttributes(const std::string& userId);

  // Establecer atributo de usuario
  void setUserAttribute(
      const std::string& userId,
      const std::string& attributeName,
      const std::string& attributeValue
  );

  // Crear política
  int createPolicy(const PermissionPolicy& policy);

  // Actualizar política
  void updatePolicy(int policyId, const PermissionPolicy& policy);

  // Eliminar política
  void deletePolicy(int policyId);

  // Listar políticas
  std::vector<PermissionPolicy> listPolicies(
      const std::string& schemaName = "",
      const std::string& tableName = "",
      PolicyType policyType = PolicyType::COLUMN
  );

private:
  std::string connectionString_;

  // Helper methods
  std::vector<PermissionPolicy> loadPoliciesFromDatabase(
      const std::string& schemaName,
      const std::string& tableName,
      PolicyType policyType
  );

  bool evaluateCondition(
      const std::string& conditionExpression,
      const std::string& username,
      const std::vector<std::string>& roles
  );

  std::vector<UserAttribute> loadUserAttributesFromDatabase(const std::string& userId);
};

#endif // FINE_GRAINED_PERMISSIONS_H
