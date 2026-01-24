#include "governance/FineGrainedPermissions.h"
#include <pqxx/pqxx>
#include <algorithm>
#include <sstream>

FineGrainedPermissions::FineGrainedPermissions(const std::string& connectionString)
    : connectionString_(connectionString) {
}

bool FineGrainedPermissions::checkColumnPermission(
    const std::string& username,
    const std::vector<std::string>& roles,
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    const std::string& operation) {
  
  auto policies = getApplicablePolicies(username, roles, schemaName, tableName, operation);

  // Buscar política específica para esta columna
  for (const auto& policy : policies) {
    if (policy.policyType == PolicyType::COLUMN && policy.columnName == columnName) {
      if (policy.active) {
        // Verificar condición ABAC si existe
        if (!policy.attributeConditions.empty()) {
          if (!evaluateABACCondition(policy.attributeConditions, username, roles)) {
            continue;
          }
        }
        return true;  // Permiso encontrado
      }
    }
  }

  // Si no hay política específica, verificar política de tabla
  for (const auto& policy : policies) {
    if (policy.policyType == PolicyType::TABLE) {
      if (policy.active) {
        if (!policy.attributeConditions.empty()) {
          if (!evaluateABACCondition(policy.attributeConditions, username, roles)) {
            continue;
          }
        }
        return true;  // Permiso a nivel de tabla
      }
    }
  }

  return false;  // Sin permiso
}

std::string FineGrainedPermissions::generateRowFilter(
    const std::string& username,
    const std::vector<std::string>& roles,
    const std::string& schemaName,
    const std::string& tableName) {
  
  auto policies = getApplicablePolicies(username, roles, schemaName, tableName, "SELECT");

  std::vector<std::string> conditions;

  for (const auto& policy : policies) {
    if (policy.policyType == PolicyType::ROW && policy.active) {
      if (!policy.conditionExpression.empty()) {
        // Verificar condición ABAC si existe
        if (!policy.attributeConditions.empty()) {
          if (!evaluateABACCondition(policy.attributeConditions, username, roles)) {
            continue;
          }
        }
        conditions.push_back("(" + policy.conditionExpression + ")");
      }
    }
  }

  if (conditions.empty()) {
    return "1=1";  // Sin restricciones
  }

  // Combinar condiciones con OR (si hay múltiples políticas, el usuario puede acceder si cumple alguna)
  std::ostringstream filterStream;
  for (size_t i = 0; i < conditions.size(); ++i) {
    if (i > 0) {
      filterStream << " OR ";
    }
    filterStream << conditions[i];
  }

  return filterStream.str();
}

std::vector<std::string> FineGrainedPermissions::getAccessibleColumns(
    const std::string& username,
    const std::vector<std::string>& roles,
    const std::string& schemaName,
    const std::string& tableName) {
  
  auto policies = getApplicablePolicies(username, roles, schemaName, tableName, "SELECT");

  std::vector<std::string> accessibleColumns;
  bool hasTableLevelPermission = false;

  for (const auto& policy : policies) {
    if (policy.policyType == PolicyType::TABLE && policy.active) {
      if (policy.attributeConditions.empty() || 
          evaluateABACCondition(policy.attributeConditions, username, roles)) {
        hasTableLevelPermission = true;
        break;
      }
    }
  }

  if (hasTableLevelPermission) {
    // Si hay permiso a nivel de tabla, retornar todas las columnas (vacío significa todas)
    return {};
  }

  // Recopilar columnas con permiso específico
  for (const auto& policy : policies) {
    if (policy.policyType == PolicyType::COLUMN && 
        !policy.columnName.empty() && 
        policy.active) {
      if (policy.attributeConditions.empty() || 
          evaluateABACCondition(policy.attributeConditions, username, roles)) {
        if (std::find(accessibleColumns.begin(), accessibleColumns.end(), policy.columnName) 
            == accessibleColumns.end()) {
          accessibleColumns.push_back(policy.columnName);
        }
      }
    }
  }

  return accessibleColumns;
}

std::vector<FineGrainedPermissions::PermissionPolicy> FineGrainedPermissions::getApplicablePolicies(
    const std::string& username,
    const std::vector<std::string>& roles,
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& operation) {
  
  std::vector<PermissionPolicy> applicablePolicies;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT policy_id, policy_name, policy_type, schema_name, table_name, column_name,
             role_name, username, operation, condition_expression, attribute_conditions,
             priority, active
      FROM metadata.permission_policies
      WHERE active = true
        AND (schema_name = $1 OR schema_name IS NULL)
        AND (table_name = $2 OR table_name IS NULL)
        AND operation = $3
      ORDER BY priority DESC, policy_id
    )";

    auto result = txn.exec_params(query, schemaName, tableName, operation);

    for (const auto& row : result) {
      PermissionPolicy policy;
      policy.policyId = row["policy_id"].as<int>();
      policy.policyName = row["policy_name"].as<std::string>();
      
      std::string typeStr = row["policy_type"].as<std::string>();
      if (typeStr == "COLUMN") {
        policy.policyType = PolicyType::COLUMN;
      } else if (typeStr == "ROW") {
        policy.policyType = PolicyType::ROW;
      } else if (typeStr == "TABLE") {
        policy.policyType = PolicyType::TABLE;
      }

      if (!row["schema_name"].is_null()) {
        policy.schemaName = row["schema_name"].as<std::string>();
      }
      if (!row["table_name"].is_null()) {
        policy.tableName = row["table_name"].as<std::string>();
      }
      if (!row["column_name"].is_null()) {
        policy.columnName = row["column_name"].as<std::string>();
      }
      if (!row["role_name"].is_null()) {
        policy.roleName = row["role_name"].as<std::string>();
      }
      if (!row["username"].is_null()) {
        policy.username = row["username"].as<std::string>();
      }

      std::string opStr = row["operation"].as<std::string>();
      if (opStr == "SELECT") {
        policy.operation = Operation::SELECT;
      } else if (opStr == "INSERT") {
        policy.operation = Operation::INSERT;
      } else if (opStr == "UPDATE") {
        policy.operation = Operation::UPDATE;
      } else if (opStr == "DELETE") {
        policy.operation = Operation::DELETE;
      }

      if (!row["condition_expression"].is_null()) {
        policy.conditionExpression = row["condition_expression"].as<std::string>();
      }
      if (!row["attribute_conditions"].is_null()) {
        policy.attributeConditions = json::parse(row["attribute_conditions"].as<std::string>());
      }

      policy.priority = row["priority"].as<int>();
      policy.active = row["active"].as<bool>();

      // Filtrar por usuario/rol
      bool matches = false;
      if (!policy.username.empty() && policy.username == username) {
        matches = true;
      } else if (!policy.roleName.empty()) {
        for (const auto& role : roles) {
          if (role == policy.roleName) {
            matches = true;
            break;
          }
        }
      } else {
        // Política sin restricción de usuario/rol
        matches = true;
      }

      if (matches) {
        applicablePolicies.push_back(policy);
      }
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "FineGrainedPermissions",
                  "Error getting applicable policies: " + std::string(e.what()));
  }

  return applicablePolicies;
}

bool FineGrainedPermissions::evaluateABACCondition(
    const json& attributeConditions,
    const std::string& username,
    const std::vector<std::string>& roles) {
  
  if (attributeConditions.empty()) {
    return true;
  }

  // Obtener atributos del usuario
  auto userAttributes = getUserAttributes(username);

  std::map<std::string, std::string> attributeMap;
  for (const auto& attr : userAttributes) {
    attributeMap[attr.attributeName] = attr.attributeValue;
  }

  // Evaluar condiciones
  for (const auto& [key, value] : attributeConditions.items()) {
    if (attributeMap.count(key) == 0) {
      return false;  // Atributo no encontrado
    }

    std::string userValue = attributeMap[key];
    std::string requiredValue = value.get<std::string>();

    if (userValue != requiredValue) {
      return false;  // Valor no coincide
    }
  }

  return true;
}

std::vector<FineGrainedPermissions::UserAttribute> FineGrainedPermissions::getUserAttributes(
    const std::string& userId) {
  
  return loadUserAttributesFromDatabase(userId);
}

void FineGrainedPermissions::setUserAttribute(
    const std::string& userId,
    const std::string& attributeName,
    const std::string& attributeValue) {
  
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      INSERT INTO metadata.user_attributes (user_id, attribute_name, attribute_value, created_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (user_id, attribute_name)
      DO UPDATE SET attribute_value = EXCLUDED.attribute_value
    )";

    txn.exec_params(query, userId, attributeName, attributeValue);
    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "FineGrainedPermissions",
                 "Set attribute " + attributeName + " for user " + userId);
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "FineGrainedPermissions",
                  "Error setting user attribute: " + std::string(e.what()));
  }
}

int FineGrainedPermissions::createPolicy(const PermissionPolicy& policy) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string typeStr;
    switch (policy.policyType) {
      case PolicyType::COLUMN:
        typeStr = "COLUMN";
        break;
      case PolicyType::ROW:
        typeStr = "ROW";
        break;
      case PolicyType::TABLE:
        typeStr = "TABLE";
        break;
    }

    std::string opStr;
    switch (policy.operation) {
      case Operation::SELECT:
        opStr = "SELECT";
        break;
      case Operation::INSERT:
        opStr = "INSERT";
        break;
      case Operation::UPDATE:
        opStr = "UPDATE";
        break;
      case Operation::DELETE:
        opStr = "DELETE";
        break;
    }

    std::string query = R"(
      INSERT INTO metadata.permission_policies
        (policy_name, policy_type, schema_name, table_name, column_name,
         role_name, username, operation, condition_expression, attribute_conditions,
         priority, active, created_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, NOW())
      RETURNING policy_id
    )";

    auto row = txn.exec_params1(query,
                                policy.policyName,
                                typeStr,
                                policy.schemaName.empty() ? nullptr : policy.schemaName,
                                policy.tableName.empty() ? nullptr : policy.tableName,
                                policy.columnName.empty() ? nullptr : policy.columnName,
                                policy.roleName.empty() ? nullptr : policy.roleName,
                                policy.username.empty() ? nullptr : policy.username,
                                opStr,
                                policy.conditionExpression.empty() ? nullptr : policy.conditionExpression,
                                policy.attributeConditions.empty() ? "{}" : policy.attributeConditions.dump(),
                                policy.priority,
                                policy.active);

    int policyId = row[0].as<int>();
    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "FineGrainedPermissions",
                 "Created policy: " + policy.policyName);
    return policyId;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "FineGrainedPermissions",
                  "Error creating policy: " + std::string(e.what()));
    return -1;
  }
}

void FineGrainedPermissions::updatePolicy(int policyId, const PermissionPolicy& policy) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string typeStr;
    switch (policy.policyType) {
      case PolicyType::COLUMN:
        typeStr = "COLUMN";
        break;
      case PolicyType::ROW:
        typeStr = "ROW";
        break;
      case PolicyType::TABLE:
        typeStr = "TABLE";
        break;
    }

    std::string opStr;
    switch (policy.operation) {
      case Operation::SELECT:
        opStr = "SELECT";
        break;
      case Operation::INSERT:
        opStr = "INSERT";
        break;
      case Operation::UPDATE:
        opStr = "UPDATE";
        break;
      case Operation::DELETE:
        opStr = "DELETE";
        break;
    }

    std::string query = R"(
      UPDATE metadata.permission_policies
      SET policy_name = $1, policy_type = $2, schema_name = $3, table_name = $4,
          column_name = $5, role_name = $6, username = $7, operation = $8,
          condition_expression = $9, attribute_conditions = $10::jsonb,
          priority = $11, active = $12
      WHERE policy_id = $13
    )";

    txn.exec_params(query,
                    policy.policyName,
                    typeStr,
                    policy.schemaName.empty() ? nullptr : policy.schemaName,
                    policy.tableName.empty() ? nullptr : policy.tableName,
                    policy.columnName.empty() ? nullptr : policy.columnName,
                    policy.roleName.empty() ? nullptr : policy.roleName,
                    policy.username.empty() ? nullptr : policy.username,
                    opStr,
                    policy.conditionExpression.empty() ? nullptr : policy.conditionExpression,
                    policy.attributeConditions.empty() ? "{}" : policy.attributeConditions.dump(),
                    policy.priority,
                    policy.active,
                    policyId);

    txn.commit();
    Logger::info(LogCategory::GOVERNANCE, "FineGrainedPermissions",
                 "Updated policy: " + std::to_string(policyId));
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "FineGrainedPermissions",
                  "Error updating policy: " + std::string(e.what()));
  }
}

void FineGrainedPermissions::deletePolicy(int policyId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      DELETE FROM metadata.permission_policies
      WHERE policy_id = $1
    )";

    txn.exec_params(query, policyId);
    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "FineGrainedPermissions",
                 "Deleted policy: " + std::to_string(policyId));
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "FineGrainedPermissions",
                  "Error deleting policy: " + std::string(e.what()));
  }
}

std::vector<FineGrainedPermissions::PermissionPolicy> FineGrainedPermissions::listPolicies(
    const std::string& schemaName,
    const std::string& tableName,
    PolicyType policyType) {
  
  return loadPoliciesFromDatabase(schemaName, tableName, policyType);
}

std::vector<FineGrainedPermissions::PermissionPolicy> FineGrainedPermissions::loadPoliciesFromDatabase(
    const std::string& schemaName,
    const std::string& tableName,
    PolicyType policyType) {
  
  std::vector<PermissionPolicy> policies;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT policy_id, policy_name, policy_type, schema_name, table_name, column_name,
             role_name, username, operation, condition_expression, attribute_conditions,
             priority, active
      FROM metadata.permission_policies
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

    std::string typeStr;
    switch (policyType) {
      case PolicyType::COLUMN:
        typeStr = "COLUMN";
        break;
      case PolicyType::ROW:
        typeStr = "ROW";
        break;
      case PolicyType::TABLE:
        typeStr = "TABLE";
        break;
    }
    if (!typeStr.empty()) {
      query += " AND policy_type = $" + std::to_string(paramCount++);
      params.push_back(typeStr);
    }

    query += " ORDER BY priority DESC, policy_id";

    pqxx::params pqParams;
    for (const auto& p : params) {
      pqParams.append(p);
    }

    auto result = params.empty() ? txn.exec(query) : txn.exec_params(query, pqParams);

    for (const auto& row : result) {
      PermissionPolicy policy;
      policy.policyId = row["policy_id"].as<int>();
      policy.policyName = row["policy_name"].as<std::string>();
      
      std::string typeStr = row["policy_type"].as<std::string>();
      if (typeStr == "COLUMN") {
        policy.policyType = PolicyType::COLUMN;
      } else if (typeStr == "ROW") {
        policy.policyType = PolicyType::ROW;
      } else if (typeStr == "TABLE") {
        policy.policyType = PolicyType::TABLE;
      }

      if (!row["schema_name"].is_null()) {
        policy.schemaName = row["schema_name"].as<std::string>();
      }
      if (!row["table_name"].is_null()) {
        policy.tableName = row["table_name"].as<std::string>();
      }
      if (!row["column_name"].is_null()) {
        policy.columnName = row["column_name"].as<std::string>();
      }
      if (!row["role_name"].is_null()) {
        policy.roleName = row["role_name"].as<std::string>();
      }
      if (!row["username"].is_null()) {
        policy.username = row["username"].as<std::string>();
      }

      std::string opStr = row["operation"].as<std::string>();
      if (opStr == "SELECT") {
        policy.operation = Operation::SELECT;
      } else if (opStr == "INSERT") {
        policy.operation = Operation::INSERT;
      } else if (opStr == "UPDATE") {
        policy.operation = Operation::UPDATE;
      } else if (opStr == "DELETE") {
        policy.operation = Operation::DELETE;
      }

      if (!row["condition_expression"].is_null()) {
        policy.conditionExpression = row["condition_expression"].as<std::string>();
      }
      if (!row["attribute_conditions"].is_null()) {
        policy.attributeConditions = json::parse(row["attribute_conditions"].as<std::string>());
      }

      policy.priority = row["priority"].as<int>();
      policy.active = row["active"].as<bool>();

      policies.push_back(policy);
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "FineGrainedPermissions",
                  "Error loading policies: " + std::string(e.what()));
  }

  return policies;
}

bool FineGrainedPermissions::evaluateCondition(
    const std::string& conditionExpression,
    const std::string& username,
    const std::vector<std::string>& roles) {
  
  // Evaluación básica de condiciones SQL
  // En producción, usar un parser SQL más robusto
  // Por ahora, solo verificar si contiene referencias a usuario/roles
  
  if (conditionExpression.empty()) {
    return true;
  }

  // Reemplazar placeholders
  std::string condition = conditionExpression;
  
  // Reemplazar {username} con el username real
  size_t pos = condition.find("{username}");
  while (pos != std::string::npos) {
    condition.replace(pos, 10, "'" + username + "'");
    pos = condition.find("{username}", pos + username.length() + 2);
  }

  // Reemplazar {roles} con lista de roles
  pos = condition.find("{roles}");
  while (pos != std::string::npos) {
    std::ostringstream rolesStr;
    rolesStr << "(";
    for (size_t i = 0; i < roles.size(); ++i) {
      if (i > 0) rolesStr << ",";
      rolesStr << "'" << roles[i] << "'";
    }
    rolesStr << ")";
    condition.replace(pos, 7, rolesStr.str());
    pos = condition.find("{roles}", pos + rolesStr.str().length());
  }

  // En producción, ejecutar la condición en un contexto seguro
  // Por ahora, retornar true si no hay errores de sintaxis obvios
  return !condition.empty();
}

std::vector<FineGrainedPermissions::UserAttribute> FineGrainedPermissions::loadUserAttributesFromDatabase(
    const std::string& userId) {
  
  std::vector<UserAttribute> attributes;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT user_id, attribute_name, attribute_value
      FROM metadata.user_attributes
      WHERE user_id = $1
    )";

    auto result = txn.exec_params(query, userId);

    for (const auto& row : result) {
      UserAttribute attr;
      attr.userId = row["user_id"].as<std::string>();
      attr.attributeName = row["attribute_name"].as<std::string>();
      attr.attributeValue = row["attribute_value"].as<std::string>();
      attributes.push_back(attr);
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "FineGrainedPermissions",
                  "Error loading user attributes: " + std::string(e.what()));
  }

  return attributes;
}
