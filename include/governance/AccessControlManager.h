#ifndef ACCESS_CONTROL_MANAGER_H
#define ACCESS_CONTROL_MANAGER_H

#include <pqxx/pqxx>
#include <string>
#include <vector>

struct AccessLogEntry {
  std::string schema_name;
  std::string table_name;
  std::string column_name;
  std::string access_type;
  std::string username;
  std::string application_name;
  std::string client_addr;
  std::string query_text;
  long long rows_accessed;
  bool is_sensitive_data;
  bool masking_applied;
  std::string compliance_requirement;
};

class AccessControlManager {
private:
  std::string connectionString_;

  bool isSensitiveData(const std::string &schemaName,
                       const std::string &tableName,
                       const std::string &columnName = "");
  bool shouldMask(const std::string &schemaName, const std::string &tableName,
                  const std::string &columnName, const std::string &username);
  std::string getMaskingPolicy(const std::string &schemaName,
                               const std::string &tableName,
                               const std::string &columnName);

public:
  explicit AccessControlManager(const std::string &connectionString);
  ~AccessControlManager() = default;

  void logAccess(const AccessLogEntry &entry);
  void logQueryAccess(const std::string &schemaName,
                      const std::string &tableName, const std::string &username,
                      const std::string &queryText, long long rowsAccessed,
                      const std::string &applicationName = "",
                      const std::string &clientAddr = "");

  std::vector<AccessLogEntry> getAccessHistory(const std::string &schemaName,
                                               const std::string &tableName,
                                               const std::string &username = "",
                                               int limit = 100);

  std::vector<AccessLogEntry>
  getSensitiveDataAccess(const std::string &username = "", int days = 30);

  bool checkAccessPermission(const std::string &schemaName,
                             const std::string &tableName,
                             const std::string &username,
                             const std::string &accessType);

  void detectAccessAnomalies(int days = 7);
};

#endif
