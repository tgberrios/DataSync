#ifndef SCHEMA_CHANGE_AUDITOR_H
#define SCHEMA_CHANGE_AUDITOR_H

#include <memory>
#include <string>
#include <vector>

struct SchemaChangeRecord {
  std::string db_engine;
  std::string server_name;
  std::string database_name;
  std::string schema_name;
  std::string object_name;
  std::string object_type;
  std::string change_type;  // CREATE, ALTER, DROP, RENAME, TRUNCATE
  std::string ddl_statement;
  std::string executed_by;
  std::string connection_string;
  std::string before_state_json;
  std::string after_state_json;
  std::vector<std::string> affected_columns;
  std::string rollback_sql;
  bool is_rollback_possible = false;
  std::string metadata_json;
};

class SchemaChangeAuditor {
private:
  std::string metadataConnectionString_;

  void storeAuditRecord(const SchemaChangeRecord &record);
  std::string getObjectStatePostgreSQL(const std::string &connStr,
                                       const std::string &schema,
                                       const std::string &objectName,
                                       const std::string &objectType);
  std::string getObjectStateMariaDB(const std::string &connStr,
                                     const std::string &schema,
                                     const std::string &objectName,
                                     const std::string &objectType);
  std::string getObjectStateMSSQL(const std::string &connStr,
                                   const std::string &schema,
                                   const std::string &objectName,
                                   const std::string &objectType);
  std::string getObjectStateMongoDB(const std::string &connStr,
                                    const std::string &database,
                                    const std::string &collectionName);

  void setupPostgreSQLDDLCapture(const std::string &connStr);
  void setupMariaDBDDLCapture(const std::string &connStr);
  void setupMSSQLDDLCapture(const std::string &connStr);
  void setupMongoDBDDLCapture(const std::string &connStr);

public:
  explicit SchemaChangeAuditor(const std::string &metadataConnectionString);
  ~SchemaChangeAuditor();

  void initializeDDLCapture();
  void captureDDLChange(const SchemaChangeRecord &record);
  void setupDDLCaptureForEngine(const std::string &dbEngine,
                                 const std::string &connectionString);
};

#endif
