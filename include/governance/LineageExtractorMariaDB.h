#ifndef LINEAGE_EXTRACTOR_MARIADB_H
#define LINEAGE_EXTRACTOR_MARIADB_H

#include <mutex>
#include <mysql/mysql.h>
#include <set>
#include <string>
#include <utility>
#include <vector>

struct MariaDBLineageEdge {
  std::string edge_key;
  std::string server_name;
  std::string database_name;
  std::string schema_name;
  std::string object_name;
  std::string object_type;
  std::string column_name;
  std::string target_object_name;
  std::string target_object_type;
  std::string target_column_name;
  std::string relationship_type;
  std::string definition_text;
  int dependency_level = 0;
  std::string discovery_method;
  double confidence_score = 1.0;
  std::string consumer_type;
  std::string consumer_name;
};

class LineageExtractorMariaDB {
public:
  LineageExtractorMariaDB(const std::string &connectionString);
  ~LineageExtractorMariaDB();

  void extractLineage();
  void storeLineage();

private:
  std::string connectionString_;
  std::string serverName_;
  std::string databaseName_;
  std::vector<MariaDBLineageEdge> lineageEdges_;
  mutable std::mutex lineageEdgesMutex_;

  std::string extractServerName(const std::string &connectionString);
  std::string extractDatabaseName(const std::string &connectionString);
  std::string escapeSQL(MYSQL *conn, const std::string &str);
  std::vector<std::vector<std::string>> executeQuery(MYSQL *conn,
                                                     const std::string &query);
  std::string generateEdgeKey(const MariaDBLineageEdge &edge);

  void extractTableDependencies();
  void extractViewDependencies();
  void extractTriggerDependencies();
  void extractForeignKeyDependencies();

  std::set<std::pair<std::string, std::string>>
  extractReferencedTablesFromStatement(const std::string &actionStatement);
  void addTriggerEdge(
      const std::string &triggerSchema, const std::string &triggerName,
      const std::string &eventTable,
      const std::set<std::pair<std::string, std::string>> &referencedTables);
};

#endif
