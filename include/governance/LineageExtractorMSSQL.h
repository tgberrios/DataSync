#ifndef LINEAGE_EXTRACTOR_MSSQL_H
#define LINEAGE_EXTRACTOR_MSSQL_H

#include <string>
#include <vector>
#include <sql.h>

struct LineageEdge {
  std::string edge_key;
  std::string server_name;
  std::string instance_name;
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
  long long execution_count = 0;
  double avg_duration_ms = 0.0;
  double avg_cpu_ms = 0.0;
  long long avg_logical_reads = 0;
  long long avg_physical_reads = 0;
  std::string consumer_type;
  std::string consumer_name;
};

class LineageExtractorMSSQL {
public:
  LineageExtractorMSSQL(const std::string &connectionString);
  ~LineageExtractorMSSQL();

  void extractLineage();
  void storeLineage();

private:
  std::string connectionString_;
  std::string serverName_;
  std::string databaseName_;
  std::vector<LineageEdge> lineageEdges_;

  std::string extractServerName(const std::string &connectionString);
  std::string extractDatabaseName(const std::string &connectionString);
  std::string escapeSQL(const std::string &str);
  std::vector<std::vector<std::string>> executeQuery(SQLHDBC conn, const std::string &query);
  std::string generateEdgeKey(const LineageEdge &edge);
  
  void extractTableDependencies();
  void extractStoredProcedureDependencies();
  void extractViewDependencies();
  void extractForeignKeyDependencies();
  void extractSqlExpressionDependencies();
};

#endif
