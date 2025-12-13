#ifndef DATA_GOVERNANCE_MONGODB_H
#define DATA_GOVERNANCE_MONGODB_H

#include <mongoc/mongoc.h>
#include <string>
#include <vector>

struct MongoDBGovernanceData {
  std::string server_name;
  std::string database_name;
  std::string collection_name;
  std::string index_name;
  std::string index_keys;
  bool index_unique = false;
  bool index_sparse = false;
  std::string index_type;
  long long document_count = 0;
  double collection_size_mb = 0.0;
  double index_size_mb = 0.0;
  double total_size_mb = 0.0;
  double storage_size_mb = 0.0;
  double avg_object_size_bytes = 0.0;
  int index_count = 0;
  std::string replica_set_name;
  bool is_sharded = false;
  std::string shard_key;
  std::string access_frequency;
  std::string health_status;
  std::string recommendation_summary;
  double health_score = 0.0;
  std::string mongodb_version;
  std::string storage_engine;
};

class DataGovernanceMongoDB {
private:
  std::string connectionString_;
  mongoc_client_t *client_;
  std::string serverName_;
  std::string databaseName_;
  std::vector<MongoDBGovernanceData> governanceData_;

  std::string extractServerName(const std::string &connectionString);
  std::string extractDatabaseName(const std::string &connectionString);
  bool connect(const std::string &connectionString);
  void disconnect();

public:
  explicit DataGovernanceMongoDB(const std::string &connectionString);
  ~DataGovernanceMongoDB();

  void collectGovernanceData();
  void storeGovernanceData();
  void generateReport();

private:
  void queryCollectionStats();
  void queryIndexStats();
  void queryReplicaSetInfo();
  void calculateHealthScores();
};

#endif
