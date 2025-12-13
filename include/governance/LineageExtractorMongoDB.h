#ifndef LINEAGE_EXTRACTOR_MONGODB_H
#define LINEAGE_EXTRACTOR_MONGODB_H

#include <mongoc/mongoc.h>
#include <mutex>
#include <string>
#include <vector>

struct MongoDBLineageEdge {
  std::string edge_key;
  std::string server_name;
  std::string database_name;
  std::string source_collection;
  std::string source_field;
  std::string target_collection;
  std::string target_field;
  std::string relationship_type;
  std::string definition_text;
  int dependency_level = 0;
  std::string discovery_method;
  double confidence_score = 1.0;
};

class LineageExtractorMongoDB {
private:
  std::string connectionString_;
  mongoc_client_t *client_;
  std::string serverName_;
  std::string databaseName_;
  std::vector<MongoDBLineageEdge> lineageEdges_;
  mutable std::mutex lineageEdgesMutex_;

  std::string extractServerName(const std::string &connectionString);
  std::string extractDatabaseName(const std::string &connectionString);
  bool connect(const std::string &connectionString);
  void disconnect();
  std::string generateEdgeKey(const MongoDBLineageEdge &edge);

public:
  explicit LineageExtractorMongoDB(const std::string &connectionString);
  ~LineageExtractorMongoDB();

  void extractLineage();
  void storeLineage();

private:
  void extractCollectionDependencies();
  void extractViewDependencies();
  void extractPipelineDependencies();
};

#endif
